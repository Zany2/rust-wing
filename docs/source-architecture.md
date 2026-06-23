# RustWing Source Architecture

This document shows the current source architecture after the multi-connection-system model landed.

## Workspace View

```mermaid
flowchart TB
    App["Application / rust-wing-example"]
    Axum["rust-wing-axum<br/>WebSocket integration"]
    Core["rust-wing-core<br/>connection, session, routing, protocol"]
    Adapter["rust-wing-adapter<br/>infrastructure bridges"]
    Redis["Redis<br/>optional backend"]

    App --> Axum
    App --> Core
    Axum --> Core
    Adapter --> Core
    Adapter -. redis feature .-> Redis
    Core -. cluster contracts .-> Adapter
```

## Core Modules

```mermaid
flowchart LR
    Lib["lib.rs<br/>public exports"]
    Identity["identity.rs<br/>ConnectionType / UserId / ClientId / SessionId / Identity"]
    Config["config.rs<br/>RustWingConfig / ConnectionPolicy / ClusterConfig"]
    Session["session.rs<br/>Session / AcceptedSession / SessionSnapshot"]
    Protocol["protocol.rs<br/>WsMessage / OutboundFrame / heartbeat payloads"]
    Manager["manager.rs<br/>RustWing / Registry / DeliveryReport"]
    Cluster["cluster.rs<br/>Route / ClusterEnvelope / PresenceStore / NodePublisher"]
    Error["error.rs<br/>RustWingError / Result"]

    Lib --> Identity
    Lib --> Config
    Lib --> Session
    Lib --> Protocol
    Lib --> Manager
    Lib --> Cluster
    Lib --> Error

    Manager --> Identity
    Manager --> Config
    Manager --> Session
    Manager --> Protocol
    Manager --> Cluster
    Manager --> Error

    Session --> Identity
    Session --> Protocol
    Cluster --> Identity
    Cluster --> Protocol
```

## Multi-Connection Registry

```mermaid
flowchart TB
    Identity["Identity<br/>connection_type + user_id + client_id"]
    Policy["RustWingConfig::policy_for(connection_type)<br/>default policy + per-system overrides"]
    Registry["Registry"]
    BySession["by_session<br/>SessionId -> Session"]
    ByUser["by_user<br/>(ConnectionType, UserId) -> SessionIds"]
    ByClient["by_client<br/>(ConnectionType, UserId, ClientId?) -> SessionIds"]

    Identity --> Policy
    Identity --> Registry
    Policy --> Registry
    Registry --> BySession
    Registry --> ByUser
    Registry --> ByClient
```

## Accept Flow

```mermaid
sequenceDiagram
    participant App as Application / Axum
    participant Auth as AxumAuthenticator
    participant Wing as RustWing
    participant Session as AcceptedSession
    participant Registry as Registry
    participant Presence as PresenceStore

    App->>Auth: authenticate(AxumAuthContext)
    Auth-->>App: Identity
    App->>Wing: accept(Identity)
    Wing->>Session: new(node_id, identity, queue_capacity)
    Wing->>Registry: insert(session, policy_for(connection_type))
    Registry-->>Wing: replaced sessions
    Wing->>Wing: close replaced sessions
    alt cluster enabled
        Wing->>Presence: register_node(node_id, instance_id, node_lease_ttl)
        Wing->>Presence: register(Route{connection_type,user_id,client_id,session_id,node_id})
    end
    Wing-->>App: AcceptedSession { session, outbound }
```

## Delivery Flow

```mermaid
flowchart TB
    SendUser["send_to_user(user_id, frame)"]
    SendSystemUser["send_to_user_in(connection_type, user_id, frame)"]
    SendClient["send_to_client(user_id, client_id, frame)"]
    SendSystemClient["send_to_client_in(connection_type, user_id, client_id, frame)"]
    SendSession["send_to_session(session_id, frame)"]
    Broadcast["broadcast(frame)"]
    BroadcastSystem["broadcast_in(connection_type, frame)"]
    BroadcastAll["broadcast_all(frame)"]
    Disconnect["disconnect_user/client/session(...)"]

    LocalUser["local by_user lookup"]
    LocalClient["local by_client lookup"]
    LocalSession["local by_session lookup"]
    LocalBroadcast["local connection/global broadcast"]
    Queue["Session::enqueue(frame)<br/>bounded outbound queue"]
    Unregister["RustWing::unregister(session)"]

    Presence["PresenceStore::locate(connection_type, user_id)"]
    PresenceSession["PresenceStore::locate_session(session_id)"]
    PresenceNodes["PresenceStore::list_nodes()"]
    Publisher["NodePublisher::publish(node_id, ClusterEnvelope)"]
    Remote["remote RustWing::handle_cluster_envelope(envelope)"]

    SendUser --> LocalUser --> Queue
    SendSystemUser --> LocalUser
    SendClient --> LocalClient --> Queue
    SendSystemClient --> LocalClient
    SendSession --> LocalSession --> Queue
    Broadcast --> LocalBroadcast --> Queue
    BroadcastSystem --> LocalBroadcast
    BroadcastAll --> LocalBroadcast
    Disconnect --> LocalUser
    Disconnect --> LocalClient
    Disconnect --> LocalSession
    Disconnect --> Unregister

    SendUser -. cluster enabled .-> Presence
    SendClient -. cluster enabled .-> Presence
    SendSession -. cluster enabled .-> PresenceSession
    Broadcast -. cluster enabled .-> PresenceNodes
    BroadcastAll -. cluster enabled .-> PresenceNodes
    Disconnect -. cluster enabled .-> Presence
    Disconnect -. exact session .-> PresenceSession
    Presence --> Publisher --> Remote
    PresenceSession --> Publisher
    PresenceNodes --> Publisher
    Remote --> LocalUser
    Remote --> LocalClient
    Remote --> LocalSession
    Remote --> LocalBroadcast
```

## Acknowledgement Flow

```mermaid
sequenceDiagram
    participant App
    participant Wing as RustWing
    participant Session
    participant Client
    participant Tracker as AckTracker

    App->>Wing: next_message_id()
    App->>Wing: send_to_user(frame.require_ack(message_id))
    Wing->>Session: enqueue(frame)
    Wing->>Tracker: track(message_id, session_id)
    Session-->>Client: WebSocket message
    Client-->>Wing: {"type":"ack","data":{"message_id","stage"}}
    Wing->>Tracker: acknowledge(session_id, message_id, stage)
    App->>Wing: ack_snapshot / wait_for_ack
    App->>Wing: reap_expired_acks / ack_pending_count
```

## Shutdown Flow

```mermaid
sequenceDiagram
    participant App
    participant Wing as RustWing
    participant Registry
    participant Presence as PresenceStore

    App->>Wing: shutdown().await
    Wing->>Wing: stop node lease refresher
    Wing->>Registry: all_sessions()
    loop each local session
        Wing->>Presence: remove(connection_type, user_id, session_id)
        Wing->>Wing: session.close("unregistered")
    end
    Wing->>Presence: unregister_node(node_id, instance_id)
    Wing-->>App: closed session count
```

## Adapter Layer

```mermaid
flowchart LR
    CorePresence["core::PresenceStore"]
    CorePublisher["core::NodePublisher"]
    PresenceAdapter["PresenceStoreAdapter"]
    PublisherAdapter["NodePublisherAdapter"]
    BridgePresence["PresenceStoreBridge"]
    BridgePublisher["NodePublisherBridge"]
    Memory["MemoryPresenceAdapter"]
    RedisPresence["RedisPresenceAdapter"]
    RedisPublisher["RedisNodePublisherAdapter"]
    RedisSubscriber["RedisNodeSubscriberAdapter"]
    RedisHandle["RedisNodeSubscriberHandle"]

    PresenceAdapter --> BridgePresence --> CorePresence
    PublisherAdapter --> BridgePublisher --> CorePublisher
    Memory --> PresenceAdapter
    RedisPresence --> PresenceAdapter
    RedisPublisher --> PublisherAdapter
    RedisSubscriber --> RedisHandle
    RedisSubscriber --> CorePublisher
```

## External Send API

```mermaid
sequenceDiagram
    participant Biz as External business system
    participant Api as rust-wing-axum send_api_router
    participant Guard as AxumSendApiGuard
    participant Wing as RustWing

    Biz->>Api: POST /send/user or /broadcast/all
    Api->>Guard: authorize(headers)
    Guard-->>Api: allow or HTTP error
    Api->>Wing: send_to_user / send_to_client / send_to_session / broadcast
    Wing-->>Api: DeliveryReport
    Api-->>Biz: SendApiResponse
```

## Axum Integration

```mermaid
sequenceDiagram
    participant Client
    participant Axum as rust-wing-axum
    participant Wing as RustWing
    participant Handler as AxumMessageHandler
    participant Session

    Client->>Axum: WebSocket upgrade
    Axum->>Wing: accept(identity)
    Wing-->>Axum: AcceptedSession
    par reader task
        Axum->>Wing: handle_heartbeat(session, client_time)
        Axum->>Handler: handle_text(context, text)
    and writer task
        Session-->>Axum: outbound frame
        Axum-->>Client: WebSocket message
    end
    Axum->>Wing: unregister(session)
```

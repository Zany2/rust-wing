<p align="center">
  <img src="./docs/images/logo.png" alt="RustWing" width="120"/>
</p>

<h1 align="center">RustWing</h1>

<p align="center">
  A modular WebSocket foundation for Rust realtime services.
</p>

<p align="center">
  <img alt="Rust" src="https://img.shields.io/badge/Rust-2024-000000?logo=rust&logoColor=white" />
  <img alt="Status" src="https://img.shields.io/badge/status-early--stage-orange" />
  <img alt="License" src="https://img.shields.io/badge/license-see%20LICENSE-blue" />
</p>

<p align="center">
  English | <a href="./README_zh.md">简体中文</a>
</p>

---

RustWing is an early-stage Rust framework for building realtime WebSocket
services. It focuses on the infrastructure that many WebSocket applications
repeat: connection lifecycle, session indexes, heartbeat handling, bounded
outbound queues, user-targeted delivery, and distributed routing.

The project is split into small crates so applications can use the core
connection manager directly, add an Axum WebSocket integration, or plug in
external infrastructure such as Redis when they need multi-node delivery.

## Features

- Session lifecycle management with strongly typed connection type, user, client, session, and node IDs.
- Multi-connection systems with default and per-system connection policies.
- Bounded outbound queues with explicit backpressure behavior.
- User-targeted local delivery and local broadcast.
- Built-in heartbeat acknowledgement and inactive session reaping.
- Optional cluster routing through presence storage and node publishers.
- Adapter bridges for custom infrastructure implementations.
- Axum WebSocket upgrade integration.
- Redis presence, publisher, and subscriber adapters behind the `redis` feature.

## Workspace

| Crate | Purpose |
| --- | --- |
| `rust-wing-core` | Core session, protocol, routing, heartbeat, and manager APIs. |
| `rust-wing-axum` | Axum WebSocket integration built on top of `rust-wing-core`. |
| `rust-wing-adapter` | Adapter contracts, memory adapter, and optional Redis backend pieces. |

See [Source Architecture](./docs/source-architecture.md) for the current crate and module diagrams.

RustWing keeps public module roots flat where practical. For example, the
core delivery entry lives in `delivery.rs`, and adapter messaging lives in
`messaging.rs`, with internals split into sibling files. This keeps the crate
layout easier to scan than deep `mod.rs` trees.

## Status

RustWing is usable as a foundation, but the public API is still pre-stable.
Expect crate boundaries, configuration names, and adapter details to evolve
before a stable release.

## Installation

Add the crates you need:

```toml
[dependencies]
rust-wing-core = "0.0.1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

For Axum integration:

```toml
rust-wing-axum = "0.0.1"
axum = { version = "0.8", features = ["ws"] }
```

For Redis-backed cluster adapters:

```toml
rust-wing-adapter = { version = "0.0.1", features = ["redis"] }
```

When developing inside this repository, use the workspace crates directly:

```toml
rust-wing-core = { path = "rust-wing-core" }
rust-wing-axum = { path = "rust-wing-axum" }
rust-wing-adapter = { path = "rust-wing-adapter", features = ["redis"] }
```

## Quick Start

Create a manager, accept a default-system session, and send a frame to a user:

```rust
use rust_wing_core::{OutboundFrame, RustWing, RustWingConfig};

#[tokio::main]
async fn main() -> rust_wing_core::Result<()> {
    let wing = RustWing::new(RustWingConfig::default());
    let mut accepted = wing.accept_user("alice").await?;

    let report = wing
        .send_to_user("alice", OutboundFrame::text("hello"))
        .await?;

    assert_eq!(report.local_sessions, 1);
    assert_eq!(report.remote_nodes, 0);

    if let Some(frame) = accepted.outbound.recv().await {
        println!("queued frame: {:?}", frame.kind);
    }

    Ok(())
}
```

## Axum Usage

Authenticate the request in your application, build an `Identity`, then hand the
upgraded socket to RustWing:

```rust
use axum::{
    extract::{ws::WebSocketUpgrade, State},
    response::Response,
    routing::get,
    Router,
};
use rust_wing_axum::upgrade;
use rust_wing_core::{Identity, RustWing, RustWingConfig};

async fn ws_handler(ws: WebSocketUpgrade, State(wing): State<RustWing>) -> Response {
    let identity = Identity::default_connection("alice").with_client("browser");
    upgrade(ws, wing, identity)
}

fn app() -> Router {
    let wing = RustWing::new(RustWingConfig::default());

    Router::new()
        .route("/ws", get(ws_handler))
        .with_state(wing)
}
```

Use `upgrade_with_handler` when your application needs to process non-heartbeat
client messages. Implement `AxumMessageHandler` and keep business authentication,
authorization, and payload validation in your application layer.

When the WebSocket module must authenticate against another service, provide an
`AxumAuthenticator`. Authentication runs before the WebSocket upgrade; failures
return HTTP responses and do not create sessions:

```rust
use async_trait::async_trait;
use axum::{
    extract::{ws::WebSocketUpgrade, State},
    http::{HeaderMap, Uri},
    response::Response,
};
use rust_wing_axum::{
    AxumAuthContext, AxumAuthError, AxumAuthenticator, NoopAxumMessageHandler,
    upgrade_with_auth,
};
use rust_wing_core::{Identity, RustWing};

struct TokenAuth;

#[async_trait]
impl AxumAuthenticator for TokenAuth {
    async fn authenticate(&self, context: AxumAuthContext) -> Result<Identity, AxumAuthError> {
        let token = context
            .headers
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .ok_or_else(|| AxumAuthError::unauthorized("missing authorization"))?;

        let user_id = verify_token(token).await?;
        Ok(Identity::default_connection(user_id).with_client("browser"))
    }
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    headers: HeaderMap,
    uri: Uri,
    State(wing): State<RustWing>,
) -> Response {
    upgrade_with_auth(
        ws,
        wing,
        AxumAuthContext { headers, uri },
        TokenAuth,
        NoopAxumMessageHandler,
    )
    .await
}
# async fn verify_token(_token: &str) -> Result<String, AxumAuthError> { Ok("alice".into()) }
```

## Heartbeat Protocol

The Axum integration recognizes RustWing protocol messages encoded as JSON text
frames. A client heartbeat can be sent as:

```json
{
  "type": "heartbeat",
  "event": "client_report",
  "data": {
    "client_time": 1716000000000
  }
}
```

RustWing replies with a `heartbeat_ack` message that includes the echoed client
timestamp, server heartbeat timestamp, configured heartbeat interval, and
configured timeout.

## Connection Systems

RustWing defaults to `ConnectionPolicy::UniqueClient`, which keeps one active
session per `(connection_type, user_id, client_id)` tuple. Different connection
systems can keep the same user and client identifiers online without replacing
each other, which is useful when one application has multiple authentication
systems such as user, admin, and game gateways.

When an application only has one connection system, use the default helpers.
They still store routes with `connection_type = "default"` internally:

```rust
use rust_wing_core::Identity;

let identity = Identity::default_connection("alice").with_client("browser");

wing.send_to_user("alice", frame).await?;
wing.send_to_client("alice", Some("browser"), frame).await?;
wing.broadcast(frame).await?;
```

Use `ConnectionPolicy::UniqueUser` when one user may only keep one connection,
or `ConnectionPolicy::MultiSession` when repeated connections from the same client
should all remain active. Use the config helpers to set the common policy and
override specific connection systems:

```rust
use rust_wing_core::{ConnectionPolicy, RustWing, RustWingConfig};

let wing = RustWing::new(
    RustWingConfig::default()
        .with_default_connection_policy(ConnectionPolicy::UniqueClient)
        .with_connection_policy("game", ConnectionPolicy::MultiSession),
);
```

Delivery targets are independent from the connection policy:

- `send_to_user(user_id, frame)` sends to the default connection system.
- `send_to_client(user_id, client_id, frame)` sends to one default-system client slot.
- `broadcast(frame)` broadcasts to the default connection system across the cluster.
- `send_to_user_in(connection_type, user_id, frame)` sends to every active session for the user in one connection system.
- `send_to_client_in(connection_type, user_id, client_id, frame)` sends to every active session in one client slot.
- `send_to_session(session_id, frame)` sends to one exact local or remote session.
- `broadcast_in(connection_type, frame)` broadcasts to one connection system across the cluster.
- `broadcast_all(frame)` broadcasts to all local and remote sessions.
- `broadcast_local(frame)` broadcasts only on the current node.

Disconnect targets follow the same default-first naming:

- `disconnect_user(user_id, reason)` disconnects all default-system sessions for one user.
- `disconnect_client(user_id, client_id, reason)` disconnects one default-system client slot.
- `disconnect_session(session_id, reason)` disconnects one exact local or remote session.
- `disconnect_user_in(connection_type, user_id, reason)` disconnects one user in a connection system.
- `disconnect_client_in(connection_type, user_id, client_id, reason)` disconnects one client slot in a connection system.

## External Send API

`rust-wing-axum` can expose a reusable HTTP router for non-Rust business
systems. Always protect this router with a guard in production:

```rust
use rust_wing_axum::{ApiKeySendApiGuard, send_api_router};
use rust_wing_core::RustWing;

fn internal_router(wing: RustWing) -> axum::Router {
    send_api_router(wing, ApiKeySendApiGuard::new("internal-secret"))
}
```

The router provides:

- `POST /send/user`
- `POST /send/client`
- `POST /send/session`
- `POST /broadcast`
- `POST /broadcast/all`
- `POST /disconnect/user`
- `POST /disconnect/client`
- `POST /disconnect/session`
- `POST /systems/{connection_type}/send/user`
- `POST /systems/{connection_type}/broadcast`
- `POST /systems/{connection_type}/disconnect/user`
- `POST /systems/{connection_type}/disconnect/client`
- `GET /stats`
- `GET /cluster/nodes`
- `GET /cluster/routes`
- `GET /systems/{connection_type}/routes`

External broker messages use the same target model and can also disconnect a
user, a client slot, or an exact session with `disconnect_user`,
`disconnect_client`, and `disconnect_session` targets.

## Cluster Routing

Cluster routing is opt-in. When enabled, RustWing registers live routes in a
presence store and publishes cross-node envelopes through a node publisher.

For in-process tests or local experiments:

```rust
use rust_wing_core::cluster::NoopPublisher;
use rust_wing_core::{
    Cluster, ClusterConfig, MemoryPresenceStore, RustWing, RustWingConfig,
};

#[tokio::main]
async fn main() -> rust_wing_core::Result<()> {
    let wing = RustWing::with_cluster_checked(
        RustWingConfig {
            cluster: ClusterConfig {
                enabled: true,
                ..ClusterConfig::default()
            },
            ..RustWingConfig::default()
        },
        Some(Cluster::new(MemoryPresenceStore::new(), NoopPublisher)),
    )
    .await?;

    Ok(())
}
```

For Redis-backed routing, prefer the managed runtime from the adapter crate. It
creates the Redis presence store, node publisher, and subscriber task for the
current node, then exposes a regular `RustWing` handle:

```rust
use rust_wing_adapter::redis_rust_wing_from_config;
use rust_wing_core::{OutboundFrame, RustWingConfig};

#[tokio::main]
async fn main() -> rust_wing_core::Result<()> {
    let redis_url = "redis://127.0.0.1:6379";
    let runtime = redis_rust_wing_from_config(
        RustWingConfig::default().with_node_id("node-a"),
        redis_url,
    )
    .await?;

    let wing = runtime.wing_clone();
    wing.send_to_user("alice", OutboundFrame::text("hello")).await?;

    // On graceful shutdown:
    runtime.shutdown().await?;

    Ok(())
}
```

Route storage and the node-to-node message channel are intentionally separate.
Use `rust_wing_from_adapters` when they come from different infrastructure, for
example Redis for routes and Kafka/NATS/custom middleware for cluster messages:

```rust
use rust_wing_adapter::{
    NatsNodePublisherAdapter, NatsNodeSubscriberAdapter, NatsPublisherConfig,
    RedisPresenceAdapter, RedisPresenceConfig, rust_wing_from_adapters,
};
use rust_wing_core::RustWingConfig;

# async fn build() -> rust_wing_core::Result<()> {
let presence = RedisPresenceAdapter::connect(
    RedisPresenceConfig::new("redis://127.0.0.1:6379")
).await?;
let nats = NatsPublisherConfig::from_urls([
    "nats://nats-1:4222",
    "nats://nats-2:4222",
]);
let publisher = NatsNodePublisherAdapter::connect(nats.clone()).await?;
let subscriber = NatsNodeSubscriberAdapter::connect(nats).await?;

let wing = rust_wing_from_adapters(
    RustWingConfig::default().with_node_id("node-a"),
    presence,
    publisher,
).await?;
let subscriber = subscriber.spawn_current_node(wing.clone());

// On graceful shutdown:
subscriber.shutdown().await?;
wing.shutdown().await?;
# Ok(())
# }
```

Enable the `redis` and `nats` features for this composition. Kafka offers the
same `KafkaNodePublisherAdapter` / `KafkaNodeSubscriberAdapter` split on
non-Windows targets. Kafka uses one topic and consumer group per `NodeId`; the
broker must allow automatic topic creation or the node topics must be created
before startup. Kafka-backed nodes must use `NodeId` values containing only
ASCII letters, digits, `.`, `_`, or `-`; generated node ids already satisfy
this requirement.

If an application needs separate Redis key prefixes, channel prefixes, or manual
subscriber lifecycle control, it can still use
`redis_cluster_parts_from_config(...)` to assemble the cluster and subscriber
explicitly.

Redis presence and Redis node messaging support standalone, Cluster, and
Sentinel deployments through `RedisDeployment`:

```rust
use rust_wing_adapter::{
    RedisDeployment, RedisPresenceConfig, RedisPublisherConfig,
    RedisSentinelConfig,
};

let cluster = RedisDeployment::cluster([
    "redis://redis-1:6379",
    "redis://redis-2:6379",
    "redis://redis-3:6379",
]);
let cluster_presence = RedisPresenceConfig::from_deployment(cluster.clone());
let cluster_publisher = RedisPublisherConfig::from_deployment(cluster);

let sentinel = RedisDeployment::sentinel(
    RedisSentinelConfig::new(
        ["redis://sentinel-1:26379", "redis://sentinel-2:26379"],
        "mymaster",
    )
    .with_redis_credentials("default", "password"),
);
let sentinel_presence = RedisPresenceConfig::from_deployment(sentinel.clone());
let sentinel_publisher = RedisPublisherConfig::from_deployment(sentinel);
# let _ = (cluster_presence, cluster_publisher, sentinel_presence, sentinel_publisher);
```

All v2 presence keys share one Redis Cluster hash slot so multi-key route
updates remain atomic. The session index replaces cluster-wide `SCAN`; this
prioritizes correctness and failover over sharding presence data across slots.
The v2 key layout is not readable by older RustWing versions, so all nodes in
one cluster should be upgraded together.

Every `RustWingConfig::default()` generates a `node-<UUID v7>` identifier. Set
`node_id` explicitly with `with_node_id(...)` when a stable, readable value is
needed, or use `RustWingConfig::from_env()` to let `RUST_WING_NODE_ID` override
the generated value:

```rust
let wing = RustWing::new(RustWingConfig::from_env());
```

Recommended values:

| Environment | Recommended `node_id` |
| --- | --- |
| Kubernetes | Pod name, for example `rust-wing-ws-0` or the Deployment pod name. |
| Docker Compose | Container hostname or container name. |
| Bare metal | `hostname:port`, so multiple instances on one host stay distinct. |
| Cloud VM | Instance id plus port when available. |
| Local development | Use the generated default unless a stable name is useful. |

Use `RustWing::with_cluster_checked(...)` or an adapter-managed runtime for
clustered startup. Cluster startup always registers and refreshes a short-lived
node lease, whether the `node_id` was generated or configured, and returns an
error when the same value is already owned by another live instance. Broadcast
routing only uses nodes with an active lease, so stale node records do not
receive new cluster envelopes.

Call `shutdown().await` during graceful service shutdown to unregister local
sessions and release the node lease:

```rust
let closed = wing.shutdown().await?;
```

## Configuration

| Field | Default | Purpose |
| --- | --- | --- |
| `node_id` | Generated `node-<UUID v7>`, or `RUST_WING_NODE_ID` via `from_env()` | Identifies the current server node. |
| `heartbeat_interval` | `30s` | Heartbeat interval reported to clients. |
| `heartbeat_timeout` | `90s` | Inactivity window before a session can be reaped. |
| `write_queue_capacity` | `64` | Bounded outbound queue size per session. |
| `default_connection_policy` | `UniqueClient` | Controls how sessions coexist when a connection system has no override. |
| `connection_policies` | empty | Overrides the policy for specific connection systems. |
| `cluster.enabled` | `false` | Enables presence registration and remote routing when adapter dependencies are injected. |
| `cluster.route_ttl` | `90s` | Lifetime for distributed route records. |
| `cluster.node_lease_ttl` | `30s` | Lifetime for duplicate-node protection leases. |

`RustWingConfig::normalized()` replaces empty or zero values with safe defaults.
`rust-wing-core` does not choose Redis, Kafka, NATS, or memory backends by configuration. Distributed routing and node messaging are injected through `rust-wing-adapter`, so route storage and the message channel can be composed independently.

## Development

This repository uses a Cargo workspace:

```bash
cargo fmt --all
cargo check --workspace
cargo test
```

Run feature-specific tests when working on infrastructure adapters:

```bash
cargo test -p rust-wing-adapter --features redis
cargo test -p rust-wing-adapter --features nats
cargo test -p rust-wing-adapter --features kafka
```

## Contributing

Contributions are welcome while the project is still small and easy to shape.
Please keep changes focused, include tests for behavior changes, and run
formatting before opening a pull request.

Useful contribution areas:

- Examples for real applications.
- Additional web framework integrations.
- More production-oriented Redis documentation.
- Tracing, metrics, and operational hooks.
- Room, channel, and topic helpers built on top of the core routing model.

## Security

RustWing manages connection infrastructure, not application trust. Applications
should authenticate WebSocket requests before creating an `Identity`, validate
all business payloads, and enforce authorization before sending user-targeted
messages.

## License

This project is licensed under the terms in [LICENSE](./LICENSE).


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

- Session lifecycle management with strongly typed user, device, session, and node IDs.
- Single-session and multi-session connection policies per user.
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

Create a manager, accept a session, and send a frame to a user:

```rust
use rust_wing_core::{Identity, OutboundFrame, RustWing, RustWingConfig};

#[tokio::main]
async fn main() -> rust_wing_core::Result<()> {
    let wing = RustWing::new(RustWingConfig::default());
    let mut accepted = wing.accept(Identity::new("alice")).await?;

    let delivered = wing
        .send_to_user("alice", OutboundFrame::text("hello"))
        .await?;

    assert_eq!(delivered, 1);

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
    let identity = Identity::new("alice").with_device("browser");
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

## Connection Policy

RustWing defaults to `ConnectionPolicy::Single`, which keeps only one active
session per user and closes older sessions when the same user reconnects.

Use `ConnectionPolicy::Multi` when one user may stay connected from multiple
devices or browser tabs:

```rust
use rust_wing_core::{ConnectionPolicy, RustWing, RustWingConfig};

let wing = RustWing::new(RustWingConfig {
    connection_policy: ConnectionPolicy::Multi,
    ..RustWingConfig::default()
});
```

## Cluster Routing

Cluster routing is opt-in. When enabled, RustWing registers live routes in a
presence store and publishes cross-node envelopes through a node publisher.

For in-process tests or local experiments:

```rust
use rust_wing_core::{ClusterConfig, RustWing, RustWingConfig};

#[tokio::main]
async fn main() -> rust_wing_core::Result<()> {
    let wing = RustWing::from_config(RustWingConfig {
        cluster: ClusterConfig {
            enabled: true,
            ..ClusterConfig::default()
        },
        ..RustWingConfig::default()
    })
    .await?;

    Ok(())
}
```

For Redis-backed routing, build a cluster from the adapter crate and run a
subscriber for the current node:

```rust
use rust_wing_adapter::{
    redis_cluster_from_config, RedisNodeSubscriberAdapter, RedisPresenceConfig,
    RedisPublisherConfig,
};
use rust_wing_core::{ClusterConfig, NodeId, RustWing, RustWingConfig};

#[tokio::main]
async fn main() -> rust_wing_core::Result<()> {
    let redis_url = "redis://127.0.0.1:6379";
    let publisher = RedisPublisherConfig::new(redis_url);
    let cluster = redis_cluster_from_config(
        RedisPresenceConfig::new(redis_url),
        publisher.clone(),
    )
    .await?;

    let wing = RustWing::with_cluster(
        RustWingConfig {
            node_id: NodeId::from("node-a"),
            cluster: ClusterConfig {
                enabled: true,
                ..ClusterConfig::default()
            },
            ..RustWingConfig::default()
        },
        Some(cluster),
    );

    let subscriber = RedisNodeSubscriberAdapter::connect(publisher).await?;
    let subscriber_wing = wing.clone();
    tokio::spawn(async move {
        if let Err(error) = subscriber.run_current_node(subscriber_wing).await {
            eprintln!("rust-wing redis subscriber stopped: {error}");
        }
    });

    Ok(())
}
```

## Configuration

| Field | Default | Purpose |
| --- | --- | --- |
| `node_id` | `local` | Identifies the current server node. |
| `heartbeat_interval` | `15s` | Heartbeat interval reported to clients. |
| `heartbeat_timeout` | `45s` | Inactivity window before a session can be reaped. |
| `write_queue_capacity` | `64` | Bounded outbound queue size per session. |
| `connection_policy` | `Single` | Controls single-session or multi-session users. |
| `cluster.enabled` | `false` | Enables presence registration and remote routing. |
| `cluster.backend` | `Memory` | Config-driven backend selector used by `from_config`. |
| `cluster.route_ttl` | `90s` | Lifetime for distributed route records. |

`RustWingConfig::normalized()` replaces empty or zero values with safe defaults.

## Development

This repository uses a Cargo workspace:

```bash
cargo fmt --all
cargo check --workspace
cargo test
```

Run feature-specific tests when working on Redis code:

```bash
cargo test -p rust-wing-adapter --features redis
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

<p align="center">
  <img src="./docs/images/logo.png" alt="RustWing" width="120"/>
</p>

<h1 align="center">RustWing</h1>

<p align="center">
  A modular WebSocket framework for Rust realtime applications.
</p>

<p align="center">
  <img alt="Rust" src="https://img.shields.io/badge/Rust-2024-000000?logo=rust&logoColor=white" />
  <img alt="Status" src="https://img.shields.io/badge/status-early--stage-orange" />
  <img alt="License" src="https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue" />
</p>

<p align="center">
  English | <a href="./README_zh.md">简体中文</a>
</p>

---

RustWing is a Rust framework for building realtime WebSocket services. It provides a clean foundation for connection lifecycle, session management, heartbeat handling, message delivery, and distributed routing.

The project is split into focused crates, so applications can start small and add framework integrations or external backends only when needed.

## Why RustWing

- Build WebSocket services with a reusable core instead of repeating connection plumbing.
- Keep business authentication and message handling inside your application.
- Choose single-session or multi-session users according to your product model.
- Scale from local development to distributed deployment with the same core API.
- Use framework integrations without coupling the core to one web stack.

## Quick Start

Add RustWing to your application:

```toml
[dependencies]
rust-wing-core = "0.0.1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

Add the extra crate you need for the following examples:

```toml
rust-wing-axum = "0.0.1"
rust-wing-adapter = { version = "0.0.1", features = ["redis"] }
axum = { version = "0.8", features = ["ws"] }
```

Create a manager, accept a session, and send a message to a user:

```rust
use rust_wing_core::{Identity, OutboundFrame, RustWing, RustWingConfig};

#[tokio::main]
async fn main() -> rust_wing_core::Result<()> {
    let wing = RustWing::new(RustWingConfig::default());
    let accepted = wing.accept(Identity::new("alice")).await?;

    wing.send_to_user("alice", OutboundFrame::text("hello")).await?;

    let mut outbound = accepted.outbound;
    if let Some(frame) = outbound.recv().await {
        println!("send frame: {:?}", frame.kind);
    }

    Ok(())
}
```

## Web Framework Usage

In a web application, authenticate the request first, create an `Identity`, then pass the upgraded connection to RustWing:

```rust
use axum::{extract::ws::WebSocketUpgrade, response::Response};
use rust_wing_axum::upgrade;
use rust_wing_core::{Identity, RustWing};

async fn websocket_handler(ws: WebSocketUpgrade, wing: RustWing) -> Response {
    let identity = Identity::new("alice");
    upgrade(ws, wing, identity)
}
```

Use `upgrade_with_handler` when your application needs to handle business messages received from the client.

## Distributed Usage

RustWing can run with an external routing backend when your service has multiple nodes:

```rust
use rust_wing_adapter::{
    redis_cluster_from_config, RedisPresenceConfig, RedisPublisherConfig,
};
use rust_wing_core::{ClusterConfig, RustWing, RustWingConfig};

#[tokio::main]
async fn main() -> rust_wing_core::Result<()> {
    let cluster = redis_cluster_from_config(
        RedisPresenceConfig::new("redis://127.0.0.1:6379"),
        RedisPublisherConfig::new("redis://127.0.0.1:6379"),
    )
    .await?;

    let wing = RustWing::with_cluster(
        RustWingConfig {
            cluster: ClusterConfig {
                enabled: true,
                ..ClusterConfig::default()
            },
            ..RustWingConfig::default()
        },
        Some(cluster),
    );

    Ok(())
}
```

## Workspace

| Crate | Purpose |
| --- | --- |
| `rust-wing-core` | Core APIs for sessions, protocol messages, routing, and connection management. |
| `rust-wing-adapter` | Adapter contracts and backend implementations for external infrastructure. |
| `rust-wing-axum` | WebSocket integration for Axum applications. |

## Configuration

Common configuration fields:

| Field | Purpose |
| --- | --- |
| `node_id` | Identifies the current server node. |
| `heartbeat_interval` | Suggested heartbeat interval for clients. |
| `heartbeat_timeout` | Timeout for inactive sessions. |
| `write_queue_capacity` | Outbound queue size for each session. |
| `connection_policy` | Controls whether users can keep one or multiple sessions. |
| `cluster.enabled` | Enables distributed routing. |
| `cluster.route_ttl` | Expiration duration for distributed routes. |

## Status

RustWing is currently in an early foundation stage. The core APIs are usable, but package names, module boundaries, and adapter APIs may still change before the first stable release.

## Development

```bash
cargo fmt --all
cargo check --workspace
cargo test
```

## Roadmap

- Stabilize the public APIs.
- Add complete examples.
- Expand framework integrations.
- Improve distributed deployment documentation.
- Add tracing, metrics, and operational hooks.
- Add room, channel, and topic helpers.

## License

Licensed under either of:

- Apache License, Version 2.0
- MIT license

See [LICENSE](./LICENSE).

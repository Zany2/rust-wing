# RustWing

[Language: English | [中文](README_zh.md)]

RustWing is an open-source, general-purpose distributed WebSocket framework written in Rust.

## Project Goal

RustWing is not a business-specific IM utility package. It is a reusable WebSocket framework core. It separates connection lifecycle, message protocol, local delivery, presence routing, and cluster transport into composable boundaries, so applications can plug in different web frameworks and distributed backends.

RustWing is suitable for:

- instant messaging gateways
- notification delivery services
- collaboration, presence, and realtime dashboards
- games, IoT, and device connections
- horizontally scalable WebSocket services

## Architecture

```text
application
  |
web framework adapter
  |  axum / hyper / tokio-tungstenite
  |
rust-wing core
  |-- protocol: versioned message envelope and frame types
  |-- session: connection identity, snapshots, write queue
  |-- manager: local registry, connection policy, send/broadcast
  |-- cluster: presence store and node publisher traits
  |
cluster backends
  |-- redis
  |-- nats
  |-- memory/testing
```

## Current Features

- single-session or multi-session connection policy
- identity model with user, device, node, and session IDs
- bounded per-session write queues
- local user delivery and local broadcast
- `serde`-based protocol envelope
- distributed presence routing abstraction
- node-to-node publishing abstraction
- in-memory presence store for tests and examples

## Example

```rust
use rust_wing::{Identity, OutboundFrame, RustWing, RustWingConfig};

#[tokio::main]
async fn main() -> rust_wing::Result<()> {
    let wing = RustWing::new(RustWingConfig::default());
    let accepted = wing.accept(Identity::new("alice")).await?;

    wing.send_to_user("alice", OutboundFrame::text("hello")).await?;

    // The web framework adapter owns this receiver and writes frames to the socket.
    let mut outbound = accepted.outbound;
    if let Some(frame) = outbound.recv().await {
        println!("send frame: {:?}", frame.kind);
    }

    Ok(())
}
```

## Roadmap

- `axum` WebSocket adapter
- Redis presence store
- Redis Pub/Sub node publisher
- NATS backend
- room, channel, and topic broadcast
- heartbeat handling and timeout cleanup
- authentication middleware
- metrics and tracing integration
- more examples and benchmarks

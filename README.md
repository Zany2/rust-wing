<h1 align="center">RustWing</h1>

<p align="center">
  <img alt="Rust" src="https://img.shields.io/badge/Rust-2024-000000?logo=rust&logoColor=white" />
  <img alt="Tokio" src="https://img.shields.io/badge/Tokio-1-5E5CE6" />
  <img alt="Serde" src="https://img.shields.io/badge/Serde-1-3B82F6" />
  <img alt="Status" src="https://img.shields.io/badge/status-core%20foundation-orange" />
</p>

<p align="center">
  English · <a href="./README_zh.md">简体中文</a>
</p>

> **A distributed WebSocket framework core for Rust.** RustWing provides reusable session management, bounded write queues, protocol envelopes, local delivery, presence routing, and cluster transport abstractions for horizontally scalable realtime services.

```bash
cargo test
```

## What It Is

RustWing is not a business-specific IM utility package. It is a reusable WebSocket framework core that keeps the most important realtime service concerns small and composable.

The project currently focuses on the core library. Web framework adapters and distributed backends are designed as pluggable layers instead of being hard-wired into the session manager.

## Core Features

- **Session lifecycle**: accepts user identities, creates session IDs, stores snapshots, and unregisters sessions.
- **Connection policy**: supports single-session and multi-session users.
- **Bounded write queue**: each session owns a bounded outbound channel with explicit backpressure behavior.
- **Heartbeat lifecycle**: records heartbeats, returns acknowledgement timing data, and reaps inactive sessions.
- **Protocol envelope**: provides versioned message and frame types built on `serde`.
- **Local delivery**: sends messages to local users and broadcasts to local sessions.
- **Presence routing**: abstracts multi-session user route registration, touch, lookup, and removal.
- **Cluster publishing**: abstracts node-to-node message publishing for Redis, NATS, or custom backends.
- **Memory backend**: includes an in-memory presence store for tests and examples.

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

## Delivery Model

| Step | Responsibility | Description |
| --- | --- | --- |
| Accept | `RustWing::accept` | Register an authenticated identity and create a session. |
| Write | web adapter | Own the outbound receiver and write frames to the socket. |
| Send local | manager | Deliver frames to local sessions first. |
| Route remote | presence store | Locate the node that owns the target user. |
| Publish remote | node publisher | Forward the frame to the target node channel. |
| Apply remote | manager | Handle the cluster envelope and deliver it locally. |

## Quick Start

Add RustWing to your application crate:

```toml
[dependencies]
rust-wing = "0.1"
```

Use the core manager:

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

## Configuration

| Option | Description |
| --- | --- |
| `node_id` | Current node identity used by cluster routing. |
| `heartbeat_interval` | Suggested heartbeat interval for clients. |
| `heartbeat_timeout` | Timeout threshold for inactive sessions. |
| `write_queue_capacity` | Per-session outbound queue capacity. |
| `connection_policy` | `Single` replaces old sessions; `Multi` keeps multiple sessions. |
| `cluster.enabled` | Enables presence registration and remote publishing. |
| `cluster.backend` | Selects `Memory` by default or `Redis { url }` explicitly. |
| `cluster.route_ttl` | Presence route expiration duration. |

## Project Structure

```text
rust-wing/
├─ src/
│  ├─ cluster.rs      Presence store and node publisher abstractions
│  ├─ config.rs       Framework configuration
│  ├─ error.rs        Public error type
│  ├─ identity.rs     Node, user, device, and session IDs
│  ├─ manager.rs      Core RustWing manager
│  ├─ protocol.rs     Message envelope and outbound frame types
│  ├─ session.rs      Session state and write queue
│  └─ lib.rs          Public crate exports
├─ tests/             Core behavior tests
├─ README.md
└─ README_zh.md
```

## Development Commands

```bash
cargo fmt
cargo check
cargo test
```

## Roadmap

- `axum` WebSocket adapter
- Redis presence store
- Redis Pub/Sub node publisher
- NATS backend
- room, channel, and topic broadcast
- authentication middleware
- metrics and tracing integration
- examples and benchmarks

## Security Notes

RustWing is a framework core. Authentication, authorization, rate limiting, TLS termination, and public network exposure should be handled by the application or adapter layer. Do not expose a WebSocket gateway to untrusted networks without a clear authentication and resource-limit strategy.

## Git Notes

The following paths are local build output, IDE state, or personal assistant instructions and should not be committed:

- `target/`
- `.idea/`
- `.vscode/`
- `.env`
- `AGENTS.md`
- `CLAUDE.md`

If these files are already tracked by Git, adding them to `.gitignore` will not untrack them automatically. Use `git rm --cached` before committing.

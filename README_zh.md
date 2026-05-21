<p align="center">
  <img src="./docs/images/logo.png" alt="RustWing logo" width="180" />
</p>

<h1 align="center">RustWing</h1>

<p align="center">
  面向 Rust 实时应用的模块化 WebSocket 框架。
</p>

<p align="center">
  <img alt="Rust" src="https://img.shields.io/badge/Rust-2024-000000?logo=rust&logoColor=white" />
  <img alt="Status" src="https://img.shields.io/badge/status-early--stage-orange" />
  <img alt="License" src="https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue" />
</p>

<p align="center">
  <a href="./README.md">English</a> | 简体中文
</p>

---

RustWing 是一个用于构建实时 WebSocket 服务的 Rust 框架。它提供连接生命周期、会话管理、心跳处理、消息投递和分布式路由等基础能力。

项目采用多个职责清晰的 crate 组成，应用可以从核心能力开始，在需要时再接入 Web 框架集成或外部后端。

## 为什么选择 RustWing

- 用可复用的框架核心承载 WebSocket 连接基础设施。
- 认证、权限和业务消息处理仍然保留在应用自身。
- 支持按产品模型选择单端在线或多端在线。
- 从本地单节点到多节点部署都使用同一套核心 API。
- Web 框架集成独立于核心模块，便于按需组合。

## 快速开始

在应用中添加 RustWing：

```toml
[dependencies]
rust-wing-core = "0.0.1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

如果使用下面的扩展示例，再按需添加对应 crate：

```toml
rust-wing-axum = "0.0.1"
rust-wing-adapter = { version = "0.0.1", features = ["redis"] }
axum = { version = "0.8", features = ["ws"] }
```

创建管理器、接收会话，并向用户发送消息：

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

## Web 框架用法

在应用中先完成认证，构建 `Identity`，再把升级后的连接交给 RustWing：

```rust
use axum::{extract::ws::WebSocketUpgrade, response::Response};
use rust_wing_axum::upgrade;
use rust_wing_core::{Identity, RustWing};

async fn websocket_handler(ws: WebSocketUpgrade, wing: RustWing) -> Response {
    let identity = Identity::new("alice");
    upgrade(ws, wing, identity)
}
```

如果应用需要处理客户端发来的业务消息，可以使用 `upgrade_with_handler`。

## 分布式用法

当服务部署为多个节点时，RustWing 可以接入外部路由后端：

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

| Crate | 作用 |
| --- | --- |
| `rust-wing-core` | 会话、协议消息、路由和连接管理的核心 API。 |
| `rust-wing-adapter` | 面向外部基础设施的适配器契约和后端实现。 |
| `rust-wing-axum` | 面向 Axum 应用的 WebSocket 集成。 |

## 配置

常用配置项：

| 字段 | 作用 |
| --- | --- |
| `node_id` | 当前服务节点标识。 |
| `heartbeat_interval` | 建议客户端使用的心跳间隔。 |
| `heartbeat_timeout` | 不活跃会话的超时时间。 |
| `write_queue_capacity` | 每个会话的出站队列大小。 |
| `connection_policy` | 控制用户单端在线或多端在线。 |
| `cluster.enabled` | 启用分布式路由。 |
| `cluster.route_ttl` | 分布式路由的过期时间。 |

## 当前状态

RustWing 目前处于早期基础阶段。核心 API 已经可以使用，但在第一个稳定版本前，包名、模块边界和适配器 API 仍可能调整。

## 开发

```bash
cargo fmt --all
cargo check --workspace
cargo test
```

## 路线图

- 稳定公共 API。
- 增加完整示例。
- 扩展更多 Web 框架集成。
- 完善分布式部署文档。
- 增加 tracing、metrics 和运维钩子。
- 增加房间、频道和 topic 辅助能力。

## 许可证

本项目使用以下任一许可证：

- Apache License, Version 2.0
- MIT license

详见 [LICENSE](./LICENSE)。

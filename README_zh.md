<p align="center">
  <img src="./docs/images/logo.png" alt="RustWing" width="120"/>
</p>

<h1 align="center">RustWing</h1>

<p align="center">
  面向 Rust 实时服务的模块化 WebSocket 基础框架。
</p>

<p align="center">
  <img alt="Rust" src="https://img.shields.io/badge/Rust-2024-000000?logo=rust&logoColor=white" />
  <img alt="Status" src="https://img.shields.io/badge/status-early--stage-orange" />
  <img alt="License" src="https://img.shields.io/badge/license-see%20LICENSE-blue" />
</p>

<p align="center">
  <a href="./README.md">English</a> | 简体中文
</p>

---

RustWing 是一个处于早期阶段的 Rust WebSocket 框架，用于构建实时服务。
它专注于很多 WebSocket 应用都会重复实现的基础能力：连接生命周期、会话索引、
心跳处理、有界出站队列、按用户投递消息，以及分布式路由。

项目由多个职责清晰的 crate 组成。应用可以只使用核心连接管理器，也可以接入
Axum WebSocket 集成，或在需要多节点投递时接入 Redis 等外部基础设施。

## 功能特性

- 使用强类型表示用户、设备、会话和节点标识。
- 支持每个用户单会话在线或多会话在线。
- 使用有界出站队列，并明确处理背压。
- 支持按用户本地投递和本地广播。
- 内置心跳确认和不活跃会话回收。
- 通过在线状态存储和节点发布器支持可选的集群路由。
- 提供 adapter bridge，便于接入自定义基础设施。
- 提供 Axum WebSocket 升级集成。
- Redis 在线状态、发布器和订阅器通过 `redis` feature 启用。

## Workspace

| Crate | 作用 |
| --- | --- |
| `rust-wing-core` | 核心会话、协议、路由、心跳和管理器 API。 |
| `rust-wing-axum` | 基于 `rust-wing-core` 的 Axum WebSocket 集成。 |
| `rust-wing-adapter` | Adapter 契约、内存 adapter，以及可选 Redis 后端组件。 |

## 当前状态

RustWing 目前可以作为基础框架使用，但公共 API 仍处于稳定前阶段。在稳定版本发布前，
crate 边界、配置命名和 adapter 细节仍可能调整。

## 安装

按需添加 crate：

```toml
[dependencies]
rust-wing-core = "0.0.1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

如果使用 Axum 集成：

```toml
rust-wing-axum = "0.0.1"
axum = { version = "0.8", features = ["ws"] }
```

如果使用 Redis 集群 adapter：

```toml
rust-wing-adapter = { version = "0.0.1", features = ["redis"] }
```

在本仓库内开发时，可以直接引用 workspace 路径：

```toml
rust-wing-core = { path = "rust-wing-core" }
rust-wing-axum = { path = "rust-wing-axum" }
rust-wing-adapter = { path = "rust-wing-adapter", features = ["redis"] }
```

## 快速开始

创建管理器，接收一个会话，并向用户发送一帧消息：

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

## Axum 用法

应用先完成请求认证，构建 `Identity`，然后将升级后的 WebSocket 交给 RustWing：

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

如果应用需要处理非心跳的客户端消息，可以使用 `upgrade_with_handler`。
实现 `AxumMessageHandler` 后，业务认证、授权和 payload 校验仍应保留在应用层。

## 心跳协议

Axum 集成会识别 JSON 文本帧形式的 RustWing 协议消息。客户端可以发送如下心跳：

```json
{
  "type": "heartbeat",
  "event": "client_report",
  "data": {
    "client_time": 1716000000000
  }
}
```

RustWing 会回复 `heartbeat_ack` 消息，其中包含回显的客户端时间戳、服务端心跳时间戳、
配置的心跳间隔和超时时间。

## 连接策略

RustWing 默认使用 `ConnectionPolicy::Single`，即每个用户只保留一个活跃会话。
当同一用户重新连接时，旧会话会被关闭。

如果一个用户可以同时从多个设备或多个浏览器标签页连接，请使用
`ConnectionPolicy::Multi`：

```rust
use rust_wing_core::{ConnectionPolicy, RustWing, RustWingConfig};

let wing = RustWing::new(RustWingConfig {
    connection_policy: ConnectionPolicy::Multi,
    ..RustWingConfig::default()
});
```

## 集群路由

集群路由默认关闭。启用后，RustWing 会把活跃路由注册到在线状态存储中，并通过节点发布器
发布跨节点消息。

用于进程内测试或本地实验时，可以使用内存后端：

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

如果使用 Redis 路由，可以通过 adapter crate 构建 cluster，并为当前节点运行订阅器：

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

## 配置项

| 字段 | 默认值 | 作用 |
| --- | --- | --- |
| `node_id` | `local` | 标识当前服务节点。 |
| `heartbeat_interval` | `15s` | 返回给客户端的心跳间隔。 |
| `heartbeat_timeout` | `45s` | 会话可被回收前允许的不活跃窗口。 |
| `write_queue_capacity` | `64` | 每个会话的有界出站队列容量。 |
| `connection_policy` | `Single` | 控制用户是单会话还是多会话在线。 |
| `cluster.enabled` | `false` | 启用在线状态注册和远程路由。 |
| `cluster.backend` | `Memory` | `from_config` 使用的配置驱动后端选择。 |
| `cluster.route_ttl` | `90s` | 分布式路由记录的有效期。 |

`RustWingConfig::normalized()` 会把空值或零值替换为安全默认值。

## 开发

本仓库使用 Cargo workspace：

```bash
cargo fmt --all
cargo check --workspace
cargo test
```

修改 Redis 相关代码时，可以运行 feature 相关测试：

```bash
cargo test -p rust-wing-adapter --features redis
```

## 贡献

项目仍处于早期阶段，欢迎参与一起打磨。请保持改动聚焦，为行为变化补充测试，
并在提交 pull request 前运行格式化。

适合贡献的方向：

- 面向真实应用的完整示例。
- 更多 Web 框架集成。
- 更完善的 Redis 生产部署文档。
- tracing、metrics 和运维钩子。
- 基于核心路由模型的房间、频道和 topic 辅助能力。

## 安全说明

RustWing 负责连接基础设施，不负责应用信任边界。应用应在创建 `Identity` 前完成
WebSocket 请求认证，校验所有业务 payload，并在向用户投递消息前执行授权检查。

## 许可证

本项目使用 [LICENSE](./LICENSE) 中声明的许可证条款。

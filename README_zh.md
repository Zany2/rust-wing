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

- 使用强类型表示连接体系、用户、客户端、会话和节点标识。
- 支持多连接体系，并可配置默认策略和体系级策略覆盖。
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

当前 crate 和模块关系图见 [源码架构图](./docs/source-architecture.md)。

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

路由存储和节点间消息通道是两个独立概念，可以自由组合。例如路由使用 Redis
保存，节点消息使用 Kafka、NATS 或业务自定义中间件发送：

```rust
use rust_wing_adapter::{RedisPresenceAdapter, RedisPresenceConfig, rust_wing_from_adapters};
use rust_wing_core::RustWingConfig;

# async fn build() -> rust_wing_core::Result<()> {
let presence = RedisPresenceAdapter::connect(
    RedisPresenceConfig::new("redis://127.0.0.1:6379")
).await?;

let publisher = build_kafka_or_nats_node_publisher().await?;
let wing = rust_wing_from_adapters(
    RustWingConfig::default().with_node_id("node-a"),
    presence,
    publisher,
).await?;
# let _ = wing;
# Ok(())
# }
# async fn build_kafka_or_nats_node_publisher() -> rust_wing_core::Result<impl rust_wing_adapter::NodePublisherAdapter> {
#     struct DemoPublisher;
#     #[async_trait::async_trait]
#     impl rust_wing_adapter::NodePublisherAdapter for DemoPublisher {
#         async fn publish(&self, _node_id: &rust_wing_core::NodeId, _envelope: rust_wing_core::ClusterEnvelope) -> rust_wing_core::Result<()> { Ok(()) }
#     }
#     Ok(DemoPublisher)
# }
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

如果应用需要处理非心跳的客户端消息，可以使用 `upgrade_with_handler`。
实现 `AxumMessageHandler` 后，业务认证、授权和 payload 校验仍应保留在应用层。

如果 WebSocket 模块需要调用其他语言编写的业务系统做认证，可以实现
`AxumAuthenticator`。认证会发生在 WebSocket 升级之前；失败时直接返回 HTTP 响应，
不会创建 session：

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

## 消息确认

出站帧可以通过携带 `message_id` 启用内存确认追踪：

```rust
let message_id = wing.next_message_id();
let frame = OutboundFrame::text("important").require_ack(message_id.clone());
wing.send_to_user("alice", frame).await?;
```

客户端通过如下消息确认投递：

```json
{
  "type": "ack",
  "event": "ack",
  "data": {
    "message_id": "node-a-1716000000000-1",
    "stage": "client_received",
    "client_time": 1716000000100
  }
}
```

当前阶段包括 `client_received` 和 `business_processed`。可以使用
`ack_snapshot(message_id)` 查看本地确认状态，或使用
`wait_for_ack(message_id, stage, timeout)` 等待全部已知本地目标达到指定确认阶段。
确认追踪条目会在 `ack_ttl` 后过期；可以调用 `reap_expired_acks()` 或
`ack_pending_count()` 清理内存追踪器。这是一层内存回执基础；持久化重投和跨节点
ACK 聚合后续应放在可靠性层继续完善。

## 连接体系

RustWing 默认使用 `ConnectionPolicy::UniqueClient`，即每个 `(user_id, client_id)`
组合只保留一个活跃会话。现在实际隔离维度是
`(connection_type, user_id, client_id)`，因此同一个用户和客户端可以在
`user`、`admin`、`game` 等不同连接体系中同时在线，互不顶替。

如果应用只有一个连接体系，可以使用默认快捷方法。框架内部仍然会使用
`connection_type = "default"` 保存路由：

```rust
use rust_wing_core::Identity;

let identity = Identity::default_connection("alice").with_client("browser");

wing.send_to_user("alice", frame).await?;
wing.send_to_client("alice", Some("browser"), frame).await?;
wing.broadcast(frame).await?;
```

如果一个用户只能保留一个连接，请使用 `ConnectionPolicy::UniqueUser`；如果同一客户端的重复连接也都需要保留，请使用
`ConnectionPolicy::MultiSession`。通过配置辅助方法设置常规策略，并覆盖特定连接体系：

```rust
use rust_wing_core::{ConnectionPolicy, RustWing, RustWingConfig};

let wing = RustWing::new(
    RustWingConfig::default()
        .with_default_connection_policy(ConnectionPolicy::UniqueClient)
        .with_connection_policy("game", ConnectionPolicy::MultiSession),
);
```

投递目标与连接策略相互独立：

- `send_to_user(user_id, frame)` 会发送给默认连接体系内该用户的全部活跃会话。
- `send_to_client(user_id, client_id, frame)` 会发送给默认连接体系内某个客户端槽位。
- `broadcast(frame)` 会在集群内广播给默认连接体系的全部会话。
- `send_to_user_in(connection_type, user_id, frame)` 会发送给该连接体系内该用户的全部活跃会话。
- `send_to_client_in(connection_type, user_id, client_id, frame)` 会发送给该连接体系内该用户某个客户端槽位下的全部活跃会话。
- `send_to_session(session_id, frame)` 会发送给一条精确的本地或远端会话。
- `broadcast_in(connection_type, frame)` 会在集群内广播给某个连接体系的全部会话。
- `broadcast_all(frame)` 会广播给全部本地与远端会话。
- `broadcast_local(frame)` 只广播当前节点。

断开连接也遵循默认体系优先的命名：

- `disconnect_user(user_id, reason)` 会断开默认连接体系内该用户的全部会话。
- `disconnect_client(user_id, client_id, reason)` 会断开默认连接体系内某个客户端槽位。
- `disconnect_session(session_id, reason)` 会断开一条精确的本地或远端会话。
- `disconnect_user_in(connection_type, user_id, reason)` 会断开指定连接体系内某个用户的全部会话。
- `disconnect_client_in(connection_type, user_id, client_id, reason)` 会断开指定连接体系内某个客户端槽位。

## 外部发送接口

`rust-wing-axum` 可以暴露一个可复用 HTTP router，方便 Go、Java、PHP、Node
等业务系统调用 Rust WebSocket 网关发送消息。生产环境一定要给这个 router 加保护器：

```rust
use rust_wing_axum::{ApiKeySendApiGuard, send_api_router};
use rust_wing_core::RustWing;

fn internal_router(wing: RustWing) -> axum::Router {
    send_api_router(wing, ApiKeySendApiGuard::new("internal-secret"))
}
```

该 router 提供：

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
- `GET /ack/{message_id}`
- `POST /ack/wait`
- `GET /stats`
- `GET /cluster/nodes`
- `GET /cluster/routes`
- `GET /systems/{connection_type}/routes`

发送请求体可以包含 `"require_ack": true` 和可选 `"message_id"`。如果
`require_ack` 为 true 且没有传入 id，RustWing 会自动生成并在响应中返回。
外部消息组件使用同一套目标模型，也可以通过 `disconnect_user`、
`disconnect_client`、`disconnect_session` 目标断开用户、客户端槽位或精确会话。

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

如果使用 Redis 路由，推荐使用 adapter crate 提供的托管运行时。它会创建
Redis 在线状态存储、节点发布器和当前节点订阅任务，并返回可直接使用的 `RustWing`：

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

    // 服务优雅退出时：
    runtime.shutdown().await?;

    Ok(())
}
```

如果应用需要分别控制 Redis key 前缀、频道前缀或订阅任务，也可以继续使用
`redis_cluster_parts_from_config(...)` 手动组装 cluster 和 subscriber。

分布式部署时，每个运行实例都需要唯一的 `node_id`。可以通过
`with_node_id(...)` 显式设置，也可以使用 `RustWingConfig::from_env()`
读取 `RUST_WING_NODE_ID`：

```rust
let wing = RustWing::new(RustWingConfig::from_env());
```

推荐取值：

| 环境 | 推荐 `node_id` |
| --- | --- |
| Kubernetes | Pod Name，例如 `rust-wing-ws-0` 或 Deployment 生成的 Pod 名。 |
| Docker Compose | 容器 hostname 或容器名。 |
| 裸机部署 | `hostname:port`，避免同一机器多实例冲突。 |
| 云服务器 | 优先使用 instance id 加端口。 |
| 本地开发 | 默认 `local` 即可。 |

集群启动建议使用 `RustWing::with_cluster_checked(...)` 或
`RustWing::from_config(...)`。这些构造方法会注册并持续刷新短期节点租约，
如果同一个 `node_id` 已经被另一个活跃实例占用，会直接返回错误。广播路由只会使用
仍持有有效租约的节点，因此旧节点记录不会继续接收新的集群信封。

服务优雅关闭时调用 `shutdown().await`，用于注销本地会话并释放节点租约：

```rust
let closed = wing.shutdown().await?;
```

## 配置项

| 字段 | 默认值 | 作用 |
| --- | --- | --- |
| `node_id` | `local`，或通过 `from_env()` 读取 `RUST_WING_NODE_ID` | 标识当前服务节点。 |
| `heartbeat_interval` | `30s` | 返回给客户端的心跳间隔。 |
| `heartbeat_timeout` | `90s` | 会话可被回收前允许的不活跃窗口。 |
| `write_queue_capacity` | `64` | 每个会话的有界出站队列容量。 |
| `ack_ttl` | `300s` | 确认追踪条目的内存保留时间。 |
| `default_connection_policy` | `UniqueClient` | 控制没有体系级覆盖时的会话共存方式。 |
| `connection_policies` | 空 | 覆盖特定连接体系的会话共存策略。 |
| `cluster.enabled` | `false` | 在注入 adapter 集群依赖后启用在线状态注册和远程路由。 |
| `cluster.route_ttl` | `90s` | 分布式路由记录的有效期。 |
| `cluster.node_lease_ttl` | `30s` | 重复节点保护租约的有效期。 |

`RustWingConfig::normalized()` 会把空值或零值替换为安全默认值。
`rust-wing-core` 不通过配置选择 Redis、Kafka、NATS 或 Memory 后端。分布式路由存储和节点消息通道由 `rust-wing-adapter` 注入，因此可以自由组合，例如 Redis 存路由、Kafka/NATS 传节点消息。

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


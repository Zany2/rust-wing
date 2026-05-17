<h1 align="center">RustWing</h1>

<p align="center">
  <img alt="Rust" src="https://img.shields.io/badge/Rust-2024-000000?logo=rust&logoColor=white" />
  <img alt="Tokio" src="https://img.shields.io/badge/Tokio-1-5E5CE6" />
  <img alt="Serde" src="https://img.shields.io/badge/Serde-1-3B82F6" />
  <img alt="Status" src="https://img.shields.io/badge/status-core%20foundation-orange" />
</p>

<p align="center">
  <a href="./README.md">English</a> · 简体中文
</p>

> **一个面向 Rust 的分布式 WebSocket 框架内核。** RustWing 为可水平扩展的实时服务提供可复用的会话管理、有界写队列、协议信封、本地投递、在线路由和集群传输抽象。

```bash
cargo test
```

## 项目定位

RustWing 不是某个业务项目里的 IM 工具包，而是一个可复用的 WebSocket 框架内核。它把实时服务中最关键的能力拆成小而清晰的模块。

当前项目重点是核心库。Web 框架适配层和分布式后端会作为可插拔层接入，而不是和会话管理器强绑定。

## 核心能力

- **会话生命周期**：接收用户身份、创建会话 ID、保存快照、注销会话。
- **连接策略**：支持单用户单连接和单用户多连接。
- **有界写队列**：每个会话拥有独立的有界 outbound channel，并明确背压行为。
- **心跳生命周期**：记录心跳、返回确认时序数据，并回收不活跃会话。
- **协议信封**：提供基于 `serde` 的版本化消息和帧类型。
- **本地投递**：支持本地用户发送和本地会话广播。
- **在线路由**：抽象多会话用户路由的注册、续期、查找和删除。
- **集群发布**：抽象节点间消息发布，可接入 Redis、NATS 或自定义后端。
- **内存后端**：提供用于测试和示例的内存 presence store。

## 架构

```text
application
  |
web framework adapter
  |  axum / hyper / tokio-tungstenite
  |
rust-wing core
  |-- protocol: 版本化消息信封和帧类型
  |-- session: 连接身份、快照、写队列
  |-- manager: 本地注册表、连接策略、发送和广播
  |-- cluster: presence store 和 node publisher 抽象
  |
cluster backends
  |-- redis
  |-- nats
  |-- memory/testing
```

## 投递模型

| 步骤 | 责任方 | 说明 |
| --- | --- | --- |
| 接入 | `RustWing::accept` | 注册已认证身份并创建会话。 |
| 写回 | web adapter | 持有 outbound receiver，并把 frame 写入 socket。 |
| 本地发送 | manager | 优先投递到本地会话。 |
| 远端路由 | presence store | 查找目标用户所在节点。 |
| 远端发布 | node publisher | 把 frame 转发到目标节点通道。 |
| 远端落地 | manager | 处理集群 envelope 并投递到本地会话。 |

## 快速开始

在应用 crate 中添加 RustWing：

```toml
[dependencies]
rust-wing = "0.1"
```

使用核心管理器：

```rust
use rust_wing::{Identity, OutboundFrame, RustWing, RustWingConfig};

#[tokio::main]
async fn main() -> rust_wing::Result<()> {
    let wing = RustWing::new(RustWingConfig::default());
    let accepted = wing.accept(Identity::new("alice")).await?;

    wing.send_to_user("alice", OutboundFrame::text("hello")).await?;

    // Web 框架适配层会持有这个 receiver，并把 frame 写回 socket。
    let mut outbound = accepted.outbound;
    if let Some(frame) = outbound.recv().await {
        println!("send frame: {:?}", frame.kind);
    }

    Ok(())
}
```

## 配置

| 配置项 | 说明 |
| --- | --- |
| `node_id` | 当前节点标识，用于集群路由。 |
| `heartbeat_interval` | 建议客户端使用的心跳间隔。 |
| `heartbeat_timeout` | 会话不活跃的超时阈值。 |
| `write_queue_capacity` | 每个会话的 outbound 队列容量。 |
| `connection_policy` | `Single` 顶掉旧连接；`Multi` 保留多连接。 |
| `cluster.enabled` | 是否启用 presence 注册和远端发布。 |
| `cluster.backend` | 默认选择 `Memory`，也可显式配置 `Redis { url }`。 |
| `cluster.route_ttl` | presence 路由过期时间。 |

## 项目结构

```text
rust-wing/
├─ src/
│  ├─ cluster.rs      Presence store 和 node publisher 抽象
│  ├─ config.rs       框架配置
│  ├─ error.rs        公共错误类型
│  ├─ identity.rs     节点、用户、设备和会话 ID
│  ├─ manager.rs      RustWing 核心管理器
│  ├─ protocol.rs     消息信封和 outbound frame 类型
│  ├─ session.rs      会话状态和写队列
│  └─ lib.rs          crate 公共导出
├─ tests/             核心行为测试
├─ README.md
└─ README_zh.md
```

## 开发命令

```bash
cargo fmt
cargo check
cargo test
```

## 路线图

- `axum` WebSocket adapter
- Redis presence store
- Redis Pub/Sub node publisher
- NATS backend
- 房间、频道、Topic 广播
- 鉴权中间件
- metrics 和 tracing 集成
- examples 和 benchmark

## 安全说明

RustWing 是框架内核。认证、授权、限流、TLS 终止和公网暴露策略应由应用层或适配层处理。不要在没有清晰认证和资源限制策略的情况下，把 WebSocket 网关暴露到不可信网络。

## Git 注意事项

以下内容属于本地构建产物、IDE 状态或个人助手提示词，不应提交：

- `target/`
- `.idea/`
- `.vscode/`
- `.env`
- `AGENTS.md`
- `CLAUDE.md`

如果这些文件已经被 Git 跟踪，仅加入 `.gitignore` 不会自动取消跟踪，需要执行 `git rm --cached` 后再提交。

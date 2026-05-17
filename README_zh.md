# RustWing

[语言： [English](README.md) | 中文]

RustWing 是一个使用 Rust 编写的开源、通用、分布式 WebSocket 框架。

## 项目定位

RustWing 不是某个业务项目里的 IM 工具包，而是一个可复用的 WebSocket 框架内核。它把连接生命周期、消息协议、本地投递、在线路由和集群传输拆成独立边界，让上层应用可以自由接入不同 Web 框架和分布式后端。

适合场景：

- 即时通信网关
- 通知推送服务
- 协作编辑、在线状态、实时看板
- 游戏、IoT、设备长连接
- 需要水平扩展的 WebSocket 服务

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

## 当前能力

- 单端登录或多端登录连接策略
- 用户、设备、节点、会话 ID 身份模型
- 每个会话独立的有界写队列
- 本地用户发送和本地广播
- 基于 `serde` 的统一消息协议
- 分布式 presence 路由抽象
- 节点间消息发布抽象
- 用于测试和示例的内存 presence store

## 使用示例

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

## 路线图

- `axum` WebSocket adapter
- Redis presence store
- Redis Pub/Sub node publisher
- NATS backend
- 房间、频道、Topic 广播
- 心跳处理和超时回收
- 鉴权中间件
- metrics 和 tracing 集成
- 更多 examples 和 benchmark

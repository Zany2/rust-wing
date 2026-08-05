use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use rust_wing_adapter::{
    MemoryPresenceAdapter, NodePublisherAdapter, PresenceStoreAdapter, cluster_from_adapters,
    rust_wing_from_adapters,
};
use rust_wing_core::{
    ClusterConfig, ClusterEnvelope, ConnectionType, NodeId, OutboundFrame, Result, Route, RustWing,
    RustWingConfig, UserId,
};

// Memory adapter can be bridged into the core cluster 内存适配器可桥接到核心集群
#[tokio::test]
async fn memory_adapter_bridges_into_core_cluster() {
    // Seed a remote route through the adapter 通过适配器写入远端路由
    let presence = MemoryPresenceAdapter::new();
    presence
        .register_node(
            &NodeId::from("node-b"),
            "instance-node-b",
            Duration::from_secs(60),
        )
        .await
        .unwrap();
    presence
        .register(
            Route {
                connection_type: ConnectionType::from("default"),
                user_id: UserId::from("alice"),
                client_id: None,
                session_id: "remote-session".into(),
                node_id: NodeId::from("node-b"),
            },
            Duration::from_secs(60),
        )
        .await
        .unwrap();

    // Build a core cluster from adapter implementations 从适配器实现构建核心集群
    let publisher = RecordingPublisher::default();
    let published = publisher.published.clone();
    let cluster = cluster_from_adapters(presence, publisher);
    let wing = RustWing::with_cluster_checked(
        RustWingConfig {
            node_id: NodeId::from("node-a"),
            cluster: ClusterConfig {
                enabled: true,
                ..ClusterConfig::default()
            },
            ..RustWingConfig::default()
        },
        Some(cluster),
    )
    .await
    .unwrap();

    // Send through the cluster route 通过集群路由发送消息
    let report = wing
        .send_to_user("alice", OutboundFrame::text("hello"))
        .await
        .unwrap();

    // Confirm the bridged publisher received the message 确认桥接发布器收到消息
    let published = published.lock().unwrap();
    assert_eq!(report.local_sessions, 0);
    assert_eq!(report.remote_nodes, 1);
    assert_eq!(published.len(), 1);
    assert_eq!(published[0].0, NodeId::from("node-b"));
}

// RustWing can be built from independently selected adapters RustWing 可以通过独立选择的适配器构建
#[tokio::test]
async fn rust_wing_from_adapters_composes_presence_and_publisher() {
    // Seed a remote route in the selected presence store 在选定路由存储中写入远端路由
    let presence = MemoryPresenceAdapter::new();
    presence
        .register_node(
            &NodeId::from("node-b"),
            "instance-node-b",
            Duration::from_secs(60),
        )
        .await
        .unwrap();
    presence
        .register(
            Route {
                connection_type: ConnectionType::from("default"),
                user_id: UserId::from("alice"),
                client_id: None,
                session_id: "remote-session".into(),
                node_id: NodeId::from("node-b"),
            },
            Duration::from_secs(60),
        )
        .await
        .unwrap();

    // Use an independently selected publisher 使用独立选择的发布器
    let publisher = RecordingPublisher::default();
    let published = publisher.published.clone();
    let wing = rust_wing_from_adapters(
        RustWingConfig::default().with_node_id("node-a"),
        presence,
        publisher,
    )
    .await
    .unwrap();

    let report = wing
        .send_to_user("alice", OutboundFrame::text("hello"))
        .await
        .unwrap();

    let published = published.lock().unwrap();
    assert_eq!(report.local_sessions, 0);
    assert_eq!(report.remote_nodes, 1);
    assert_eq!(published.len(), 1);
    assert_eq!(published[0].0, NodeId::from("node-b"));
}

// Test publisher adapter that records envelopes 测试用记录型发布适配器
#[derive(Default)]
struct RecordingPublisher {
    // Published node and envelope pairs 已发布的节点和信封
    published: Arc<Mutex<Vec<(NodeId, ClusterEnvelope)>>>,
}

#[async_trait]
impl NodePublisherAdapter for RecordingPublisher {
    // Store each publish request for assertions 保存发布请求用于断言
    async fn publish(&self, node_id: &NodeId, envelope: ClusterEnvelope) -> Result<()> {
        self.published
            .lock()
            .unwrap()
            .push((node_id.clone(), envelope));
        Ok(())
    }
}

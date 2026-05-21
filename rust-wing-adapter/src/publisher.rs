use async_trait::async_trait;
use rust_wing_core::{ClusterEnvelope, NodeId, Result};

// Node publisher adapter interface 节点消息发布适配器接口
#[async_trait]
pub trait NodePublisherAdapter: Send + Sync {
    // Publish one envelope to a target node 向目标节点发布一个信封
    async fn publish(&self, node_id: &NodeId, envelope: ClusterEnvelope) -> Result<()>;
}

// Bridge an adapter into the core NodePublisher trait 将适配器桥接为核心发布契约
pub struct NodePublisherBridge<T> {
    // Wrapped adapter implementation 被包装的适配器实现
    inner: T,
}

impl<T> NodePublisherBridge<T> {
    // Create a bridge around one adapter 创建适配器桥接器
    pub fn new(inner: T) -> Self {
        Self { inner }
    }

    // Borrow the wrapped adapter 借用被包装的适配器
    pub fn inner(&self) -> &T {
        &self.inner
    }

    // Consume the bridge and return the adapter 取回被包装的适配器
    pub fn into_inner(self) -> T {
        self.inner
    }
}

#[async_trait]
impl<T> rust_wing_core::NodePublisher for NodePublisherBridge<T>
where
    T: NodePublisherAdapter,
{
    // Publish through the adapter 通过适配器发布消息
    async fn publish(&self, node_id: &NodeId, envelope: ClusterEnvelope) -> Result<()> {
        self.inner.publish(node_id, envelope).await
    }
}

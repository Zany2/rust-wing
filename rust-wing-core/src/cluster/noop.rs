use async_trait::async_trait;

use crate::error::Result;
use crate::identity::NodeId;

use super::{ClusterEnvelope, NodePublisher};

// Publisher that intentionally drops every envelope 有意丢弃所有信封的发布器
#[derive(Debug, Default)]
pub struct NoopPublisher;

#[async_trait]
impl NodePublisher for NoopPublisher {
    // Accept publish requests without side effects 接收发布请求但不产生副作用
    async fn publish(&self, _node_id: &NodeId, _envelope: ClusterEnvelope) -> Result<()> {
        Ok(())
    }
}

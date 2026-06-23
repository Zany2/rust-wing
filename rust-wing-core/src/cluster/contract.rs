use std::time::Duration;

use async_trait::async_trait;

use crate::error::Result;
use crate::identity::{ConnectionType, NodeId, SessionId, UserId};

use super::{ClusterEnvelope, NodeLease, Route};

// Presence persistence contract 在线状态持久化契约
#[async_trait]
pub trait PresenceStore: Send + Sync {
    // Register a route with its lifetime 注册带有效期的路由
    async fn register(&self, route: Route, ttl: Duration) -> Result<()>;

    // Remove one exact route 删除一条精确路由
    async fn remove(
        &self,
        connection_type: &ConnectionType,
        user_id: &UserId,
        session_id: &SessionId,
    ) -> Result<()>;

    // Refresh one exact route 刷新一条精确路由
    async fn touch(
        &self,
        connection_type: &ConnectionType,
        user_id: &UserId,
        session_id: &SessionId,
        ttl: Duration,
    ) -> Result<()>;

    // Locate every current route for a user 查询用户当前全部路由
    async fn locate(
        &self,
        connection_type: &ConnectionType,
        user_id: &UserId,
    ) -> Result<Vec<Route>>;

    // Locate one exact session route 查询一条精确会话路由
    async fn locate_session(&self, session_id: &SessionId) -> Result<Option<Route>>;

    // List live routes in one connection system 列出某个连接体系中的活跃路由
    async fn list_routes(&self, connection_type: &ConnectionType) -> Result<Vec<Route>>;

    // List all live routes across connection systems 列出全部连接体系中的活跃路由
    async fn list_all_routes(&self) -> Result<Vec<Route>>;

    // List nodes that currently own live routes 列出当前拥有活跃路由的节点
    async fn list_nodes(&self) -> Result<Vec<NodeId>>;

    // Register or refresh the current node lease 注册或刷新当前节点租约
    async fn register_node(
        &self,
        node_id: &NodeId,
        instance_id: &str,
        ttl: Duration,
    ) -> Result<NodeLease>;

    // Remove the current node lease when it is still owned by the instance 当前实例仍持有时删除节点租约
    async fn unregister_node(&self, node_id: &NodeId, instance_id: &str) -> Result<()>;
}

// Node-to-node publish contract 节点间发布契约
#[async_trait]
pub trait NodePublisher: Send + Sync {
    // Publish one envelope to a target node 向目标节点发布一个信封
    async fn publish(&self, node_id: &NodeId, envelope: ClusterEnvelope) -> Result<()>;
}

// Cluster integration dependencies 集群集成依赖
pub struct Cluster {
    // Route storage implementation 路由存储实现
    pub presence: Box<dyn PresenceStore>,
    // Remote node publisher 远端节点发布器
    pub publisher: Box<dyn NodePublisher>,
}

impl Cluster {
    // Compose cluster dependencies 组合集群依赖
    pub fn new(
        presence: impl PresenceStore + 'static,
        publisher: impl NodePublisher + 'static,
    ) -> Self {
        Self {
            presence: Box::new(presence),
            publisher: Box::new(publisher),
        }
    }
}

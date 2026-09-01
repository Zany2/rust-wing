use std::time::Duration;

use async_trait::async_trait;
use rust_wing_core::{
    ConnectionPolicy, ConnectionType, NodeId, NodeLease, Result, Route, RouteClaim, RouteRefresh,
    SessionId, UserId,
};

// Presence storage adapter interface 在线路由存储适配器接口
#[async_trait]
pub trait PresenceStoreAdapter: Send + Sync {
    // Register a route with a finite lifetime 注册带有效期的路由
    async fn register(&self, route: Route, ttl: Duration) -> Result<()>;

    // Atomically claim a route according to the connection policy 按连接策略原子仲裁并注册路由
    async fn claim(
        &self,
        route: Route,
        policy: ConnectionPolicy,
        ttl: Duration,
    ) -> Result<RouteClaim>;

    // Remove one exact connection-user-session route 删除指定连接体系用户会话路由
    async fn remove(
        &self,
        connection_type: &ConnectionType,
        user_id: &UserId,
        session_id: &SessionId,
    ) -> Result<()>;

    // Refresh one exact connection-user-session route 刷新指定连接体系用户会话路由
    async fn touch(
        &self,
        connection_type: &ConnectionType,
        user_id: &UserId,
        session_id: &SessionId,
        ttl: Duration,
    ) -> Result<RouteRefresh>;

    // Locate all live routes for one connection-user pair 查询连接体系用户当前可用路由
    async fn locate(
        &self,
        connection_type: &ConnectionType,
        user_id: &UserId,
    ) -> Result<Vec<Route>>;

    // Locate one live route by session id 按会话标识查询一个可用路由
    async fn locate_session(&self, session_id: &SessionId) -> Result<Option<Route>>;

    // List live routes in one connection system 列出某个连接体系中的活跃路由
    async fn list_routes(&self, connection_type: &ConnectionType) -> Result<Vec<Route>>;

    // List all live routes across connection systems 列出全部连接体系中的活跃路由
    async fn list_all_routes(&self) -> Result<Vec<Route>>;

    // List nodes that currently own live routes 列出当前拥有活跃路由的节点
    async fn list_nodes(&self) -> Result<Vec<NodeId>>;

    // Register or refresh one node lease 注册或刷新一个节点租约
    async fn register_node(
        &self,
        node_id: &NodeId,
        instance_id: &str,
        ttl: Duration,
    ) -> Result<NodeLease>;

    // Remove one node lease if still owned by the instance 当实例仍持有时删除节点租约
    async fn unregister_node(&self, node_id: &NodeId, instance_id: &str) -> Result<()>;
}

// Bridge an adapter into the core PresenceStore trait 将适配器桥接为核心存储契约
pub struct PresenceStoreBridge<T> {
    // Wrapped adapter implementation 被包装的适配器实现
    inner: T,
}

impl<T> PresenceStoreBridge<T> {
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
impl<T> rust_wing_core::PresenceStore for PresenceStoreBridge<T>
where
    T: PresenceStoreAdapter,
{
    // Register a route through the adapter 通过适配器注册路由
    async fn register(&self, route: Route, ttl: Duration) -> Result<()> {
        self.inner.register(route, ttl).await
    }

    // Claim a route through the adapter 通过适配器原子仲裁路由
    async fn claim(
        &self,
        route: Route,
        policy: ConnectionPolicy,
        ttl: Duration,
    ) -> Result<RouteClaim> {
        self.inner.claim(route, policy, ttl).await
    }

    // Remove a route through the adapter 通过适配器删除路由
    async fn remove(
        &self,
        connection_type: &ConnectionType,
        user_id: &UserId,
        session_id: &SessionId,
    ) -> Result<()> {
        self.inner
            .remove(connection_type, user_id, session_id)
            .await
    }

    // Refresh a route through the adapter 通过适配器刷新路由
    async fn touch(
        &self,
        connection_type: &ConnectionType,
        user_id: &UserId,
        session_id: &SessionId,
        ttl: Duration,
    ) -> Result<RouteRefresh> {
        self.inner
            .touch(connection_type, user_id, session_id, ttl)
            .await
    }

    // Locate routes through the adapter 通过适配器查询路由
    async fn locate(
        &self,
        connection_type: &ConnectionType,
        user_id: &UserId,
    ) -> Result<Vec<Route>> {
        self.inner.locate(connection_type, user_id).await
    }

    // Locate a session route through the adapter 通过适配器查询会话路由
    async fn locate_session(&self, session_id: &SessionId) -> Result<Option<Route>> {
        self.inner.locate_session(session_id).await
    }

    // List routes through the adapter 通过适配器列出路由
    async fn list_routes(&self, connection_type: &ConnectionType) -> Result<Vec<Route>> {
        self.inner.list_routes(connection_type).await
    }

    // List all routes through the adapter 通过适配器列出全部路由
    async fn list_all_routes(&self) -> Result<Vec<Route>> {
        self.inner.list_all_routes().await
    }

    // List route-owning nodes through the adapter 通过适配器列出路由节点
    async fn list_nodes(&self) -> Result<Vec<NodeId>> {
        self.inner.list_nodes().await
    }

    // Register a node lease through the adapter 通过适配器注册节点租约
    async fn register_node(
        &self,
        node_id: &NodeId,
        instance_id: &str,
        ttl: Duration,
    ) -> Result<NodeLease> {
        self.inner.register_node(node_id, instance_id, ttl).await
    }

    // Remove a node lease through the adapter 通过适配器删除节点租约
    async fn unregister_node(&self, node_id: &NodeId, instance_id: &str) -> Result<()> {
        self.inner.unregister_node(node_id, instance_id).await
    }
}

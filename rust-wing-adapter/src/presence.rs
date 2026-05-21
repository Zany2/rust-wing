use std::time::Duration;

use async_trait::async_trait;
use rust_wing_core::{Result, Route, SessionId, UserId};

// Presence storage adapter interface 在线路由存储适配器接口
#[async_trait]
pub trait PresenceStoreAdapter: Send + Sync {
    // Register a route with a finite lifetime 注册带有效期的路由
    async fn register(&self, route: Route, ttl: Duration) -> Result<()>;

    // Remove one exact user-session route 删除指定用户会话路由
    async fn remove(&self, user_id: &UserId, session_id: &SessionId) -> Result<()>;

    // Refresh one exact user-session route 刷新指定用户会话路由
    async fn touch(&self, user_id: &UserId, session_id: &SessionId, ttl: Duration) -> Result<()>;

    // Locate all live routes for one user 查询用户当前可用路由
    async fn locate(&self, user_id: &UserId) -> Result<Vec<Route>>;
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

    // Remove a route through the adapter 通过适配器删除路由
    async fn remove(&self, user_id: &UserId, session_id: &SessionId) -> Result<()> {
        self.inner.remove(user_id, session_id).await
    }

    // Refresh a route through the adapter 通过适配器刷新路由
    async fn touch(&self, user_id: &UserId, session_id: &SessionId, ttl: Duration) -> Result<()> {
        self.inner.touch(user_id, session_id, ttl).await
    }

    // Locate routes through the adapter 通过适配器查询路由
    async fn locate(&self, user_id: &UserId) -> Result<Vec<Route>> {
        self.inner.locate(user_id).await
    }
}

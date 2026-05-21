use std::collections::HashMap;
use std::sync::RwLock;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use rust_wing_core::{Result, Route, RustWingError, SessionId, UserId};

use crate::PresenceStoreAdapter;

// In-memory presence adapter for development and tests 开发和测试用内存路由适配器
#[derive(Debug, Default)]
pub struct MemoryPresenceAdapter {
    // User-to-session route index 用户到会话路由的索引
    routes: RwLock<HashMap<UserId, HashMap<SessionId, MemoryRoute>>>,
}

// Stored route plus expiration metadata 已存路由及其过期元数据
#[derive(Debug, Clone)]
struct MemoryRoute {
    // Stored routing value 已存储的路由值
    route: Route,
    // Expiration instant 过期时刻
    expires_at: Instant,
}

impl MemoryPresenceAdapter {
    // Create an empty in-memory adapter 创建空的内存适配器
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl PresenceStoreAdapter for MemoryPresenceAdapter {
    // Insert or replace a user route 插入或替换用户路由
    async fn register(&self, route: Route, ttl: Duration) -> Result<()> {
        // Acquire exclusive access before changing routes 修改路由前获取独占访问
        let mut routes = self
            .routes
            .write()
            .map_err(|_| RustWingError::Cluster("presence adapter lock poisoned".into()))?;
        // Keep one route entry for each live session 为每个活跃会话保留一条路由
        routes.entry(route.user_id.clone()).or_default().insert(
            route.session_id.clone(),
            MemoryRoute {
                route,
                expires_at: Instant::now() + ttl,
            },
        );
        Ok(())
    }

    // Remove one exact user-session route 删除指定用户会话路由
    async fn remove(&self, user_id: &UserId, session_id: &SessionId) -> Result<()> {
        // Acquire exclusive access before changing routes 修改路由前获取独占访问
        let mut routes = self
            .routes
            .write()
            .map_err(|_| RustWingError::Cluster("presence adapter lock poisoned".into()))?;
        // Remove the exact session route 删除精确匹配的会话路由
        if let Some(entries) = routes.get_mut(user_id) {
            entries.remove(session_id);
        }
        // Drop empty user buckets 清理空的用户路由桶
        if routes.get(user_id).is_some_and(HashMap::is_empty) {
            routes.remove(user_id);
        }
        Ok(())
    }

    // Extend one exact user-session route 延长指定用户会话路由
    async fn touch(&self, user_id: &UserId, session_id: &SessionId, ttl: Duration) -> Result<()> {
        // Acquire exclusive access before refreshing routes 刷新路由前获取独占访问
        let mut routes = self
            .routes
            .write()
            .map_err(|_| RustWingError::Cluster("presence adapter lock poisoned".into()))?;
        // Refresh only the matching live route 只刷新匹配的活跃路由
        if let Some(entries) = routes.get_mut(user_id) {
            if let Some(entry) = entries.get_mut(session_id) {
                entry.expires_at = Instant::now() + ttl;
            }
        }
        Ok(())
    }

    // Read current non-expired routes 读取当前未过期路由
    async fn locate(&self, user_id: &UserId) -> Result<Vec<Route>> {
        // Acquire exclusive access because expired routes may be removed 查询时可能清理过期路由
        let mut routes = self
            .routes
            .write()
            .map_err(|_| RustWingError::Cluster("presence adapter lock poisoned".into()))?;
        // Return no routes when the user is unknown 用户没有路由时直接返回空结果
        let Some(entries) = routes.get_mut(user_id) else {
            return Ok(Vec::new());
        };
        // Remove stale entries before returning live routes 返回前先移除过期条目
        let now = Instant::now();
        entries.retain(|_, entry| entry.expires_at > now);
        // Drop empty user buckets after cleanup 清理后移除空的用户路由桶
        if entries.is_empty() {
            routes.remove(user_id);
            return Ok(Vec::new());
        }
        // Clone live routes for the caller 为调用方复制活跃路由
        Ok(entries.values().map(|entry| entry.route.clone()).collect())
    }
}

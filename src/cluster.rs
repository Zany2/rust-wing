use std::collections::HashMap;
use std::sync::RwLock;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::error::{Result, RustWingError};
use crate::identity::{NodeId, SessionId, UserId};
use crate::protocol::{FrameKind, OutboundFrame};

// Cluster routing entry 集群路由条目
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Route {
    // Routed user identifier 被路由的用户标识
    pub user_id: UserId,
    // Active session identifier 活跃会话标识
    pub session_id: SessionId,
    // Owning node identifier 所属节点标识
    pub node_id: NodeId,
}

// Cross-node frame envelope 跨节点帧信封
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterEnvelope {
    // Target user identifier 目标用户标识
    pub user_id: UserId,
    // Original frame kind 原始帧类别
    pub frame_kind: FrameKind,
    // Original frame payload 原始帧负载
    pub payload: Vec<u8>,
}

impl ClusterEnvelope {
    // Convert a frame into a cluster envelope 将帧转换为集群信封
    pub fn new(user_id: UserId, frame: OutboundFrame) -> Self {
        Self {
            user_id,
            frame_kind: frame.kind,
            payload: frame.payload,
        }
    }

    // Recover the original outbound frame 还原原始出站帧
    pub fn into_frame(self) -> OutboundFrame {
        OutboundFrame {
            kind: self.frame_kind,
            payload: self.payload,
        }
    }
}

// Presence persistence contract 在线状态持久化契约
#[async_trait]
pub trait PresenceStore: Send + Sync {
    // Register a route with its lifetime 注册带有效期的路由
    async fn register(&self, route: Route, ttl: Duration) -> Result<()>;
    // Remove one exact route 删除一条精确路由
    async fn remove(&self, user_id: &UserId, session_id: &SessionId) -> Result<()>;
    // Refresh one exact route 刷新一条精确路由
    async fn touch(&self, user_id: &UserId, session_id: &SessionId, ttl: Duration) -> Result<()>;
    // Locate every current route for a user 查询用户当前全部路由
    async fn locate(&self, user_id: &UserId) -> Result<Vec<Route>>;
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

// In-memory presence store for local use 本地使用的内存在线状态存储
#[derive(Debug, Default)]
pub struct MemoryPresenceStore {
    // User-to-session route index 用户到会话路由的索引
    routes: RwLock<HashMap<UserId, HashMap<SessionId, MemoryRoute>>>,
}

// Route plus its expiration metadata 路由及其过期元数据
#[derive(Debug, Clone)]
struct MemoryRoute {
    // Stored routing value 已存储的路由值
    route: Route,
    // Expiration instant 过期时刻
    expires_at: Instant,
}

impl MemoryPresenceStore {
    // Create an empty in-memory store 创建空的内存存储
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl PresenceStore for MemoryPresenceStore {
    // Insert or replace a user route 插入或替换用户路由
    async fn register(&self, route: Route, ttl: Duration) -> Result<()> {
        // Acquire exclusive access before mutating the route table 修改路由表前获取独占访问
        let mut routes = self
            .routes
            .write()
            .map_err(|_| RustWingError::Cluster("presence store lock poisoned".into()))?;
        // Keep one route entry per live session 为每个活跃会话保留一条路由
        routes.entry(route.user_id.clone()).or_default().insert(
            route.session_id.clone(),
            MemoryRoute {
                route,
                expires_at: Instant::now() + ttl,
            },
        );
        Ok(())
    }

    // Remove the route only when it still matches the session 仅在仍匹配该会话时删除路由
    async fn remove(&self, user_id: &UserId, session_id: &SessionId) -> Result<()> {
        // Acquire exclusive access before mutating the route table 修改路由表前获取独占访问
        let mut routes = self
            .routes
            .write()
            .map_err(|_| RustWingError::Cluster("presence store lock poisoned".into()))?;
        // Remove only the exact session route 仅移除精确匹配的会话路由
        if let Some(entries) = routes.get_mut(user_id) {
            entries.remove(session_id);
        }
        // Drop the user bucket after its last route disappears 最后一条路由消失后移除用户桶
        if routes.get(user_id).is_some_and(HashMap::is_empty) {
            routes.remove(user_id);
        }
        Ok(())
    }

    // Extend one matching route lifetime 延长一条匹配路由的生命周期
    async fn touch(&self, user_id: &UserId, session_id: &SessionId, ttl: Duration) -> Result<()> {
        // Acquire exclusive access before refreshing metadata 刷新元数据前获取独占访问
        let mut routes = self
            .routes
            .write()
            .map_err(|_| RustWingError::Cluster("presence store lock poisoned".into()))?;
        // Refresh only the route owned by the same live session 仅刷新同一活跃会话拥有的路由
        if let Some(entries) = routes.get_mut(user_id) {
            if let Some(entry) = entries.get_mut(session_id) {
                entry.expires_at = Instant::now() + ttl;
            }
        }
        Ok(())
    }

    // Read the current non-expired routes 读取当前未过期路由
    async fn locate(&self, user_id: &UserId) -> Result<Vec<Route>> {
        // Acquire exclusive access because expired entries may be purged 查询时可能清理过期项，因此获取独占访问
        let mut routes = self
            .routes
            .write()
            .map_err(|_| RustWingError::Cluster("presence store lock poisoned".into()))?;
        // Return early when no route is known 用户没有路由时直接返回
        let Some(entries) = routes.get_mut(user_id) else {
            return Ok(Vec::new());
        };
        // Remove stale entries before reporting routes 返回路由前先移除过期项
        let now = Instant::now();
        entries.retain(|_, entry| entry.expires_at > now);
        // Remove the user bucket when every route expired 全部路由过期后移除用户桶
        if entries.is_empty() {
            routes.remove(user_id);
            return Ok(Vec::new());
        }
        // Clone every live route for the caller 为调用方克隆全部活跃路由
        Ok(entries.values().map(|entry| entry.route.clone()).collect())
    }
}

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

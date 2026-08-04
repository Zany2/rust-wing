use std::collections::HashMap;
use std::sync::RwLock;
use std::time::{Duration, Instant};

use async_trait::async_trait;

use crate::error::{Result, RustWingError};
use crate::identity::{ConnectionType, NodeId, SessionId, UserId};

use super::{NodeLease, PresenceStore, Route};

// In-memory presence store for local use 本地使用的内存在线状态存储
#[derive(Debug, Default)]
pub struct MemoryPresenceStore {
    // Connection-user-to-session route index 连接体系用户到会话路由的索引
    routes: RwLock<HashMap<PresenceKey, HashMap<SessionId, MemoryRoute>>>,
    // Node leases keyed by node id 按节点标识存储的节点租约
    node_leases: RwLock<HashMap<NodeId, MemoryNodeLease>>,
}

// Route plus its expiration metadata 路由及其过期元数据
#[derive(Debug, Clone)]
struct MemoryRoute {
    // Stored routing value 已存储的路由值
    route: Route,
    // Expiration instant 过期时刻
    expires_at: Instant,
}

// Stored node lease plus expiration metadata 已存节点租约及其过期元数据
#[derive(Debug, Clone)]
struct MemoryNodeLease {
    // Runtime instance that owns the lease 持有租约的运行实例
    instance_id: String,
    // Expiration instant 过期时刻
    expires_at: Instant,
}

// Presence lookup key 在线状态查询键
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PresenceKey {
    // Connection system identifier 连接体系标识
    connection_type: ConnectionType,
    // Routed user identifier 被路由的用户标识
    user_id: UserId,
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
        let key = PresenceKey::from_route(&route);
        routes.entry(key).or_default().insert(
            route.session_id.clone(),
            MemoryRoute {
                route,
                expires_at: Instant::now() + ttl,
            },
        );
        Ok(())
    }

    // Remove the route only when it still matches the session 仅在仍匹配该会话时删除路由
    async fn remove(
        &self,
        connection_type: &ConnectionType,
        user_id: &UserId,
        session_id: &SessionId,
    ) -> Result<()> {
        // Acquire exclusive access before mutating the route table 修改路由表前获取独占访问
        let mut routes = self
            .routes
            .write()
            .map_err(|_| RustWingError::Cluster("presence store lock poisoned".into()))?;
        // Remove only the exact session route 仅移除精确匹配的会话路由
        let key = PresenceKey::new(connection_type.clone(), user_id.clone());
        if let Some(entries) = routes.get_mut(&key) {
            entries.remove(session_id);
        }
        // Drop the user bucket after its last route disappears 最后一条路由消失后移除用户桶
        if routes.get(&key).is_some_and(HashMap::is_empty) {
            routes.remove(&key);
        }
        Ok(())
    }

    // Extend one matching route lifetime 延长一条匹配路由的生命周期
    async fn touch(
        &self,
        connection_type: &ConnectionType,
        user_id: &UserId,
        session_id: &SessionId,
        ttl: Duration,
    ) -> Result<()> {
        // Acquire exclusive access before refreshing metadata 刷新元数据前获取独占访问
        let mut routes = self
            .routes
            .write()
            .map_err(|_| RustWingError::Cluster("presence store lock poisoned".into()))?;
        // Refresh only the route owned by the same live session 仅刷新同一活跃会话拥有的路由
        let key = PresenceKey::new(connection_type.clone(), user_id.clone());
        if let Some(entries) = routes.get_mut(&key) {
            if let Some(entry) = entries.get_mut(session_id) {
                entry.expires_at = Instant::now() + ttl;
            }
        }
        Ok(())
    }

    // Read the current non-expired routes 读取当前未过期路由
    async fn locate(
        &self,
        connection_type: &ConnectionType,
        user_id: &UserId,
    ) -> Result<Vec<Route>> {
        // Acquire exclusive access because expired entries may be purged 查询时可能清理过期项，因此获取独占访问
        let mut routes = self
            .routes
            .write()
            .map_err(|_| RustWingError::Cluster("presence store lock poisoned".into()))?;
        // Return early when no route is known 用户没有路由时直接返回
        let key = PresenceKey::new(connection_type.clone(), user_id.clone());
        let Some(entries) = routes.get_mut(&key) else {
            return Ok(Vec::new());
        };
        // Remove stale entries before reporting routes 返回路由前先移除过期项
        let now = Instant::now();
        let live_nodes = self.live_node_ids(now)?;
        entries.retain(|_, entry| entry.expires_at > now);
        // Remove the user bucket when every route expired 全部路由过期后移除用户桶
        if entries.is_empty() {
            routes.remove(&key);
            return Ok(Vec::new());
        }
        // Clone routes owned by live nodes for the caller 为调用方克隆活跃节点拥有的路由
        Ok(entries
            .values()
            .filter(|entry| live_nodes.contains(&entry.route.node_id))
            .map(|entry| entry.route.clone())
            .collect())
    }

    // Locate one current route by session id 按会话标识查询当前路由
    async fn locate_session(&self, session_id: &SessionId) -> Result<Option<Route>> {
        // Acquire exclusive access because expired entries may be purged 查询时可能清理过期项，因此获取独占访问
        let mut routes = self
            .routes
            .write()
            .map_err(|_| RustWingError::Cluster("presence store lock poisoned".into()))?;
        let now = Instant::now();
        let live_nodes = self.live_node_ids(now)?;
        let keys = routes.keys().cloned().collect::<Vec<_>>();
        for key in keys {
            let Some(entries) = routes.get_mut(&key) else {
                continue;
            };
            entries.retain(|_, entry| entry.expires_at > now);
            if let Some(entry) = entries.get(session_id) {
                if live_nodes.contains(&entry.route.node_id) {
                    return Ok(Some(entry.route.clone()));
                }
                return Ok(None);
            }
            if entries.is_empty() {
                routes.remove(&key);
            }
        }
        Ok(None)
    }

    // List live routes in one connection system 列出某个连接体系中的活跃路由
    async fn list_routes(&self, connection_type: &ConnectionType) -> Result<Vec<Route>> {
        // Acquire exclusive access because expired entries may be purged 查询时可能清理过期项，因此获取独占访问
        let mut routes = self
            .routes
            .write()
            .map_err(|_| RustWingError::Cluster("presence store lock poisoned".into()))?;
        let now = Instant::now();
        let live_nodes = self.live_node_ids(now)?;
        let keys = routes.keys().cloned().collect::<Vec<_>>();
        let mut found = Vec::new();
        for key in keys {
            if &key.connection_type != connection_type {
                continue;
            }
            let Some(entries) = routes.get_mut(&key) else {
                continue;
            };
            entries.retain(|_, entry| entry.expires_at > now);
            for entry in entries.values() {
                if live_nodes.contains(&entry.route.node_id) {
                    found.push(entry.route.clone());
                }
            }
            if entries.is_empty() {
                routes.remove(&key);
            }
        }
        Ok(found)
    }

    // List all live routes across connection systems 列出全部连接体系中的活跃路由
    async fn list_all_routes(&self) -> Result<Vec<Route>> {
        // Acquire exclusive access because expired entries may be purged 查询时可能清理过期项，因此获取独占访问
        let mut routes = self
            .routes
            .write()
            .map_err(|_| RustWingError::Cluster("presence store lock poisoned".into()))?;
        let now = Instant::now();
        let live_nodes = self.live_node_ids(now)?;
        let keys = routes.keys().cloned().collect::<Vec<_>>();
        let mut found = Vec::new();
        for key in keys {
            let Some(entries) = routes.get_mut(&key) else {
                continue;
            };
            entries.retain(|_, entry| entry.expires_at > now);
            for entry in entries.values() {
                if live_nodes.contains(&entry.route.node_id) {
                    found.push(entry.route.clone());
                }
            }
            if entries.is_empty() {
                routes.remove(&key);
            }
        }
        Ok(found)
    }

    // List nodes that still have at least one live route 列出仍有活跃路由的节点
    async fn list_nodes(&self) -> Result<Vec<NodeId>> {
        // Acquire exclusive access because expired entries may be purged 查询时可能清理过期项，因此获取独占访问
        let mut routes = self
            .routes
            .write()
            .map_err(|_| RustWingError::Cluster("presence store lock poisoned".into()))?;
        let now = Instant::now();
        let live_nodes = self.live_node_ids(now)?;
        let keys = routes.keys().cloned().collect::<Vec<_>>();
        let mut nodes = Vec::new();
        for key in keys {
            let Some(entries) = routes.get_mut(&key) else {
                continue;
            };
            entries.retain(|_, entry| entry.expires_at > now);
            for entry in entries.values() {
                if live_nodes.contains(&entry.route.node_id)
                    && !nodes.contains(&entry.route.node_id)
                {
                    nodes.push(entry.route.node_id.clone());
                }
            }
            if entries.is_empty() {
                routes.remove(&key);
            }
        }
        Ok(nodes)
    }

    // Register or refresh a node lease 注册或刷新节点租约
    async fn register_node(
        &self,
        node_id: &NodeId,
        instance_id: &str,
        ttl: Duration,
    ) -> Result<NodeLease> {
        let mut node_leases = self
            .node_leases
            .write()
            .map_err(|_| RustWingError::Cluster("presence node lease lock poisoned".into()))?;
        let now = Instant::now();
        if let Some(lease) = node_leases.get_mut(node_id) {
            if lease.expires_at > now && lease.instance_id != instance_id {
                return Ok(NodeLease::Conflict);
            }
            lease.instance_id = instance_id.to_owned();
            lease.expires_at = now + ttl;
            return Ok(NodeLease::Refreshed);
        }
        node_leases.insert(
            node_id.clone(),
            MemoryNodeLease {
                instance_id: instance_id.to_owned(),
                expires_at: now + ttl,
            },
        );
        Ok(NodeLease::Acquired)
    }

    // Remove a node lease when this instance still owns it 当前实例仍持有时删除节点租约
    async fn unregister_node(&self, node_id: &NodeId, instance_id: &str) -> Result<()> {
        let mut node_leases = self
            .node_leases
            .write()
            .map_err(|_| RustWingError::Cluster("presence node lease lock poisoned".into()))?;
        let should_remove = node_leases
            .get(node_id)
            .is_some_and(|lease| lease.instance_id == instance_id);
        if should_remove {
            node_leases.remove(node_id);
        }
        Ok(())
    }
}

impl PresenceKey {
    // Build a presence key from explicit parts 通过显式字段构建在线状态键
    fn new(connection_type: ConnectionType, user_id: UserId) -> Self {
        Self {
            connection_type,
            user_id,
        }
    }

    // Build a presence key from a route 通过路由构建在线状态键
    fn from_route(route: &Route) -> Self {
        Self::new(route.connection_type.clone(), route.user_id.clone())
    }
}

impl MemoryPresenceStore {
    // Snapshot currently leased node identifiers 获取当前仍持有租约的节点标识快照
    fn live_node_ids(&self, now: Instant) -> Result<Vec<NodeId>> {
        let mut node_leases = self
            .node_leases
            .write()
            .map_err(|_| RustWingError::Cluster("presence node lease lock poisoned".into()))?;
        node_leases.retain(|_, lease| lease.expires_at > now);
        Ok(node_leases.keys().cloned().collect())
    }
}

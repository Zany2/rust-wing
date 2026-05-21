use std::collections::HashSet;
use std::sync::Arc;

use dashmap::DashMap;
use dashmap::mapref::entry::Entry;

use crate::cluster::{Cluster, ClusterEnvelope, MemoryPresenceStore, NoopPublisher, Route};
use crate::config::{ClusterBackendConfig, ConnectionPolicy, RustWingConfig};
use crate::error::{Result, RustWingError};
use crate::identity::{Identity, SessionId, UserId};
use crate::protocol::{HeartbeatAckData, OutboundFrame};
use crate::session::{AcceptedSession, Session, SessionSnapshot};

// Main connection manager 主连接管理器
#[derive(Clone)]
pub struct RustWing {
    // Shared manager state 共享管理器状态
    inner: Arc<Inner>,
}

// Internal manager state 内部管理器状态
struct Inner {
    // Normalized runtime configuration 归一化后的运行配置
    config: RustWingConfig,
    // Optional cluster dependencies 可选集群依赖
    cluster: Option<Cluster>,
    // Local session registry 本地会话注册表
    registry: Registry,
}

// Local indexes for active sessions 活跃会话的本地索引
#[derive(Default)]
struct Registry {
    // Direct session lookup by id 按会话标识直接查找
    by_session: DashMap<SessionId, Session>,
    // Reverse index from user to session ids 用户到会话标识的反向索引
    by_user: DashMap<UserId, HashSet<SessionId>>,
}

impl RustWing {
    // Create a standalone manager 创建独立管理器
    pub fn new(config: RustWingConfig) -> Self {
        Self::with_cluster(config, None)
    }

    // Create a manager by assembling the configured backend 按配置组装后端并创建管理器
    pub async fn from_config(config: RustWingConfig) -> Result<Self> {
        // Normalize before backend selection so defaults are stable 后端选择前先归一化配置以稳定默认值
        let config = config.normalized();
        // Skip backend creation when cluster routing is disabled 未启用集群路由时跳过后端创建
        if !config.cluster.enabled {
            return Ok(Self::with_cluster(config, None));
        }

        // Build the selected backend from explicit configuration 依据显式配置构建所选后端
        let cluster = match &config.cluster.backend {
            ClusterBackendConfig::Memory => Cluster::new(MemoryPresenceStore::new(), NoopPublisher),
            ClusterBackendConfig::Redis { url } if url.trim().is_empty() => {
                return Err(RustWingError::InvalidConfig(
                    "redis backend requires a non-empty url".into(),
                ));
            }
            ClusterBackendConfig::Redis { .. } => {
                return Err(RustWingError::BackendUnavailable("redis".into()));
            }
        };

        Ok(Self::with_cluster(config, Some(cluster)))
    }

    // Create a manager with optional cluster support 创建带可选集群支持的管理器
    pub fn with_cluster(config: RustWingConfig, cluster: Option<Cluster>) -> Self {
        Self {
            inner: Arc::new(Inner {
                config: config.normalized(),
                cluster,
                registry: Registry::default(),
            }),
        }
    }

    // Borrow the normalized runtime configuration 借用归一化后的运行配置
    pub fn config(&self) -> &RustWingConfig {
        &self.inner.config
    }

    // Accept a new client session 接收新的客户端会话
    pub async fn accept(&self, identity: Identity) -> Result<AcceptedSession> {
        // Create the bounded outbound channel and session handle 创建有界出站通道和会话句柄
        let accepted = AcceptedSession::new(
            self.inner.config.node_id.clone(),
            identity,
            self.inner.config.write_queue_capacity,
        );

        // Insert the session and collect sessions replaced by policy 插入会话并收集被策略替换的会话
        let replaced = self.insert_session(accepted.session.clone());
        // Close and unregister sessions displaced by single-connection mode 关闭并注销被单连接模式替换的会话
        for session in replaced {
            session.close("replaced by a newer connection");
            let _ = self.unregister(&session).await;
        }

        // Publish the accepted route when clustering is enabled 启用集群时发布已接收路由
        self.register_presence(&accepted.session).await?;
        // Return the live session pair 返回活跃会话组合
        Ok(accepted)
    }

    // Remove one session from local and cluster state 从本地与集群状态中移除一个会话
    pub async fn unregister(&self, session: &Session) -> Result<()> {
        // Remove the session from the local registry 从本地注册表移除会话
        self.inner.registry.remove(session);

        // Remove the distributed route when cluster presence is active 当集群在线状态启用时删除分布式路由
        if let Some(cluster) = &self.inner.cluster {
            if self.inner.config.cluster.enabled {
                cluster
                    .presence
                    .remove(session.user_id(), session.id())
                    .await?;
            }
        }

        // Close the session after registry cleanup 在注册表清理后关闭会话
        session.close("unregistered");
        Ok(())
    }

    // Refresh activity and cluster presence 刷新活跃状态与集群在线状态
    pub async fn touch(&self, session: &Session) -> Result<()> {
        // Update the local activity clock 更新本地活跃时间
        session.mark_active();
        // Extend the distributed route when clustering is active 集群启用时延长分布式路由
        self.touch_presence(session).await?;
        Ok(())
    }

    // Record a heartbeat and build its acknowledgement 记录心跳并构建确认消息
    pub async fn handle_heartbeat(
        &self,
        session: &Session,
        client_heartbeat_time: Option<i64>,
    ) -> Result<HeartbeatAckData> {
        // Persist heartbeat timestamps on the session 在会话上保存心跳时间戳
        session.mark_heartbeat(client_heartbeat_time);
        // Keep the distributed route alive alongside the heartbeat 随心跳一起延长分布式路由
        self.touch_presence(session).await?;
        // Read one consistent view for acknowledgement fields 读取一致快照用于确认字段
        let snapshot = session.snapshot();
        Ok(HeartbeatAckData {
            client_heartbeat_time: snapshot.client_heartbeat_time,
            server_heartbeat_time: snapshot.last_heartbeat_time,
            last_heartbeat_time: snapshot.last_heartbeat_time,
            heartbeat_interval_ms: self.inner.config.heartbeat_interval.as_millis() as u64,
            heartbeat_timeout_ms: self.inner.config.heartbeat_timeout.as_millis() as u64,
        })
    }

    // Remove sessions that exceeded the inactivity timeout 移除超过不活跃超时的会话
    pub async fn reap_inactive_sessions(&self) -> Result<usize> {
        // Snapshot local sessions before awaiting unregister calls 等待注销前先获取本地会话快照
        let sessions = self.all_sessions();
        // Retain only sessions that crossed the configured inactivity threshold 仅保留超过配置阈值的会话
        let inactive = sessions
            .into_iter()
            .filter(|session| session.is_inactive(self.inner.config.heartbeat_timeout))
            .collect::<Vec<_>>();
        // Unregister every stale session and count the removals 注销每个过期会话并统计数量
        for session in &inactive {
            self.unregister(session).await?;
        }
        Ok(inactive.len())
    }

    // Send one frame to a user locally or through the cluster 本地或经由集群向用户发送一帧
    pub async fn send_to_user(
        &self,
        user_id: impl Into<UserId>,
        frame: OutboundFrame,
    ) -> Result<usize> {
        // Normalize the user identifier before routing 在路由前归一化用户标识
        let user_id = user_id.into();
        // Prefer active sessions hosted by this node 优先使用当前节点上的活跃会话
        let sent = self.send_local(&user_id, frame.clone())?;
        if sent > 0 {
            return Ok(sent);
        }

        // Stop when no cluster transport is configured 未配置集群传输时直接结束
        let Some(cluster) = &self.inner.cluster else {
            return Ok(0);
        };
        // Stop when cluster routing is disabled 未启用集群路由时直接结束
        if !self.inner.config.cluster.enabled {
            return Ok(0);
        }

        // Resolve the remote route for the target user 查询目标用户的远端路由
        let routes = cluster.presence.locate(&user_id).await?;
        // Publish once per remote node so one user message is not duplicated 同一用户消息对每个远端节点仅发布一次
        let mut remote_nodes = HashSet::new();
        for route in routes {
            if route.node_id != self.inner.config.node_id {
                remote_nodes.insert(route.node_id);
            }
        }
        // Forward the frame to every node that owns one of the user's sessions 转发到拥有该用户会话的每个节点
        for node_id in &remote_nodes {
            cluster
                .publisher
                .publish(
                    node_id,
                    ClusterEnvelope::new(user_id.clone(), frame.clone()),
                )
                .await?;
        }
        Ok(remote_nodes.len())
    }

    // Broadcast a frame to every local session 向所有本地会话广播一帧
    pub fn broadcast_local(&self, frame: OutboundFrame) -> Result<usize> {
        // Snapshot the current local sessions 获取当前本地会话快照
        let sessions = self.all_sessions();
        // Count only successfully enqueued deliveries 仅统计成功入队的投递
        let mut sent = 0;
        for session in sessions {
            if session.enqueue(frame.clone()).is_ok() {
                sent += 1;
            }
        }
        Ok(sent)
    }

    // Send a frame to every local session of one user 向某个用户的所有本地会话发送一帧
    pub fn send_local(&self, user_id: &UserId, frame: OutboundFrame) -> Result<usize> {
        // Snapshot the target user's local sessions 获取目标用户的本地会话快照
        let sessions = self.sessions_for_user(user_id);
        // Count only successfully enqueued deliveries 仅统计成功入队的投递
        let mut sent = 0;
        for session in sessions {
            if session.enqueue(frame.clone()).is_ok() {
                sent += 1;
            }
        }
        Ok(sent)
    }

    // Look up one session by id 按标识查找一个会话
    pub fn get_session(&self, session_id: &SessionId) -> Result<Option<Session>> {
        // Read from the sharded session index 从分片会话索引读取
        Ok(self
            .inner
            .registry
            .by_session
            .get(session_id)
            .map(|session| session.value().clone()))
    }

    // List snapshots for one user's sessions 列出某个用户的会话快照
    pub fn list_user_sessions(&self, user_id: &UserId) -> Result<Vec<SessionSnapshot>> {
        Ok(self
            .sessions_for_user(user_id)
            .into_iter()
            .map(|session| session.snapshot())
            .collect())
    }

    // Count active local sessions 统计活跃本地会话
    pub fn connection_count(&self) -> Result<usize> {
        // Read the sharded session index length 读取分片会话索引长度
        Ok(self.inner.registry.by_session.len())
    }

    // Deliver a received cluster envelope locally 在本地投递收到的集群信封
    pub fn handle_cluster_envelope(&self, envelope: ClusterEnvelope) -> Result<usize> {
        self.send_local(&envelope.user_id.clone(), envelope.into_frame())
    }

    // Insert a session and return sessions displaced by policy 插入会话并返回被策略替换的会话
    fn insert_session(&self, session: Session) -> Vec<Session> {
        // Mutate only the target user's shard while keeping both indexes aligned 只修改目标用户分片并保持两个索引一致
        self.inner.registry.insert(
            session,
            self.inner.config.connection_policy == ConnectionPolicy::Single,
        )
    }

    // Snapshot all local sessions for one user 获取某个用户的全部本地会话快照
    fn sessions_for_user(&self, user_id: &UserId) -> Vec<Session> {
        self.inner.registry.sessions_for_user(user_id)
    }

    // Snapshot every local session 获取所有本地会话快照
    fn all_sessions(&self) -> Vec<Session> {
        self.inner.registry.all_sessions()
    }

    // Register a distributed route for one session 为一个会话注册分布式路由
    async fn register_presence(&self, session: &Session) -> Result<()> {
        // Stop when no cluster integration exists 不存在集群集成时直接结束
        let Some(cluster) = &self.inner.cluster else {
            return Ok(());
        };
        // Stop when cluster routing is disabled 未启用集群路由时直接结束
        if !self.inner.config.cluster.enabled {
            return Ok(());
        }

        // Publish the current node as the route owner 发布当前节点为路由归属节点
        cluster
            .presence
            .register(
                Route {
                    user_id: session.user_id().clone(),
                    session_id: session.id().clone(),
                    node_id: self.inner.config.node_id.clone(),
                },
                self.inner.config.cluster.route_ttl,
            )
            .await
    }

    // Refresh the distributed route for one session 刷新一个会话的分布式路由
    async fn touch_presence(&self, session: &Session) -> Result<()> {
        // Stop when no cluster integration exists 不存在集群集成时直接结束
        let Some(cluster) = &self.inner.cluster else {
            return Ok(());
        };
        // Stop when cluster routing is disabled 未启用集群路由时直接结束
        if !self.inner.config.cluster.enabled {
            return Ok(());
        }

        // Extend the current session route lifetime 延长当前会话路由生命周期
        cluster
            .presence
            .touch(
                session.user_id(),
                session.id(),
                self.inner.config.cluster.route_ttl,
            )
            .await
    }
}

impl Registry {
    // Insert a session and optionally replace existing user sessions 插入会话并按需替换用户旧会话
    fn insert(&self, session: Session, replace_user: bool) -> Vec<Session> {
        let user_id = session.user_id().clone();
        let session_id = session.id().clone();

        // Update one user's reverse index under the DashMap shard guard 在 DashMap 分片保护下更新单个用户反向索引
        let replaced_ids = match self.by_user.entry(user_id.clone()) {
            Entry::Occupied(mut entry) => {
                let ids = entry.get_mut();
                let replaced_ids = if replace_user {
                    let replaced_ids = ids.iter().cloned().collect::<Vec<_>>();
                    ids.clear();
                    replaced_ids
                } else {
                    Vec::new()
                };
                ids.insert(session_id.clone());
                replaced_ids
            }
            Entry::Vacant(entry) => {
                let mut ids = HashSet::new();
                ids.insert(session_id.clone());
                entry.insert(ids);
                Vec::new()
            }
        };

        // Remove replaced sessions from the primary index after updating the user index 更新用户索引后移除被替换会话
        let replaced = replaced_ids
            .into_iter()
            .filter_map(|id| self.by_session.remove(&id).map(|(_, session)| session))
            .collect::<Vec<_>>();
        // Store the new session in the primary index 将新会话写入主索引
        self.by_session.insert(session_id, session);
        replaced
    }

    // Remove one exact session from both indexes 从两个索引中移除一个精确会话
    fn remove(&self, session: &Session) {
        let user_id = session.user_id().clone();
        let session_id = session.id().clone();

        // Remove the session id from the user's reverse index 从用户反向索引中移除会话标识
        let should_prune_user = match self.by_user.entry(user_id.clone()) {
            Entry::Occupied(mut entry) => {
                let ids = entry.get_mut();
                ids.remove(&session_id);
                ids.is_empty()
            }
            Entry::Vacant(_) => false,
        };
        // Drop empty user buckets after the entry guard is gone 在 entry guard 释放后清理空用户桶
        if should_prune_user {
            self.by_user.remove(&user_id);
        }

        // Remove the primary session record 删除主会话记录
        self.by_session.remove(&session_id);
    }

    // Snapshot all local sessions for one user 获取某个用户的全部本地会话快照
    fn sessions_for_user(&self, user_id: &UserId) -> Vec<Session> {
        // Copy session ids before looking up sessions 先复制会话标识再查询会话
        let session_ids = self
            .by_user
            .get(user_id)
            .map(|ids| ids.iter().cloned().collect::<Vec<_>>())
            .unwrap_or_default();

        session_ids
            .into_iter()
            .filter_map(|id| {
                self.by_session
                    .get(&id)
                    .map(|session| session.value().clone())
            })
            .collect()
    }

    // Snapshot every local session 获取所有本地会话快照
    fn all_sessions(&self) -> Vec<Session> {
        self.by_session
            .iter()
            .map(|entry| entry.value().clone())
            .collect()
    }
}

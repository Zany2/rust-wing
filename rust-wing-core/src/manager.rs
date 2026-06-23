use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::sync::watch;

use crate::cluster::{Cluster, ClusterEnvelope, Route};
use crate::config::RustWingConfig;
use crate::error::{Result, RustWingError};
use crate::identity::{ConnectionType, Identity, MessageId, NodeId, SessionId, UserId};
use crate::protocol::{AckStage, HeartbeatAckData};
use crate::session::{AcceptedSession, Session, SessionSnapshot};

mod ack;
mod delivery;
mod disconnect;
mod lease;
mod maintenance;
mod registry;
mod stats;

use ack::{AckForward, AckTracker};
pub use ack::{AckSnapshot, SessionAckSnapshot};
use lease::generate_instance_id;
use registry::Registry;
use stats::RuntimeStats;
pub use stats::StatsSnapshot;

// Main connection manager 主连接管理器
#[derive(Clone)]
pub struct RustWing {
    // Shared manager state 共享管理器状态
    pub(crate) inner: Arc<Inner>,
}

// Internal manager state 内部管理器状态
pub(super) struct Inner {
    // Normalized runtime configuration 归一化后的运行配置
    config: RustWingConfig,
    // Optional cluster dependencies 可选集群依赖
    cluster: Option<Cluster>,
    // Local session registry 本地会话注册表
    registry: Registry,
    // In-memory acknowledgement tracker 内存确认追踪器
    acks: AckTracker,
    // Runtime counters for lightweight observability 轻量可观测性使用的运行计数器
    stats: RuntimeStats,
    // Cursor used to shard maintenance scans 维护分片扫描使用的游标
    maintenance_cursor: AtomicUsize,
    // Runtime instance identifier used for node lease ownership 用于节点租约归属的运行实例标识
    instance_id: String,
    // Stop signal for the background node lease refresher 节点租约后台刷新任务的停止信号
    lease_stop: Option<watch::Sender<bool>>,
    // Stop signal for the background maintenance task 后台维护任务的停止信号
    maintenance_stop: Option<watch::Sender<bool>>,
}

// Delivery result split by local and remote routing 本地与远程路由拆分后的投递结果
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DeliveryReport {
    // Number of local sessions that accepted the frame 接收帧的本地会话数量
    pub local_sessions: usize,
    // Number of remote nodes that received a cluster publish 接收集群发布的远端节点数量
    pub remote_nodes: usize,
    // Number of remote nodes that failed to publish cluster envelopes 集群信封发布失败的远端节点数量
    pub remote_failures: usize,
}

impl DeliveryReport {
    // Total successful delivery targets using legacy counting semantics 使用旧计数语义统计成功投递目标
    pub fn delivered(&self) -> usize {
        self.local_sessions + self.remote_nodes
    }

    // Count all remote publish attempts 统计全部远端发布尝试
    pub fn remote_attempts(&self) -> usize {
        self.remote_nodes + self.remote_failures
    }
}

impl RustWing {
    // Create a standalone manager 创建独立管理器
    pub fn new(config: RustWingConfig) -> Self {
        let mut wing = Self::with_cluster_unchecked(config, None);
        wing.start_maintenance();
        wing
    }

    // Create a manager by assembling the configured backend 按配置组装后端并创建管理器
    pub async fn from_config(config: RustWingConfig) -> Result<Self> {
        let config = config.normalized();
        config.validate()?;
        if !config.cluster.enabled {
            let mut wing = Self::with_cluster_unchecked(config, None);
            wing.start_maintenance();
            return Ok(wing);
        }

        Err(RustWingError::InvalidConfig(
            "cluster.enabled requires adapter-provided cluster dependencies".into(),
        ))
    }

    // Create a manager without validation or background tasks 创建不执行校验或后台任务的管理器
    pub fn with_cluster_unchecked(config: RustWingConfig, cluster: Option<Cluster>) -> Self {
        Self {
            inner: Arc::new(Inner {
                config: config.normalized(),
                cluster,
                registry: Registry::default(),
                acks: AckTracker::default(),
                stats: RuntimeStats::default(),
                maintenance_cursor: AtomicUsize::new(0),
                instance_id: generate_instance_id(),
                lease_stop: None,
                maintenance_stop: None,
            }),
        }
    }

    // Create a manager and validate the node lease before use 创建管理器并在使用前校验节点租约
    pub async fn with_cluster_checked(
        config: RustWingConfig,
        cluster: Option<Cluster>,
    ) -> Result<Self> {
        let config = config.normalized();
        config.validate()?;
        let mut wing = Self::with_cluster_unchecked(config, cluster);
        wing.register_node_lease().await?;
        wing.start_node_lease_refresher();
        wing.start_maintenance();
        Ok(wing)
    }

    // Borrow the normalized runtime configuration 借用归一化后的运行配置
    pub fn config(&self) -> &RustWingConfig {
        &self.inner.config
    }

    // Accept a new client session 接收新的客户端会话
    pub async fn accept(&self, identity: Identity) -> Result<AcceptedSession> {
        let accepted = AcceptedSession::new(
            self.inner.config.node_id.clone(),
            identity,
            self.inner.config.write_queue_capacity,
        );

        let replaced = self.insert_session(accepted.session.clone());
        for session in replaced {
            session.close("replaced by a newer connection");
            let _ = self.unregister(&session).await;
        }

        if let Err(error) = self.register_presence(&accepted.session).await {
            self.inner.registry.remove(&accepted.session);
            accepted.session.close("presence registration failed");
            return Err(error);
        }

        if let Err(error) = self
            .close_displaced_cluster_sessions(&accepted.session)
            .await
        {
            let _ = self.unregister(&accepted.session).await;
            return Err(error);
        }

        Ok(accepted)
    }

    // Accept a new user session in the default connection system 接收默认连接体系中的用户会话
    pub async fn accept_user(&self, user_id: impl Into<UserId>) -> Result<AcceptedSession> {
        self.accept(Identity::default_connection(user_id)).await
    }

    // Accept a new user client session in the default connection system 接收默认连接体系中的用户客户端会话
    pub async fn accept_client(
        &self,
        user_id: impl Into<UserId>,
        client_id: impl Into<crate::identity::ClientId>,
    ) -> Result<AcceptedSession> {
        self.accept(Identity::default_connection(user_id).with_client(client_id))
            .await
    }

    // Gracefully unregister local sessions and release the node lease 优雅注销本地会话并释放节点租约
    pub async fn shutdown(&self) -> Result<usize> {
        if let Some(lease_stop) = &self.inner.lease_stop {
            let _ = lease_stop.send(true);
        }
        if let Some(maintenance_stop) = &self.inner.maintenance_stop {
            let _ = maintenance_stop.send(true);
        }

        let sessions = self.all_sessions();
        for session in &sessions {
            self.unregister(session).await?;
        }
        self.unregister_node_lease().await?;
        Ok(sessions.len())
    }

    // Remove one session from local and cluster state 从本地与集群状态中移除一个会话
    pub async fn unregister(&self, session: &Session) -> Result<()> {
        self.inner.registry.remove(session);

        if let Some(cluster) = &self.inner.cluster {
            if self.inner.config.cluster.enabled {
                cluster
                    .presence
                    .remove(session.connection_type(), session.user_id(), session.id())
                    .await?;
            }
        }

        session.close("unregistered");
        Ok(())
    }

    // Refresh activity and cluster presence 刷新活跃状态与集群在线状态
    pub async fn touch(&self, session: &Session) -> Result<()> {
        session.mark_active();
        self.touch_presence(session).await?;
        Ok(())
    }

    // Record a heartbeat and build its acknowledgement 记录心跳并构建确认消息
    pub async fn handle_heartbeat(
        &self,
        session: &Session,
        client_heartbeat_time: Option<i64>,
    ) -> Result<HeartbeatAckData> {
        session.mark_heartbeat(client_heartbeat_time);
        self.touch_presence(session).await?;
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
        let sessions = self.all_sessions();
        let inactive = sessions
            .into_iter()
            .filter(|session| session.is_inactive(self.inner.config.heartbeat_timeout))
            .collect::<Vec<_>>();
        for session in &inactive {
            self.unregister(session).await?;
        }
        Ok(inactive.len())
    }

    // Generate a new message id for acknowledgement tracking 生成用于确认追踪的新消息标识
    pub fn next_message_id(&self) -> MessageId {
        MessageId::generate(&self.inner.config.node_id)
    }

    // Record a client acknowledgement for one session 记录某个会话的客户端确认
    pub async fn acknowledge(
        &self,
        session_id: &SessionId,
        message_id: &MessageId,
        stage: AckStage,
        client_time: Option<i64>,
    ) -> Result<bool> {
        let update = self
            .inner
            .acks
            .acknowledge(session_id, message_id, stage, client_time)?;
        if let Some(forward) = update.forward {
            self.forward_ack(forward).await?;
        }
        Ok(update.updated)
    }

    // Read the acknowledgement snapshot for one message 读取某条消息的确认快照
    // Forward a remote acknowledgement to the origin node 向发起节点转发远程确认
    async fn forward_ack(&self, forward: AckForward) -> Result<()> {
        let Some(cluster) = &self.inner.cluster else {
            return Ok(());
        };
        if !self.inner.config.cluster.enabled {
            return Ok(());
        }
        if forward.node_id == self.inner.config.node_id {
            return Ok(());
        }

        let result = cluster
            .publisher
            .publish(
                &forward.node_id,
                ClusterEnvelope::new_for_ack(
                    forward.session_id,
                    forward.message_id,
                    forward.stage,
                    forward.client_time,
                ),
            )
            .await;
        match result {
            Ok(()) => {
                self.inner.stats.record_cluster_publish_success();
                Ok(())
            }
            Err(error) => {
                self.inner.stats.record_cluster_publish_failed();
                Err(error)
            }
        }
    }

    pub fn ack_snapshot(&self, message_id: &MessageId) -> Result<Option<AckSnapshot>> {
        self.inner.acks.snapshot(message_id)
    }

    // Count currently tracked acknowledgement messages 统计当前被追踪的确认消息数量
    pub fn ack_pending_count(&self) -> usize {
        self.inner.acks.pending_count()
    }

    // Remove expired acknowledgement entries 移除过期确认条目
    pub fn reap_expired_acks(&self) -> usize {
        self.inner.acks.reap_expired()
    }

    // Wait until all known local targets reach the required acknowledgement stage 等待全部已知本地目标达到所需确认阶段
    pub async fn wait_for_ack(
        &self,
        message_id: &MessageId,
        stage: AckStage,
        timeout: std::time::Duration,
    ) -> Result<Option<AckSnapshot>> {
        self.inner.acks.wait_for(message_id, stage, timeout).await
    }

    // Look up one session by id 按标识查找一个会话
    pub fn get_session(&self, session_id: &SessionId) -> Result<Option<Session>> {
        Ok(self
            .inner
            .registry
            .by_session
            .get(session_id)
            .map(|session| session.value().clone()))
    }

    // List snapshots for one default-system user's sessions 列出默认连接体系中某个用户的会话快照
    pub fn list_user_sessions(&self, user_id: &UserId) -> Result<Vec<SessionSnapshot>> {
        self.list_user_sessions_in(&ConnectionType::default(), user_id)
    }

    // List snapshots for one user's sessions in one connection system 列出某个连接体系中某个用户的会话快照
    pub fn list_user_sessions_in(
        &self,
        connection_type: &ConnectionType,
        user_id: &UserId,
    ) -> Result<Vec<SessionSnapshot>> {
        Ok(self
            .sessions_for_user(connection_type, user_id)
            .into_iter()
            .map(|session| session.snapshot())
            .collect())
    }

    // List snapshots for all local sessions 列出全部本地会话快照
    pub fn list_sessions(&self) -> Result<Vec<SessionSnapshot>> {
        Ok(self
            .all_sessions()
            .into_iter()
            .map(|session| session.snapshot())
            .collect())
    }

    // List snapshots for all local sessions in one connection system 列出某个连接体系中的全部本地会话快照
    pub fn list_sessions_in(
        &self,
        connection_type: &ConnectionType,
    ) -> Result<Vec<SessionSnapshot>> {
        Ok(self
            .inner
            .registry
            .sessions_for_connection_type(connection_type)
            .into_iter()
            .map(|session| session.snapshot())
            .collect())
    }

    // Count active local sessions 统计活跃本地会话
    pub fn connection_count(&self) -> Result<usize> {
        Ok(self.inner.registry.by_session.len())
    }

    // Capture a lightweight runtime statistics snapshot 捕获轻量运行统计快照
    pub fn stats_snapshot(&self) -> Result<StatsSnapshot> {
        let cluster_enabled = self.inner.cluster.is_some() && self.inner.config.cluster.enabled;
        let local_connections = self.connection_count()?;
        Ok(self.inner.stats.snapshot(
            self.inner.config.node_id.clone(),
            local_connections,
            self.inner.registry.user_count(),
            self.ack_pending_count(),
            usize::from(cluster_enabled),
            if cluster_enabled {
                local_connections
            } else {
                0
            },
        ))
    }

    // Capture a detailed runtime statistics snapshot with live cluster visibility 捕获包含实时集群可见性的详细运行统计快照
    pub async fn detailed_stats_snapshot(&self) -> Result<StatsSnapshot> {
        let cluster_enabled = self.inner.cluster.is_some() && self.inner.config.cluster.enabled;
        let local_connections = self.connection_count()?;
        let cluster_nodes = if cluster_enabled {
            self.list_cluster_nodes().await?.len()
        } else {
            0
        };
        let cluster_routes = if cluster_enabled {
            self.list_all_cluster_routes().await?.len()
        } else {
            0
        };
        Ok(self.inner.stats.snapshot(
            self.inner.config.node_id.clone(),
            local_connections,
            self.inner.registry.user_count(),
            self.ack_pending_count(),
            cluster_nodes,
            cluster_routes,
        ))
    }

    // List live nodes visible through the configured cluster store 列出配置的集群存储中可见的活跃节点
    pub async fn list_cluster_nodes(&self) -> Result<Vec<NodeId>> {
        let Some(cluster) = &self.inner.cluster else {
            return Ok(Vec::new());
        };
        if !self.inner.config.cluster.enabled {
            return Ok(Vec::new());
        }
        cluster.presence.list_nodes().await
    }

    // List live routes in one connection system through the cluster store 列出集群存储中某个连接体系的活跃路由
    pub async fn list_cluster_routes(
        &self,
        connection_type: &ConnectionType,
    ) -> Result<Vec<Route>> {
        let Some(cluster) = &self.inner.cluster else {
            return Ok(Vec::new());
        };
        if !self.inner.config.cluster.enabled {
            return Ok(Vec::new());
        }
        cluster.presence.list_routes(connection_type).await
    }

    // List live routes across all connection systems through the cluster store 列出集群存储中全部连接体系的活跃路由
    pub async fn list_all_cluster_routes(&self) -> Result<Vec<Route>> {
        let Some(cluster) = &self.inner.cluster else {
            return Ok(Vec::new());
        };
        if !self.inner.config.cluster.enabled {
            return Ok(Vec::new());
        }
        cluster.presence.list_all_routes().await
    }

    // Insert a session and return sessions displaced by policy 插入会话并返回被策略替换的会话
    fn insert_session(&self, session: Session) -> Vec<Session> {
        self.inner.registry.insert(session, &self.inner.config)
    }

    // Snapshot all local sessions for one user 获取某个用户的全部本地会话快照
    fn sessions_for_user(
        &self,
        connection_type: &ConnectionType,
        user_id: &UserId,
    ) -> Vec<Session> {
        self.inner
            .registry
            .sessions_for_user(connection_type, user_id)
    }

    // Snapshot all local sessions 获取全部本地会话快照
    fn all_sessions(&self) -> Vec<Session> {
        self.inner.registry.all_sessions()
    }

    // Snapshot the next maintenance scan window 获取下一批维护扫描窗口
    fn next_maintenance_sessions(&self, limit: usize) -> Vec<Session> {
        let total = self.inner.registry.by_session.len();
        if total == 0 || limit == 0 {
            return Vec::new();
        }
        let start = self
            .inner
            .maintenance_cursor
            .fetch_add(limit, Ordering::Relaxed)
            % total;
        self.inner.registry.session_window(start, limit)
    }

    // Register a distributed route for one session 为一个会话注册分布式路由
    async fn register_presence(&self, session: &Session) -> Result<()> {
        let Some(cluster) = &self.inner.cluster else {
            return Ok(());
        };
        if !self.inner.config.cluster.enabled {
            return Ok(());
        }

        cluster
            .presence
            .register(
                Route {
                    user_id: session.user_id().clone(),
                    connection_type: session.connection_type().clone(),
                    client_id: session.client_id().cloned(),
                    session_id: session.id().clone(),
                    node_id: self.inner.config.node_id.clone(),
                },
                self.inner.config.cluster.route_ttl,
            )
            .await
    }
}

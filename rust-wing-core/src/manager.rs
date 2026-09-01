use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, Mutex};

use tokio::sync::{broadcast, watch};
use tokio::task::JoinHandle;

use crate::cluster::Cluster;
use crate::config::RustWingConfig;
use crate::error::{Result, RustWingError};
use crate::identity::{Identity, UserId};
use crate::lifecycle::{DisconnectCause, SessionEvent};
use crate::protocol::HeartbeatAckData;
use crate::session::{AcceptedSession, Session};

mod delivery;
mod disconnect;
mod events;
mod maintenance;
mod presence;
mod query;
mod registry;
mod runtime;
mod stats;

use presence::generate_instance_id;
use registry::Registry;
use runtime::RuntimeState;
pub use runtime::{RuntimeHealth, RuntimeStatus};
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
    // Non-blocking session lifecycle event channel 非阻塞会话生命周期事件通道
    session_events: broadcast::Sender<SessionEvent>,
    // Runtime counters for lightweight observability 轻量可观测性使用的运行计数器
    stats: RuntimeStats,
    // Cursor used to shard maintenance scans 维护分片扫描使用的游标
    maintenance_cursor: AtomicUsize,
    // Runtime instance identifier used for node lease ownership 用于节点租约归属的运行实例标识
    instance_id: String,
    // Stop signal for the background node lease refresher 节点租约后台刷新任务的停止信号
    lease_stop: Mutex<Option<watch::Sender<bool>>>,
    // Managed node lease refresher task 托管的节点租约刷新任务
    lease_task: Mutex<Option<JoinHandle<()>>>,
    // Stop signal for the background maintenance task 后台维护任务的停止信号
    maintenance_stop: Mutex<Option<watch::Sender<bool>>>,
    // Managed session maintenance task 托管的会话维护任务
    maintenance_task: Mutex<Option<JoinHandle<()>>>,
    // Shared runtime lifecycle and health state 共享运行时生命周期与健康状态
    runtime: RuntimeState,
    // Serialize repeated shutdown requests 串行化重复关闭请求
    shutdown_lock: tokio::sync::Mutex<()>,
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
        let mut wing = Self::assemble(config, None);
        wing.start_maintenance();
        wing.inner.runtime.mark_running();
        wing
    }

    // Create a manager by assembling the configured backend 按配置组装后端并创建管理器
    pub async fn from_config(config: RustWingConfig) -> Result<Self> {
        let config = config.normalized();
        config.validate()?;
        if !config.cluster.enabled {
            let mut wing = Self::assemble(config, None);
            wing.start_maintenance();
            wing.inner.runtime.mark_running();
            return Ok(wing);
        }

        Err(RustWingError::InvalidConfig(
            "cluster.enabled requires adapter-provided cluster dependencies".into(),
        ))
    }

    // Assemble a manager before checked startup 供校验式启动使用的管理器组装方法
    fn assemble(config: RustWingConfig, cluster: Option<Cluster>) -> Self {
        let config = config.normalized();
        let cluster_enabled = config.cluster.enabled;
        let maintenance_enabled = config.maintenance.enabled;
        let (session_events, _) = broadcast::channel(config.session_event_capacity);
        Self {
            inner: Arc::new(Inner {
                config,
                cluster,
                registry: Registry::default(),
                session_events,
                stats: RuntimeStats::default(),
                maintenance_cursor: AtomicUsize::new(0),
                instance_id: generate_instance_id(),
                lease_stop: Mutex::new(None),
                lease_task: Mutex::new(None),
                maintenance_stop: Mutex::new(None),
                maintenance_task: Mutex::new(None),
                runtime: RuntimeState::new(cluster_enabled, maintenance_enabled),
                shutdown_lock: tokio::sync::Mutex::new(()),
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
        if config.cluster.enabled && cluster.is_none() {
            return Err(RustWingError::InvalidConfig(
                "cluster.enabled requires cluster dependencies".into(),
            ));
        }
        let mut wing = Self::assemble(config, cluster);
        wing.register_node_lease().await?;
        wing.inner.runtime.mark_node_lease_healthy();
        wing.start_maintenance();
        wing.inner.runtime.mark_running();
        wing.start_node_lease_refresher();
        Ok(wing)
    }

    // Borrow the normalized runtime configuration 借用归一化后的运行配置
    pub fn config(&self) -> &RustWingConfig {
        &self.inner.config
    }

    // Accept a new client session 接收新的客户端会话
    pub async fn accept(&self, identity: Identity) -> Result<AcceptedSession> {
        self.ensure_accepting()?;
        let _operation_guard = self
            .inner
            .registry
            .operation_lock(&identity.connection_type, &identity.user_id)
            .lock()
            .await;
        self.ensure_accepting()?;
        let accepted = AcceptedSession::new(
            self.inner.config.node_id.clone(),
            identity,
            self.inner.config.write_queue_capacity,
        );

        let replaced = self.insert_session(accepted.session.clone());
        for session in replaced {
            let _ = self
                .finalize_removed_session(&session, DisconnectCause::Replaced)
                .await;
        }

        let claim = match self.claim_presence(&accepted.session).await {
            Ok(claim) => claim,
            Err(error) => {
                self.rollback_unaccepted_session(&accepted.session, "presence claim failed")
                    .await;
                return Err(error);
            }
        };

        if let Err(error) = self.close_displaced_cluster_sessions(claim.displaced).await {
            self.rollback_unaccepted_session(
                &accepted.session,
                "cluster session replacement failed",
            )
            .await;
            return Err(error);
        }

        self.emit_session_event(SessionEvent::Connected {
            session: accepted.session.snapshot(),
        });
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
        let _shutdown_guard = self.inner.shutdown_lock.lock().await;
        if self.runtime_status() == RuntimeStatus::Stopped {
            return Ok(0);
        }
        self.inner.runtime.mark_stopping();

        if let Some(lease_stop) = self
            .inner
            .lease_stop
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .as_ref()
        {
            let _ = lease_stop.send(true);
        }
        if let Some(maintenance_stop) = self
            .inner
            .maintenance_stop
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .as_ref()
        {
            let _ = maintenance_stop.send(true);
        }

        let lease_task = self
            .inner
            .lease_task
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take();
        let maintenance_task = self
            .inner
            .maintenance_task
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take();
        let mut first_error = None;
        if let Some(task) = lease_task {
            if let Err(error) = task.await {
                first_error = Some(RustWingError::Cluster(format!(
                    "node lease task failed: {error}"
                )));
            }
        }
        if let Some(task) = maintenance_task {
            if let Err(error) = task.await {
                if first_error.is_none() {
                    first_error = Some(RustWingError::Cluster(format!(
                        "maintenance task failed: {error}"
                    )));
                }
            }
        }

        let sessions = self.all_sessions();
        for session in &sessions {
            if let Err(error) = self
                .unregister_with_cause(session, DisconnectCause::RuntimeShutdown)
                .await
            {
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
        }
        if let Err(error) = self.unregister_node_lease().await {
            if first_error.is_none() {
                first_error = Some(error);
            }
        }
        self.inner.runtime.mark_stopped();
        match first_error {
            Some(error) => Err(error),
            None => Ok(sessions.len()),
        }
    }

    // Remove one session from local and cluster state 从本地与集群状态中移除一个会话
    pub async fn unregister(&self, session: &Session) -> Result<()> {
        self.unregister_with_cause(session, DisconnectCause::Unregistered)
            .await
            .map(|_| ())
    }

    // Remove one session with an explicit lifecycle cause 使用明确生命周期原因移除一条会话
    pub async fn unregister_with_cause(
        &self,
        session: &Session,
        cause: DisconnectCause,
    ) -> Result<bool> {
        let _operation_guard = self
            .inner
            .registry
            .operation_lock(session.connection_type(), session.user_id())
            .lock()
            .await;
        self.unregister_with_cause_unlocked(session, cause).await
    }

    // Remove one session while its user operation lock is already held 用户操作锁已持有时移除一条会话
    async fn unregister_with_cause_unlocked(
        &self,
        session: &Session,
        cause: DisconnectCause,
    ) -> Result<bool> {
        let Some(removed) = self.inner.registry.remove(session) else {
            return Ok(false);
        };
        self.finalize_removed_session(&removed, cause).await?;
        Ok(true)
    }

    async fn finalize_removed_session(
        &self,
        session: &Session,
        cause: DisconnectCause,
    ) -> Result<()> {
        let presence_result = self.remove_session_presence(session).await;
        self.close_removed_session(session, cause);
        presence_result
    }

    pub(super) fn remove_local_session_with_cause(
        &self,
        session: &Session,
        cause: DisconnectCause,
    ) -> bool {
        let Some(removed) = self.inner.registry.remove(session) else {
            return false;
        };
        self.close_removed_session(&removed, cause);
        self.spawn_presence_cleanup(removed);
        true
    }

    // Schedule distributed route cleanup for synchronous compatibility paths 为同步兼容路径安排分布式路由清理
    fn spawn_presence_cleanup(&self, session: Session) {
        if self.inner.cluster.is_none() || !self.inner.config.cluster.enabled {
            return;
        }
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let wing = self.clone();
        runtime.spawn(async move {
            let _ = wing.remove_session_presence(&session).await;
        });
    }

    fn close_removed_session(&self, session: &Session, cause: DisconnectCause) {
        session.close(cause.close_reason());
        self.emit_session_event(SessionEvent::Disconnected {
            session: session.snapshot(),
            cause,
        });
    }

    async fn rollback_unaccepted_session(&self, session: &Session, reason: &str) {
        let _ = self.inner.registry.remove(session);
        let _ = self.remove_session_presence(session).await;
        session.close(reason);
    }

    async fn remove_session_presence(&self, session: &Session) -> Result<()> {
        if let Some(cluster) = &self.inner.cluster {
            if self.inner.config.cluster.enabled {
                cluster
                    .presence
                    .remove(session.connection_type(), session.user_id(), session.id())
                    .await?;
            }
        }
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

    // Insert a session and return sessions displaced by policy 插入会话并返回被策略替换的会话
    fn insert_session(&self, session: Session) -> Vec<Session> {
        self.inner.registry.insert(session, &self.inner.config)
    }
}

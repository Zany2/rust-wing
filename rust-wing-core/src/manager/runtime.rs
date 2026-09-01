use std::sync::RwLock;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};

use serde::Serialize;

use crate::error::{Result, RustWingError};

use super::RustWing;

// Shared runtime lifecycle state 共享运行时生命周期状态
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
#[repr(u8)]
pub enum RuntimeStatus {
    // Runtime dependencies are still starting 运行时依赖仍在启动
    Starting = 0,
    // Runtime can accept sessions and deliver messages 运行时可接收会话并投递消息
    Running = 1,
    // Runtime has an unhealthy dependency and rejects new sessions 运行时存在不健康依赖并拒绝新会话
    Degraded = 2,
    // Runtime is stopping background tasks and sessions 运行时正在停止后台任务与会话
    Stopping = 3,
    // Runtime has completed shutdown 运行时已完成关闭
    Stopped = 4,
}

impl RuntimeStatus {
    fn from_u8(value: u8) -> Self {
        match value {
            0 => Self::Starting,
            1 => Self::Running,
            2 => Self::Degraded,
            3 => Self::Stopping,
            4 => Self::Stopped,
            _ => Self::Stopped,
        }
    }
}

// Point-in-time core runtime health 核心运行时健康状态快照
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RuntimeHealth {
    // Current lifecycle status 当前生命周期状态
    pub status: RuntimeStatus,
    // Whether distributed routing is enabled 是否启用分布式路由
    pub cluster_enabled: bool,
    // Whether the current node lease is known to be healthy 当前节点租约是否已知健康
    pub node_lease_healthy: bool,
    // Whether session maintenance is configured 是否配置会话维护
    pub maintenance_enabled: bool,
    // Whether the maintenance background task is running 会话维护后台任务是否正在运行
    pub maintenance_running: bool,
    // Latest runtime dependency error 最近一次运行时依赖错误
    pub last_error: Option<String>,
}

// Mutable runtime state shared by all manager handles 所有管理器句柄共享的可变运行状态
pub(super) struct RuntimeState {
    status: AtomicU8,
    cluster_enabled: bool,
    node_lease_healthy: AtomicBool,
    maintenance_enabled: bool,
    maintenance_running: AtomicBool,
    last_error: RwLock<Option<String>>,
}

impl RuntimeState {
    pub(super) fn new(cluster_enabled: bool, maintenance_enabled: bool) -> Self {
        Self {
            status: AtomicU8::new(RuntimeStatus::Starting as u8),
            cluster_enabled,
            node_lease_healthy: AtomicBool::new(!cluster_enabled),
            maintenance_enabled,
            maintenance_running: AtomicBool::new(false),
            last_error: RwLock::new(None),
        }
    }

    pub(super) fn status(&self) -> RuntimeStatus {
        RuntimeStatus::from_u8(self.status.load(Ordering::Acquire))
    }

    pub(super) fn mark_running(&self) {
        self.status
            .store(RuntimeStatus::Running as u8, Ordering::Release);
        self.clear_last_error();
    }

    pub(super) fn mark_stopping(&self) {
        self.status
            .store(RuntimeStatus::Stopping as u8, Ordering::Release);
    }

    pub(super) fn mark_stopped(&self) {
        self.maintenance_running.store(false, Ordering::Release);
        self.status
            .store(RuntimeStatus::Stopped as u8, Ordering::Release);
    }

    pub(super) fn mark_node_lease_healthy(&self) {
        self.node_lease_healthy.store(true, Ordering::Release);
        if self.status() == RuntimeStatus::Degraded {
            self.mark_running();
        }
    }

    pub(super) fn mark_node_lease_unhealthy(&self, error: impl Into<String>) {
        self.node_lease_healthy.store(false, Ordering::Release);
        self.set_last_error(error.into());
        if matches!(
            self.status(),
            RuntimeStatus::Starting | RuntimeStatus::Running | RuntimeStatus::Degraded
        ) {
            self.status
                .store(RuntimeStatus::Degraded as u8, Ordering::Release);
        }
    }

    pub(super) fn set_maintenance_running(&self, running: bool) {
        self.maintenance_running.store(running, Ordering::Release);
    }

    pub(super) fn snapshot(&self) -> RuntimeHealth {
        RuntimeHealth {
            status: self.status(),
            cluster_enabled: self.cluster_enabled,
            node_lease_healthy: self.node_lease_healthy.load(Ordering::Acquire),
            maintenance_enabled: self.maintenance_enabled,
            maintenance_running: self.maintenance_running.load(Ordering::Acquire),
            last_error: self
                .last_error
                .read()
                .unwrap_or_else(|error| error.into_inner())
                .clone(),
        }
    }

    fn set_last_error(&self, error: String) {
        *self
            .last_error
            .write()
            .unwrap_or_else(|error| error.into_inner()) = Some(error);
    }

    fn clear_last_error(&self) {
        *self
            .last_error
            .write()
            .unwrap_or_else(|error| error.into_inner()) = None;
    }
}

impl RustWing {
    // Return the current core runtime lifecycle status 返回当前核心运行时生命周期状态
    pub fn runtime_status(&self) -> RuntimeStatus {
        self.inner.runtime.status()
    }

    // Return a point-in-time core runtime health snapshot 返回核心运行时健康状态快照
    pub fn health(&self) -> RuntimeHealth {
        self.inner.runtime.snapshot()
    }

    // Check whether the runtime can accept new sessions 检查运行时是否可接收新会话
    pub fn is_ready(&self) -> bool {
        self.runtime_status() == RuntimeStatus::Running
    }

    pub(super) fn ensure_accepting(&self) -> Result<()> {
        let status = self.runtime_status();
        if status == RuntimeStatus::Running {
            return Ok(());
        }
        Err(RustWingError::RuntimeNotReady(format!("{status:?}")))
    }
}

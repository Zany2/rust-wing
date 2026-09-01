// Cluster presence and node lease lifecycle 集群在线状态与节点租约生命周期
use std::sync::{Arc, Weak};

use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::cluster::{NodeLease, Route, RouteClaim, RouteRefresh};
use crate::error::{Result, RustWingError};
use crate::identity::uuid_v7_simple;
use crate::session::Session;

use super::{Inner, RustWing};

impl RustWing {
    // Atomically claim a distributed route for one session 为一个会话原子仲裁分布式路由
    pub(super) async fn claim_presence(&self, session: &Session) -> Result<RouteClaim> {
        let Some(cluster) = &self.inner.cluster else {
            return Ok(RouteClaim::default());
        };
        if !self.inner.config.cluster.enabled {
            return Ok(RouteClaim::default());
        }

        let route = Route {
            user_id: session.user_id().clone(),
            connection_type: session.connection_type().clone(),
            client_id: session.client_id().cloned(),
            session_id: session.id().clone(),
            node_id: self.inner.config.node_id.clone(),
        };
        cluster
            .presence
            .claim(
                route,
                self.inner.config.policy_for(session.connection_type()),
                self.inner.config.cluster.route_ttl,
            )
            .await
    }

    // Register this runtime instance as the owner of its node id 注册当前运行实例为节点标识持有者
    pub(super) async fn register_node_lease(&self) -> Result<()> {
        let Some(cluster) = &self.inner.cluster else {
            return Ok(());
        };
        if !self.inner.config.cluster.enabled {
            return Ok(());
        }

        match cluster
            .presence
            .register_node(
                &self.inner.config.node_id,
                &self.inner.instance_id,
                self.inner.config.cluster.node_lease_ttl,
            )
            .await?
        {
            NodeLease::Acquired | NodeLease::Refreshed => Ok(()),
            NodeLease::Conflict => Err(RustWingError::InvalidConfig(format!(
                "node_id '{}' is already active",
                self.inner.config.node_id.as_str()
            ))),
        }
    }

    // Release this runtime instance's node lease 释放当前运行实例的节点租约
    pub(super) async fn unregister_node_lease(&self) -> Result<()> {
        let Some(cluster) = &self.inner.cluster else {
            return Ok(());
        };
        if !self.inner.config.cluster.enabled {
            return Ok(());
        }

        cluster
            .presence
            .unregister_node(&self.inner.config.node_id, &self.inner.instance_id)
            .await
    }

    // Start the background task that keeps the node lease alive 启动保持节点租约存活的后台任务
    pub(super) fn start_node_lease_refresher(&mut self) {
        if self.inner.cluster.is_none() || !self.inner.config.cluster.enabled {
            return;
        }
        let (lease_stop, stop_rx) = watch::channel(false);
        *self
            .inner
            .lease_stop
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(lease_stop);
        let task_inner = Arc::downgrade(&self.inner);
        let task = spawn_node_lease_refresher(task_inner, stop_rx);
        *self
            .inner
            .lease_task
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(task);
    }

    // Refresh the distributed route for one session 刷新一个会话的分布式路由
    pub(super) async fn touch_presence(&self, session: &Session) -> Result<()> {
        // Stop when no cluster integration exists 不存在集群集成时直接结束
        let Some(cluster) = &self.inner.cluster else {
            return Ok(());
        };
        // Stop when cluster routing is disabled 未启用集群路由时直接结束
        if !self.inner.config.cluster.enabled {
            return Ok(());
        }

        // Extend the current session route lifetime 延长当前会话路由生命周期
        let refresh = cluster
            .presence
            .touch(
                session.connection_type(),
                session.user_id(),
                session.id(),
                self.inner.config.cluster.route_ttl,
            )
            .await?;
        if refresh == RouteRefresh::Lost {
            self.unregister_with_cause(session, crate::lifecycle::DisconnectCause::Replaced)
                .await?;
        }
        Ok(())
    }
}

impl Drop for Inner {
    // Signal the lease refresher to stop when the manager is dropped 管理器释放时通知租约刷新任务停止
    fn drop(&mut self) {
        if let Some(lease_stop) = self
            .lease_stop
            .get_mut()
            .unwrap_or_else(|error| error.into_inner())
            .as_ref()
        {
            let _ = lease_stop.send(true);
        }
        if let Some(maintenance_stop) = self
            .maintenance_stop
            .get_mut()
            .unwrap_or_else(|error| error.into_inner())
            .as_ref()
        {
            let _ = maintenance_stop.send(true);
        }
    }
}

// Spawn a task that refreshes the node lease until the manager is dropped 启动任务持续刷新节点租约直到管理器释放
fn spawn_node_lease_refresher(
    inner: Weak<Inner>,
    mut stop_rx: watch::Receiver<bool>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let Some(first_inner) = inner.upgrade() else {
            return;
        };
        let interval = node_lease_refresh_interval(first_inner.config.cluster.node_lease_ttl);
        drop(first_inner);
        let mut ticker = tokio::time::interval(interval);
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    let Some(inner) = inner.upgrade() else {
                        break;
                    };
                    let Some(cluster) = &inner.cluster else {
                        break;
                    };
                    if !inner.config.cluster.enabled {
                        break;
                    }
                    let result = cluster
                        .presence
                        .register_node(
                            &inner.config.node_id,
                            &inner.instance_id,
                            inner.config.cluster.node_lease_ttl,
                        )
                        .await;
                    match result {
                        Ok(NodeLease::Acquired | NodeLease::Refreshed) => {
                            inner.runtime.mark_node_lease_healthy();
                        }
                        Ok(NodeLease::Conflict) => {
                            inner.runtime.mark_node_lease_unhealthy(format!(
                                "node_id '{}' lease is owned by another instance",
                                inner.config.node_id.as_str()
                            ));
                        }
                        Err(error) => {
                            inner.runtime.mark_node_lease_unhealthy(error.to_string());
                        }
                    }
                }
                changed = stop_rx.changed() => {
                    if changed.is_err() || *stop_rx.borrow() {
                        break;
                    }
                }
            }
        }
    })
}

// Choose a refresh interval safely below the lease TTL 选择低于租约 TTL 的安全刷新间隔
fn node_lease_refresh_interval(ttl: std::time::Duration) -> std::time::Duration {
    let interval = ttl / 3;
    interval.max(std::time::Duration::from_millis(100))
}

// Generate a process-local runtime instance id 生成进程级运行实例标识
pub(super) fn generate_instance_id() -> String {
    uuid_v7_simple()
}

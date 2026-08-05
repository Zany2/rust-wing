use std::sync::atomic::Ordering;
use std::sync::{Arc, Weak};

use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::error::Result;
use crate::protocol::OutboundFrame;
use crate::session::Session;

use super::{Inner, RustWing};

impl RustWing {
    // Start the managed maintenance task when enabled 启动已启用的托管维护任务
    pub(super) fn start_maintenance(&mut self) {
        if !self.inner.config.maintenance.enabled {
            return;
        }
        // Skip background maintenance outside Tokio runtimes 避免在 Tokio 运行时外启动后台维护
        if tokio::runtime::Handle::try_current().is_err() {
            return;
        }
        let (maintenance_stop, stop_rx) = watch::channel(false);
        {
            let Some(inner) = Arc::get_mut(&mut self.inner) else {
                return;
            };
            inner.maintenance_stop = Some(maintenance_stop);
        }
        let task_inner = Arc::downgrade(&self.inner);
        spawn_maintenance_task(task_inner, stop_rx);
    }
}

// Spawn a task that reaps stale sessions 启动任务清理失活会话
fn spawn_maintenance_task(
    inner: Weak<Inner>,
    mut stop_rx: watch::Receiver<bool>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let Some(first_inner) = inner.upgrade() else {
            return;
        };
        let interval = first_inner.config.maintenance.interval;
        drop(first_inner);

        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    let Some(inner) = inner.upgrade() else {
                        break;
                    };
                    let wing = RustWing { inner };
                    let _ = wing.maintain_inactive_sessions().await;
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

impl RustWing {
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

    // Probe inactive sessions before removing them 探测失活会话后再移除
    async fn maintain_inactive_sessions(&self) -> Result<usize> {
        let max_cleanup = self.inner.config.maintenance.max_cleanup_per_tick;
        let max_probe = self.inner.config.maintenance.max_probe_per_tick;
        let max_scan = max_cleanup.saturating_add(max_probe).max(1);
        let sessions = self.next_maintenance_sessions(max_scan);
        let mut removed = 0;
        let mut probed = 0;
        for session in sessions {
            if !session.is_inactive(self.inner.config.heartbeat_timeout) {
                session.clear_probe();
                continue;
            }
            if session.probe_expired(self.inner.config.maintenance.probe_timeout) {
                if removed >= max_cleanup {
                    continue;
                }
                if self
                    .remove_confirmed_inactive_session(&session, true)
                    .await?
                {
                    removed += 1;
                }
                continue;
            }
            if session.probe_pending() || probed >= max_probe {
                continue;
            }
            probed += 1;
            if self.send_liveness_probe(&session).is_err() {
                if removed >= max_cleanup {
                    continue;
                }
                if self
                    .remove_confirmed_inactive_session(&session, false)
                    .await?
                {
                    removed += 1;
                }
            }
        }
        if removed > 0 {
            self.inner.stats.record_maintenance_sessions_reaped(removed);
        }
        Ok(removed)
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

    // Send one WebSocket ping as a liveness probe 发送一个 WebSocket Ping 作为存活探测
    fn send_liveness_probe(&self, session: &Session) -> Result<()> {
        session.mark_probe_sent();
        if let Err(error) = session.enqueue(OutboundFrame::ping(Vec::new())) {
            self.inner.stats.record_outbound_frame_failed();
            return Err(error);
        }
        self.inner.stats.record_outbound_frame_enqueued();
        self.inner.stats.record_maintenance_probe_sent();
        Ok(())
    }

    // Re-read the exact session before removing it 清理前重新读取精确会话
    async fn remove_confirmed_inactive_session(
        &self,
        session: &Session,
        require_probe_expired: bool,
    ) -> Result<bool> {
        let Some(current) = self.get_session(session.id())? else {
            return Ok(false);
        };
        if !current.is_inactive(self.inner.config.heartbeat_timeout) {
            current.clear_probe();
            return Ok(false);
        }
        if require_probe_expired
            && !current.probe_expired(self.inner.config.maintenance.probe_timeout)
        {
            return Ok(false);
        }
        self.unregister(&current).await?;
        Ok(true)
    }
}

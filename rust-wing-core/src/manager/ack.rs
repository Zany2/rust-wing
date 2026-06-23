use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

use dashmap::DashMap;
use tokio::sync::Notify;

use crate::error::{Result, RustWingError};
use crate::identity::{MessageId, NodeId, SessionId};
use crate::protocol::AckStage;

// Delivery acknowledgement snapshot 投递确认快照
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AckSnapshot {
    // Tracked message identifier 被追踪的消息标识
    pub message_id: MessageId,
    // Per-session acknowledgement state 按会话记录的确认状态
    pub sessions: Vec<SessionAckSnapshot>,
}

// Per-session acknowledgement snapshot 单会话确认快照
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionAckSnapshot {
    // Target session identifier 目标会话标识
    pub session_id: SessionId,
    // Latest acknowledged stage 最新确认阶段
    pub stage: Option<AckStage>,
    // Latest client timestamp reported with the acknowledgement 确认携带的最新客户端时间
    pub client_time: Option<i64>,
    // Latest server timestamp when the acknowledgement was updated 确认更新时的最新服务端时间
    pub server_time: Option<i64>,
}

// In-memory acknowledgement tracker 内存确认追踪器
#[derive(Default)]
pub(super) struct AckTracker {
    // Message id to per-session acknowledgement states 消息标识到单会话确认状态
    messages: DashMap<MessageId, Arc<AckEntry>>,
}

// Result of recording one acknowledgement 记录一次确认后的结果
pub(super) struct AckUpdate {
    // Whether a tracked session was updated 是否更新了已追踪会话
    pub(super) updated: bool,
    // Optional acknowledgement that must be forwarded to the origin node 需要转发给发起节点的可选确认
    pub(super) forward: Option<AckForward>,
}

// Acknowledgement forwarding request 确认转发请求
pub(super) struct AckForward {
    // Origin node that owns the authoritative tracker 拥有权威追踪器的发起节点
    pub(super) node_id: NodeId,
    // Acknowledging session identifier 确认所属的会话标识
    pub(super) session_id: SessionId,
    // Acknowledged message identifier 已确认的消息标识
    pub(super) message_id: MessageId,
    // Acknowledgement stage 确认阶段
    pub(super) stage: AckStage,
    // Optional client-side acknowledgement time 可选客户端确认时间
    pub(super) client_time: Option<i64>,
}

// Internal acknowledgement entry 内部确认条目
struct AckEntry {
    // Per-session acknowledgement states 单会话确认状态
    sessions: Mutex<HashMap<SessionId, SessionAckState>>,
    // Expiration timestamp in milliseconds 过期毫秒时间戳
    expires_at: AtomicI64,
    // Notifier for waiters 等待方通知器
    notify: Notify,
}

// Internal per-session acknowledgement state 内部单会话确认状态
#[derive(Debug, Clone)]
struct SessionAckState {
    // Latest acknowledged stage 最新确认阶段
    stage: Option<AckStage>,
    // Latest client timestamp reported with the acknowledgement 确认携带的最新客户端时间
    client_time: Option<i64>,
    // Latest server timestamp when the acknowledgement was updated 确认更新时的最新服务端时间
    server_time: Option<i64>,
    // Origin node to notify when this session acknowledges a remote message 远程消息确认后需要通知的发起节点
    origin_node_id: Option<NodeId>,
}

impl AckTracker {
    // Track one message target session 追踪某条消息的一个目标会话
    pub(super) fn track(
        &self,
        session_id: SessionId,
        message_id: MessageId,
        ttl: std::time::Duration,
    ) -> Result<()> {
        self.track_with_origin(session_id, message_id, ttl, None)
    }

    // Track one message target session with an optional origin node 追踪带可选发起节点的消息目标会话
    pub(super) fn track_with_origin(
        &self,
        session_id: SessionId,
        message_id: MessageId,
        ttl: std::time::Duration,
        origin_node_id: Option<NodeId>,
    ) -> Result<()> {
        let expires_at = ack_expires_at(ttl);
        let entry = self.messages.entry(message_id).or_insert_with(|| {
            Arc::new(AckEntry {
                sessions: Mutex::new(HashMap::new()),
                expires_at: AtomicI64::new(expires_at),
                notify: Notify::new(),
            })
        });
        entry.expires_at.store(expires_at, Ordering::Release);
        let mut sessions = entry
            .sessions
            .lock()
            .map_err(|_| RustWingError::Cluster("ack tracker lock poisoned".into()))?;
        sessions
            .entry(session_id)
            .and_modify(|state| {
                if origin_node_id.is_some() {
                    state.origin_node_id = origin_node_id.clone();
                }
            })
            .or_insert(SessionAckState {
                stage: None,
                client_time: None,
                server_time: None,
                origin_node_id,
            });
        Ok(())
    }

    // Record one acknowledgement from a session 记录某个会话的一次确认
    pub(super) fn acknowledge(
        &self,
        session_id: &SessionId,
        message_id: &MessageId,
        stage: AckStage,
        client_time: Option<i64>,
    ) -> Result<AckUpdate> {
        let Some(entry) = self.messages.get(message_id).map(|entry| entry.clone()) else {
            return Ok(AckUpdate {
                updated: false,
                forward: None,
            });
        };
        if entry.is_expired() {
            self.messages.remove(message_id);
            return Ok(AckUpdate {
                updated: false,
                forward: None,
            });
        }
        let mut sessions = entry
            .sessions
            .lock()
            .map_err(|_| RustWingError::Cluster("ack tracker lock poisoned".into()))?;
        let Some(state) = sessions.get_mut(session_id) else {
            return Ok(AckUpdate {
                updated: false,
                forward: None,
            });
        };
        state.stage = Some(stage);
        state.client_time = client_time;
        state.server_time = Some(crate::protocol::now_millis());
        let forward = state.origin_node_id.clone().map(|node_id| AckForward {
            node_id,
            session_id: session_id.clone(),
            message_id: message_id.clone(),
            stage,
            client_time,
        });
        entry.notify.notify_waiters();
        Ok(AckUpdate {
            updated: true,
            forward,
        })
    }

    // Snapshot one tracked message 读取一条被追踪消息的快照
    pub(super) fn snapshot(&self, message_id: &MessageId) -> Result<Option<AckSnapshot>> {
        let Some(entry) = self.messages.get(message_id).map(|entry| entry.clone()) else {
            return Ok(None);
        };
        if entry.is_expired() {
            self.messages.remove(message_id);
            return Ok(None);
        }
        let sessions = entry
            .sessions
            .lock()
            .map_err(|_| RustWingError::Cluster("ack tracker lock poisoned".into()))?;
        let sessions = sessions
            .iter()
            .map(|(session_id, state)| SessionAckSnapshot {
                session_id: session_id.clone(),
                stage: state.stage,
                client_time: state.client_time,
                server_time: state.server_time,
            })
            .collect();
        Ok(Some(AckSnapshot {
            message_id: message_id.clone(),
            sessions,
        }))
    }

    // Wait until every tracked session reaches the required stage 等待每个被追踪会话达到所需阶段
    pub(super) async fn wait_for(
        &self,
        message_id: &MessageId,
        stage: AckStage,
        timeout: std::time::Duration,
    ) -> Result<Option<AckSnapshot>> {
        let Some(entry) = self.messages.get(message_id).map(|entry| entry.clone()) else {
            return Ok(None);
        };
        if entry.is_expired() {
            self.messages.remove(message_id);
            return Ok(None);
        }
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let snapshot = self.snapshot(message_id)?;
            if snapshot
                .as_ref()
                .is_some_and(|snapshot| snapshot.reached(stage))
            {
                return Ok(snapshot);
            }
            let now = tokio::time::Instant::now();
            if now >= deadline {
                return Ok(snapshot);
            }
            let remaining = deadline.saturating_duration_since(now);
            tokio::select! {
                _ = entry.notify.notified() => {}
                _ = tokio::time::sleep(remaining) => {
                    return self.snapshot(message_id);
                }
            }
        }
    }

    // Count live tracked acknowledgement messages 统计仍存活的确认追踪消息
    pub(super) fn pending_count(&self) -> usize {
        self.reap_expired();
        self.messages.len()
    }

    // Remove expired acknowledgement messages 移除过期确认追踪消息
    pub(super) fn reap_expired(&self) -> usize {
        let expired = self
            .messages
            .iter()
            .filter(|entry| entry.value().is_expired())
            .map(|entry| entry.key().clone())
            .collect::<Vec<_>>();
        let removed = expired.len();
        for message_id in expired {
            if let Some((_, entry)) = self.messages.remove(&message_id) {
                entry.notify.notify_waiters();
            }
        }
        removed
    }
}

impl AckEntry {
    // Check whether this acknowledgement entry has expired 判断确认条目是否已过期
    fn is_expired(&self) -> bool {
        crate::protocol::now_millis() >= self.expires_at.load(Ordering::Acquire)
    }
}

// Calculate the expiration timestamp for an acknowledgement entry 计算确认条目的过期时间戳
fn ack_expires_at(ttl: std::time::Duration) -> i64 {
    let ttl_ms = ttl.as_millis().min(i64::MAX as u128) as i64;
    crate::protocol::now_millis().saturating_add(ttl_ms)
}

impl AckSnapshot {
    // Check whether every tracked session reached the required stage 判断每个被追踪会话是否达到所需阶段
    pub fn reached(&self, stage: AckStage) -> bool {
        !self.sessions.is_empty()
            && self
                .sessions
                .iter()
                .all(|session| session.stage.is_some_and(|current| current >= stage))
    }
}

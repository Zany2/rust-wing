use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::time::SystemTime;

use tokio::sync::mpsc;

use crate::error::{Result, RustWingError};
use crate::identity::{DeviceId, Identity, NodeId, SessionId, UserId};
use crate::protocol::{OutboundFrame, now_millis};

#[derive(Debug, Clone)]
pub struct Session {
    inner: Arc<SessionInner>,
}

#[derive(Debug)]
struct SessionInner {
    id: SessionId,
    node_id: NodeId,
    identity: Identity,
    connected_at: SystemTime,
    sender: mpsc::Sender<OutboundFrame>,
    last_active_time: AtomicI64,
    last_heartbeat_time: AtomicI64,
    client_heartbeat_time: AtomicI64,
    closed: AtomicBool,
}

#[derive(Debug)]
pub struct AcceptedSession {
    pub session: Session,
    pub outbound: mpsc::Receiver<OutboundFrame>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionSnapshot {
    pub id: SessionId,
    pub node_id: NodeId,
    pub user_id: UserId,
    pub device_id: Option<DeviceId>,
    pub connected_at: SystemTime,
    pub last_active_time: i64,
    pub last_heartbeat_time: i64,
    pub client_heartbeat_time: i64,
    pub closed: bool,
}

impl AcceptedSession {
    pub(crate) fn new(node_id: NodeId, identity: Identity, capacity: usize) -> Self {
        let (sender, outbound) = mpsc::channel(capacity);
        let now = now_millis();
        let session = Session {
            inner: Arc::new(SessionInner {
                id: SessionId::generate(&node_id),
                node_id,
                identity,
                connected_at: SystemTime::now(),
                sender,
                last_active_time: AtomicI64::new(now),
                last_heartbeat_time: AtomicI64::new(0),
                client_heartbeat_time: AtomicI64::new(0),
                closed: AtomicBool::new(false),
            }),
        };

        Self { session, outbound }
    }
}

impl Session {
    pub fn id(&self) -> &SessionId {
        &self.inner.id
    }

    pub fn node_id(&self) -> &NodeId {
        &self.inner.node_id
    }

    pub fn identity(&self) -> &Identity {
        &self.inner.identity
    }

    pub fn user_id(&self) -> &UserId {
        &self.inner.identity.user_id
    }

    pub fn device_id(&self) -> Option<&DeviceId> {
        self.inner.identity.device_id.as_ref()
    }

    pub fn mark_active(&self) {
        self.inner
            .last_active_time
            .store(now_millis(), Ordering::Relaxed);
    }

    pub fn mark_heartbeat(&self, client_heartbeat_time: Option<i64>) {
        let now = now_millis();
        self.inner.last_active_time.store(now, Ordering::Relaxed);
        self.inner.last_heartbeat_time.store(now, Ordering::Relaxed);
        if let Some(client_time) = client_heartbeat_time {
            self.inner
                .client_heartbeat_time
                .store(client_time, Ordering::Relaxed);
        }
    }

    pub fn enqueue(&self, frame: OutboundFrame) -> Result<()> {
        if self.is_closed() {
            return Err(RustWingError::SessionClosed);
        }

        match self.inner.sender.try_send(frame) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.close("write queue full");
                Err(RustWingError::QueueFull)
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.close("receiver closed");
                Err(RustWingError::SessionClosed)
            }
        }
    }

    pub fn close(&self, reason: impl Into<Vec<u8>>) {
        let already_closed = self.inner.closed.swap(true, Ordering::AcqRel);
        if !already_closed {
            let _ = self.inner.sender.try_send(OutboundFrame::close(reason));
        }
    }

    pub fn is_closed(&self) -> bool {
        self.inner.closed.load(Ordering::Acquire)
    }

    pub fn snapshot(&self) -> SessionSnapshot {
        SessionSnapshot {
            id: self.inner.id.clone(),
            node_id: self.inner.node_id.clone(),
            user_id: self.inner.identity.user_id.clone(),
            device_id: self.inner.identity.device_id.clone(),
            connected_at: self.inner.connected_at,
            last_active_time: self.inner.last_active_time.load(Ordering::Relaxed),
            last_heartbeat_time: self.inner.last_heartbeat_time.load(Ordering::Relaxed),
            client_heartbeat_time: self.inner.client_heartbeat_time.load(Ordering::Relaxed),
            closed: self.is_closed(),
        }
    }
}

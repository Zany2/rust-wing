use std::collections::HashMap;
use std::sync::RwLock;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::error::{Result, RustWingError};
use crate::identity::{NodeId, SessionId, UserId};
use crate::protocol::{FrameKind, OutboundFrame};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Route {
    pub user_id: UserId,
    pub session_id: SessionId,
    pub node_id: NodeId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterEnvelope {
    pub user_id: UserId,
    pub frame_kind: FrameKind,
    pub payload: Vec<u8>,
}

impl ClusterEnvelope {
    pub fn new(user_id: UserId, frame: OutboundFrame) -> Self {
        Self {
            user_id,
            frame_kind: frame.kind,
            payload: frame.payload,
        }
    }

    pub fn into_frame(self) -> OutboundFrame {
        OutboundFrame {
            kind: self.frame_kind,
            payload: self.payload,
        }
    }
}

#[async_trait]
pub trait PresenceStore: Send + Sync {
    async fn register(&self, route: Route, ttl: Duration) -> Result<()>;
    async fn remove(&self, user_id: &UserId, session_id: &SessionId) -> Result<()>;
    async fn touch(&self, user_id: &UserId, session_id: &SessionId, ttl: Duration) -> Result<()>;
    async fn locate(&self, user_id: &UserId) -> Result<Option<Route>>;
}

#[async_trait]
pub trait NodePublisher: Send + Sync {
    async fn publish(&self, node_id: &NodeId, envelope: ClusterEnvelope) -> Result<()>;
}

pub struct Cluster {
    pub presence: Box<dyn PresenceStore>,
    pub publisher: Box<dyn NodePublisher>,
}

impl Cluster {
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

#[derive(Debug, Default)]
pub struct MemoryPresenceStore {
    routes: RwLock<HashMap<UserId, MemoryRoute>>,
}

#[derive(Debug, Clone)]
struct MemoryRoute {
    route: Route,
    expires_at: Instant,
}

impl MemoryPresenceStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl PresenceStore for MemoryPresenceStore {
    async fn register(&self, route: Route, ttl: Duration) -> Result<()> {
        let mut routes = self
            .routes
            .write()
            .map_err(|_| RustWingError::Cluster("presence store lock poisoned".into()))?;
        routes.insert(
            route.user_id.clone(),
            MemoryRoute {
                route,
                expires_at: Instant::now() + ttl,
            },
        );
        Ok(())
    }

    async fn remove(&self, user_id: &UserId, session_id: &SessionId) -> Result<()> {
        let mut routes = self
            .routes
            .write()
            .map_err(|_| RustWingError::Cluster("presence store lock poisoned".into()))?;
        let should_remove = routes
            .get(user_id)
            .map(|entry| &entry.route.session_id == session_id)
            .unwrap_or(false);
        if should_remove {
            routes.remove(user_id);
        }
        Ok(())
    }

    async fn touch(&self, user_id: &UserId, session_id: &SessionId, ttl: Duration) -> Result<()> {
        let mut routes = self
            .routes
            .write()
            .map_err(|_| RustWingError::Cluster("presence store lock poisoned".into()))?;
        if let Some(entry) = routes.get_mut(user_id) {
            if &entry.route.session_id == session_id {
                entry.expires_at = Instant::now() + ttl;
            }
        }
        Ok(())
    }

    async fn locate(&self, user_id: &UserId) -> Result<Option<Route>> {
        let mut routes = self
            .routes
            .write()
            .map_err(|_| RustWingError::Cluster("presence store lock poisoned".into()))?;
        let Some(entry) = routes.get(user_id) else {
            return Ok(None);
        };
        if entry.expires_at <= Instant::now() {
            routes.remove(user_id);
            return Ok(None);
        }
        Ok(Some(entry.route.clone()))
    }
}

#[derive(Debug, Default)]
pub struct NoopPublisher;

#[async_trait]
impl NodePublisher for NoopPublisher {
    async fn publish(&self, _node_id: &NodeId, _envelope: ClusterEnvelope) -> Result<()> {
        Ok(())
    }
}

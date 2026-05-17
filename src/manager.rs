use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

use crate::cluster::{Cluster, ClusterEnvelope, Route};
use crate::config::{ConnectionPolicy, RustWingConfig};
use crate::error::{Result, RustWingError};
use crate::identity::{Identity, SessionId, UserId};
use crate::protocol::OutboundFrame;
use crate::session::{AcceptedSession, Session, SessionSnapshot};

#[derive(Clone)]
pub struct RustWing {
    inner: Arc<Inner>,
}

struct Inner {
    config: RustWingConfig,
    cluster: Option<Cluster>,
    registry: RwLock<Registry>,
}

#[derive(Default)]
struct Registry {
    by_session: HashMap<SessionId, Session>,
    by_user: HashMap<UserId, HashSet<SessionId>>,
}

impl RustWing {
    pub fn new(config: RustWingConfig) -> Self {
        Self::with_cluster(config, None)
    }

    pub fn with_cluster(config: RustWingConfig, cluster: Option<Cluster>) -> Self {
        Self {
            inner: Arc::new(Inner {
                config: config.normalized(),
                cluster,
                registry: RwLock::new(Registry::default()),
            }),
        }
    }

    pub fn config(&self) -> &RustWingConfig {
        &self.inner.config
    }

    pub async fn accept(&self, identity: Identity) -> Result<AcceptedSession> {
        let accepted = AcceptedSession::new(
            self.inner.config.node_id.clone(),
            identity,
            self.inner.config.write_queue_capacity,
        );

        let replaced = self.insert_session(accepted.session.clone())?;
        for session in replaced {
            session.close("replaced by a newer connection");
            let _ = self.unregister(&session).await;
        }

        self.register_presence(&accepted.session).await?;
        Ok(accepted)
    }

    pub async fn unregister(&self, session: &Session) -> Result<()> {
        {
            let mut registry = self
                .inner
                .registry
                .write()
                .map_err(|_| RustWingError::Cluster("registry lock poisoned".into()))?;
            registry.remove(session);
        }

        if let Some(cluster) = &self.inner.cluster {
            if self.inner.config.cluster.enabled {
                cluster
                    .presence
                    .remove(session.user_id(), session.id())
                    .await?;
            }
        }

        session.close("unregistered");
        Ok(())
    }

    pub async fn touch(&self, session: &Session) -> Result<()> {
        session.mark_active();
        if let Some(cluster) = &self.inner.cluster {
            if self.inner.config.cluster.enabled {
                cluster
                    .presence
                    .touch(
                        session.user_id(),
                        session.id(),
                        self.inner.config.cluster.route_ttl,
                    )
                    .await?;
            }
        }
        Ok(())
    }

    pub async fn send_to_user(
        &self,
        user_id: impl Into<UserId>,
        frame: OutboundFrame,
    ) -> Result<usize> {
        let user_id = user_id.into();
        let sent = self.send_local(&user_id, frame.clone())?;
        if sent > 0 {
            return Ok(sent);
        }

        let Some(cluster) = &self.inner.cluster else {
            return Ok(0);
        };
        if !self.inner.config.cluster.enabled {
            return Ok(0);
        }

        let Some(route) = cluster.presence.locate(&user_id).await? else {
            return Ok(0);
        };
        if route.node_id == self.inner.config.node_id {
            return Ok(0);
        }

        cluster
            .publisher
            .publish(&route.node_id, ClusterEnvelope::new(user_id, frame))
            .await?;
        Ok(1)
    }

    pub fn broadcast_local(&self, frame: OutboundFrame) -> Result<usize> {
        let sessions = self.all_sessions()?;
        let mut sent = 0;
        for session in sessions {
            if session.enqueue(frame.clone()).is_ok() {
                sent += 1;
            }
        }
        Ok(sent)
    }

    pub fn send_local(&self, user_id: &UserId, frame: OutboundFrame) -> Result<usize> {
        let sessions = self.sessions_for_user(user_id)?;
        let mut sent = 0;
        for session in sessions {
            if session.enqueue(frame.clone()).is_ok() {
                sent += 1;
            }
        }
        Ok(sent)
    }

    pub fn get_session(&self, session_id: &SessionId) -> Result<Option<Session>> {
        let registry = self
            .inner
            .registry
            .read()
            .map_err(|_| RustWingError::Cluster("registry lock poisoned".into()))?;
        Ok(registry.by_session.get(session_id).cloned())
    }

    pub fn list_user_sessions(&self, user_id: &UserId) -> Result<Vec<SessionSnapshot>> {
        Ok(self
            .sessions_for_user(user_id)?
            .into_iter()
            .map(|session| session.snapshot())
            .collect())
    }

    pub fn connection_count(&self) -> Result<usize> {
        let registry = self
            .inner
            .registry
            .read()
            .map_err(|_| RustWingError::Cluster("registry lock poisoned".into()))?;
        Ok(registry.by_session.len())
    }

    pub fn handle_cluster_envelope(&self, envelope: ClusterEnvelope) -> Result<usize> {
        self.send_local(&envelope.user_id.clone(), envelope.into_frame())
    }

    fn insert_session(&self, session: Session) -> Result<Vec<Session>> {
        let mut registry = self
            .inner
            .registry
            .write()
            .map_err(|_| RustWingError::Cluster("registry lock poisoned".into()))?;

        let mut replaced = Vec::new();
        if self.inner.config.connection_policy == ConnectionPolicy::Single {
            replaced = registry.remove_user(session.user_id());
        }

        registry
            .by_user
            .entry(session.user_id().clone())
            .or_default()
            .insert(session.id().clone());
        registry.by_session.insert(session.id().clone(), session);
        Ok(replaced)
    }

    fn sessions_for_user(&self, user_id: &UserId) -> Result<Vec<Session>> {
        let registry = self
            .inner
            .registry
            .read()
            .map_err(|_| RustWingError::Cluster("registry lock poisoned".into()))?;
        Ok(registry
            .by_user
            .get(user_id)
            .into_iter()
            .flat_map(|ids| ids.iter())
            .filter_map(|id| registry.by_session.get(id))
            .cloned()
            .collect())
    }

    fn all_sessions(&self) -> Result<Vec<Session>> {
        let registry = self
            .inner
            .registry
            .read()
            .map_err(|_| RustWingError::Cluster("registry lock poisoned".into()))?;
        Ok(registry.by_session.values().cloned().collect())
    }

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
                    session_id: session.id().clone(),
                    node_id: self.inner.config.node_id.clone(),
                },
                self.inner.config.cluster.route_ttl,
            )
            .await
    }
}

impl Registry {
    fn remove(&mut self, session: &Session) {
        self.by_session.remove(session.id());
        if let Some(ids) = self.by_user.get_mut(session.user_id()) {
            ids.remove(session.id());
            if ids.is_empty() {
                self.by_user.remove(session.user_id());
            }
        }
    }

    fn remove_user(&mut self, user_id: &UserId) -> Vec<Session> {
        let Some(ids) = self.by_user.remove(user_id) else {
            return Vec::new();
        };
        ids.into_iter()
            .filter_map(|id| self.by_session.remove(&id))
            .collect()
    }
}

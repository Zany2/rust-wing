use std::collections::HashMap;

use crate::cluster::{ClusterEnvelope, ClusterTarget, Route};
use crate::error::Result;
use crate::identity::{ClientId, ConnectionType, SessionId, UserId};
use crate::protocol::{FrameKind, OutboundFrame};
use crate::session::Session;

use super::registry::ClientRouteKey;
use super::{DeliveryReport, RustWing};

impl RustWing {
    // Send one frame to a default-system user locally or through the cluster 本地或经由集群向默认连接体系用户发送一帧
    pub async fn send_to_user(
        &self,
        user_id: impl Into<UserId>,
        frame: OutboundFrame,
    ) -> Result<DeliveryReport> {
        self.send_to_user_in(ConnectionType::default(), user_id, frame)
            .await
    }

    // Send one frame to a user in one connection system 向某个连接体系中的用户发送一帧
    pub async fn send_to_user_in(
        &self,
        connection_type: impl Into<ConnectionType>,
        user_id: impl Into<UserId>,
        frame: OutboundFrame,
    ) -> Result<DeliveryReport> {
        self.send_to_user_inner(connection_type, user_id, frame)
            .await
    }

    // Send one frame to a default-system user client slot locally or through the cluster 本地或经由集群向默认连接体系用户客户端槽位发送一帧
    pub async fn send_to_client<C>(
        &self,
        user_id: impl Into<UserId>,
        client_id: Option<C>,
        frame: OutboundFrame,
    ) -> Result<DeliveryReport>
    where
        C: Into<ClientId>,
    {
        self.send_to_client_in(ConnectionType::default(), user_id, client_id, frame)
            .await
    }

    // Send one frame to a user client slot in one connection system 向某个连接体系中的用户客户端槽位发送一帧
    pub async fn send_to_client_in<C>(
        &self,
        connection_type: impl Into<ConnectionType>,
        user_id: impl Into<UserId>,
        client_id: Option<C>,
        frame: OutboundFrame,
    ) -> Result<DeliveryReport>
    where
        C: Into<ClientId>,
    {
        let connection_type = connection_type.into();
        let user_id = user_id.into();
        let client_id = client_id.map(Into::into);
        if frame.kind == FrameKind::Close {
            return self
                .disconnect_client_in(connection_type, user_id, client_id, frame.payload)
                .await;
        }
        let client_key =
            ClientRouteKey::new(connection_type.clone(), user_id.clone(), client_id.clone());
        let sent = self.send_local_to_client_key(&client_key, frame.clone())?;

        let Some(cluster) = &self.inner.cluster else {
            return Ok(DeliveryReport {
                local_sessions: sent,
                remote_nodes: 0,
                remote_failures: 0,
            });
        };
        if !self.inner.config.cluster.enabled {
            return Ok(DeliveryReport {
                local_sessions: sent,
                remote_nodes: 0,
                remote_failures: 0,
            });
        }

        let routes = cluster
            .presence
            .locate(&connection_type, &user_id)
            .await?
            .into_iter()
            .filter(|route| route.client_id == client_id)
            .collect::<Vec<_>>();
        let (remote_nodes, remote_failures) = self
            .publish_grouped_remote_routes(
                routes,
                ClusterEnvelope::new_for_client(
                    connection_type.clone(),
                    user_id.clone(),
                    client_id.clone(),
                    frame.clone(),
                ),
                false,
            )
            .await?;
        Ok(DeliveryReport {
            local_sessions: sent,
            remote_nodes,
            remote_failures,
        })
    }

    // Send one frame to one exact session locally or through the cluster 本地或经由集群向一条精确会话发送一帧
    pub async fn send_to_session(
        &self,
        session_id: &SessionId,
        frame: OutboundFrame,
    ) -> Result<DeliveryReport> {
        if frame.kind == FrameKind::Close {
            return self.disconnect_session(session_id, frame.payload).await;
        }
        if let Some(session) = self.get_session(session_id)? {
            let sent = usize::from(self.enqueue(&session, frame).is_ok());
            return Ok(DeliveryReport {
                local_sessions: sent,
                remote_nodes: 0,
                remote_failures: 0,
            });
        }

        let Some(cluster) = &self.inner.cluster else {
            return Ok(DeliveryReport::default());
        };
        if !self.inner.config.cluster.enabled {
            return Ok(DeliveryReport::default());
        }

        let Some(route) = cluster.presence.locate_session(session_id).await? else {
            return Ok(DeliveryReport::default());
        };
        if route.node_id == self.inner.config.node_id {
            return Ok(DeliveryReport::default());
        }
        let (remote_nodes, remote_failures) = self
            .publish_grouped_remote_routes(
                vec![route],
                ClusterEnvelope::new_for_session(session_id.clone(), frame.clone()),
                false,
            )
            .await?;
        Ok(DeliveryReport {
            local_sessions: 0,
            remote_nodes,
            remote_failures,
        })
    }

    // Send a frame to every local session of one user 向某个用户的全部本地会话发送一帧
    pub fn send_local(
        &self,
        connection_type: &ConnectionType,
        user_id: &UserId,
        frame: OutboundFrame,
    ) -> Result<usize> {
        let sessions = self.sessions_for_user(connection_type, user_id);
        let mut sent = 0;
        for session in sessions {
            if self.enqueue(&session, frame.clone()).is_ok() {
                sent += 1;
            }
        }
        Ok(sent)
    }

    // Send a frame to every local session of one user-client key 向某个用户客户端键的全部本地会话发送一帧
    pub(super) fn send_local_to_client_key(
        &self,
        client_key: &ClientRouteKey,
        frame: OutboundFrame,
    ) -> Result<usize> {
        let sessions = self.inner.registry.sessions_for_client_key(client_key);
        let mut sent = 0;
        for session in sessions {
            if self.enqueue(&session, frame.clone()).is_ok() {
                sent += 1;
            }
        }
        Ok(sent)
    }

    // Queue one frame and update runtime counters 入队一帧并更新运行计数
    pub(super) fn enqueue(&self, session: &Session, frame: OutboundFrame) -> Result<()> {
        if frame.kind == FrameKind::Close {
            session.close(frame.payload);
            return Ok(());
        }

        if let Err(error) = session.enqueue(frame) {
            self.inner.stats.record_outbound_frame_failed();
            return Err(error);
        }
        self.inner.stats.record_outbound_frame_enqueued();
        Ok(())
    }

    async fn send_to_user_inner(
        &self,
        connection_type: impl Into<ConnectionType>,
        user_id: impl Into<UserId>,
        frame: OutboundFrame,
    ) -> Result<DeliveryReport> {
        let connection_type = connection_type.into();
        let user_id = user_id.into();
        if frame.kind == FrameKind::Close {
            return self
                .disconnect_user_in(connection_type, user_id, frame.payload)
                .await;
        }
        let sent = self.send_local(&connection_type, &user_id, frame.clone())?;

        let Some(cluster) = &self.inner.cluster else {
            return Ok(DeliveryReport {
                local_sessions: sent,
                remote_nodes: 0,
                remote_failures: 0,
            });
        };
        if !self.inner.config.cluster.enabled {
            return Ok(DeliveryReport {
                local_sessions: sent,
                remote_nodes: 0,
                remote_failures: 0,
            });
        }

        let routes = cluster.presence.locate(&connection_type, &user_id).await?;
        let (remote_nodes, remote_failures) = self
            .publish_grouped_remote_routes(
                routes,
                ClusterEnvelope::new(connection_type.clone(), user_id.clone(), frame.clone()),
                false,
            )
            .await?;
        Ok(DeliveryReport {
            local_sessions: sent,
            remote_nodes,
            remote_failures,
        })
    }

    // Deliver a received cluster envelope locally 在本地投递收到的集群信封
    pub fn handle_cluster_envelope(&self, envelope: ClusterEnvelope) -> Result<usize> {
        let target = envelope.target.clone();
        let frame = envelope.into_frame();
        match target {
            ClusterTarget::User {
                connection_type,
                user_id,
            } => self.send_local(&connection_type, &user_id, frame),
            ClusterTarget::Client {
                connection_type,
                user_id,
                client_id,
            } => {
                let client_key = ClientRouteKey::new(connection_type, user_id, client_id);
                self.send_local_to_client_key(&client_key, frame)
            }
            ClusterTarget::Session { session_id } => {
                let Some(session) = self.get_session(&session_id)? else {
                    return Ok(0);
                };
                if frame.kind == FrameKind::Close {
                    session.close(frame.payload);
                    self.inner.registry.remove(&session);
                    return Ok(1);
                }
                Ok(usize::from(self.enqueue(&session, frame).is_ok()))
            }
            ClusterTarget::Broadcast { connection_type } => {
                self.broadcast_local_by_connection_type(&connection_type, frame)
            }
            ClusterTarget::BroadcastAll => self.broadcast_local(frame),
        }
    }
}

impl RustWing {
    // Broadcast a frame to every local session 广播一帧到所有本地会话
    pub fn broadcast_local(&self, frame: OutboundFrame) -> Result<usize> {
        let sessions = self.all_sessions();
        let mut sent = 0;
        for session in sessions {
            if self.enqueue(&session, frame.clone()).is_ok() {
                sent += 1;
            }
        }
        Ok(sent)
    }

    // Broadcast a frame to every default-system session 广播一帧到默认连接体系的全部会话
    pub async fn broadcast(&self, frame: OutboundFrame) -> Result<DeliveryReport> {
        self.broadcast_in(ConnectionType::default(), frame).await
    }

    // Broadcast a frame to every session in one connection system 广播一帧到某个连接体系的全部会话
    pub async fn broadcast_in(
        &self,
        connection_type: impl Into<ConnectionType>,
        frame: OutboundFrame,
    ) -> Result<DeliveryReport> {
        let connection_type = connection_type.into();
        if frame.kind == FrameKind::Close {
            return self.disconnect_all_in(connection_type, frame.payload).await;
        }
        let sent = self.broadcast_local_by_connection_type(&connection_type, frame.clone())?;
        let routes = self.broadcast_routes(Some(&connection_type)).await?;
        let (remote_nodes, remote_failures) = self
            .publish_grouped_remote_routes(
                routes,
                ClusterEnvelope::new_for_broadcast(connection_type, frame.clone()),
                false,
            )
            .await?;
        Ok(DeliveryReport {
            local_sessions: sent,
            remote_nodes,
            remote_failures,
        })
    }

    // Broadcast a frame to every local and remote session 广播一帧到全部本地与远程会话
    pub async fn broadcast_all(&self, frame: OutboundFrame) -> Result<DeliveryReport> {
        if frame.kind == FrameKind::Close {
            return self.disconnect_all(frame.payload).await;
        }
        let sent = self.broadcast_local(frame.clone())?;
        let routes = self.broadcast_routes(None).await?;
        let (remote_nodes, remote_failures) = self
            .publish_grouped_remote_routes(
                routes,
                ClusterEnvelope::new_for_broadcast_all(frame.clone()),
                false,
            )
            .await?;
        Ok(DeliveryReport {
            local_sessions: sent,
            remote_nodes,
            remote_failures,
        })
    }

    // Broadcast a frame to every local session in one connection system 广播一帧到某个连接体系的全部本地会话
    pub fn broadcast_local_by_connection_type(
        &self,
        connection_type: &ConnectionType,
        frame: OutboundFrame,
    ) -> Result<usize> {
        let sessions = self
            .inner
            .registry
            .sessions_for_connection_type(connection_type);
        let mut sent = 0;
        for session in sessions {
            if self.enqueue(&session, frame.clone()).is_ok() {
                sent += 1;
            }
        }
        Ok(sent)
    }

    // Load distributed broadcast routes 加载分布式广播路由
    async fn broadcast_routes(
        &self,
        connection_type: Option<&ConnectionType>,
    ) -> Result<Vec<Route>> {
        let Some(cluster) = &self.inner.cluster else {
            return Ok(Vec::new());
        };
        if !self.inner.config.cluster.enabled {
            return Ok(Vec::new());
        }

        match connection_type {
            Some(connection_type) => cluster.presence.list_routes(connection_type).await,
            None => cluster.presence.list_all_routes().await,
        }
    }

    // Publish one cluster envelope and update runtime counters 发布一个集群信封并更新运行计数
    pub(crate) async fn publish_cluster_envelope(
        &self,
        node_id: &crate::identity::NodeId,
        envelope: ClusterEnvelope,
    ) -> Result<()> {
        let Some(cluster) = &self.inner.cluster else {
            return Ok(());
        };
        let result = cluster.publisher.publish(node_id, envelope).await;
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

    // Publish grouped remote routes and keep going after node failures 按节点分组发布远端路由并在节点失败后继续
    pub(super) async fn publish_grouped_remote_routes(
        &self,
        routes: Vec<Route>,
        envelope: ClusterEnvelope,
        include_current_node: bool,
    ) -> Result<(usize, usize)> {
        if self.inner.cluster.is_none() || !self.inner.config.cluster.enabled {
            return Ok((0, 0));
        }

        let mut grouped = HashMap::<crate::identity::NodeId, Vec<Route>>::new();
        for route in routes {
            if include_current_node || route.node_id != self.inner.config.node_id {
                grouped
                    .entry(route.node_id.clone())
                    .or_default()
                    .push(route);
            }
        }

        let mut remote_nodes = 0;
        let mut remote_failures = 0;
        for (node_id, _) in grouped {
            match self
                .publish_cluster_envelope(&node_id, envelope.clone())
                .await
            {
                Ok(()) => {
                    remote_nodes += 1;
                }
                Err(_) => {
                    remote_failures += 1;
                }
            }
        }

        Ok((remote_nodes, remote_failures))
    }
}

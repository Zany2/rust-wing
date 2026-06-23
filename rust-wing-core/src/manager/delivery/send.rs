use crate::cluster::{ClusterEnvelope, ClusterTarget};
use crate::error::Result;
use crate::identity::{ClientId, ConnectionType, NodeId, SessionId, UserId};
use crate::protocol::{FrameKind, OutboundFrame};
use crate::session::Session;

use super::super::registry::ClientRouteKey;
use super::super::{DeliveryReport, RustWing};

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
                )
                .with_origin_node(self.inner.config.node_id.clone()),
                &frame,
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
            let sent = usize::from(self.enqueue_with_ack(&session, frame).is_ok());
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
                ClusterEnvelope::new_for_session(session_id.clone(), frame.clone())
                    .with_origin_node(self.inner.config.node_id.clone()),
                &frame,
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
        self.send_local_with_origin(connection_type, user_id, frame, None)
    }

    // Send a frame to local sessions with optional acknowledgement origin 向本地会话发送带可选确认发起节点的一帧
    pub(super) fn send_local_with_origin(
        &self,
        connection_type: &ConnectionType,
        user_id: &UserId,
        frame: OutboundFrame,
        origin_node_id: Option<NodeId>,
    ) -> Result<usize> {
        let sessions = self.sessions_for_user(connection_type, user_id);
        let mut sent = 0;
        for session in sessions {
            if self
                .enqueue_with_ack_origin(&session, frame.clone(), origin_node_id.clone())
                .is_ok()
            {
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
        self.send_local_to_client_key_with_origin(client_key, frame, None)
    }

    // Send a frame to a local user-client key with optional acknowledgement origin 向本地用户客户端键发送带可选确认发起节点的一帧
    pub(super) fn send_local_to_client_key_with_origin(
        &self,
        client_key: &ClientRouteKey,
        frame: OutboundFrame,
        origin_node_id: Option<NodeId>,
    ) -> Result<usize> {
        let sessions = self.inner.registry.sessions_for_client_key(client_key);
        let mut sent = 0;
        for session in sessions {
            if self
                .enqueue_with_ack_origin(&session, frame.clone(), origin_node_id.clone())
                .is_ok()
            {
                sent += 1;
            }
        }
        Ok(sent)
    }

    // Queue a frame and register acknowledgement tracking when requested 入队一帧并在需要时登记确认追踪
    fn enqueue_with_ack(&self, session: &Session, frame: OutboundFrame) -> Result<()> {
        self.enqueue_with_ack_origin(session, frame, None)
    }

    // Queue a frame and remember an optional acknowledgement origin 入队一帧并记录可选确认发起节点
    pub(super) fn enqueue_with_ack_origin(
        &self,
        session: &Session,
        frame: OutboundFrame,
        origin_node_id: Option<NodeId>,
    ) -> Result<()> {
        if frame.kind == FrameKind::Close {
            session.close(frame.payload);
            return Ok(());
        }

        let message_id = frame.message_id.clone();
        if let Err(error) = session.enqueue(frame) {
            self.inner.stats.record_outbound_frame_failed();
            return Err(error);
        }
        self.inner.stats.record_outbound_frame_enqueued();
        if let Some(message_id) = message_id {
            self.inner.acks.track_with_origin(
                session.id().clone(),
                message_id,
                self.inner.config.ack_ttl,
                origin_node_id,
            )?;
        }
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
                ClusterEnvelope::new(connection_type.clone(), user_id.clone(), frame.clone())
                    .with_origin_node(self.inner.config.node_id.clone()),
                &frame,
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
        let origin_node_id = envelope.origin_node_id.clone();
        let target = envelope.target.clone();
        let frame = envelope.into_frame();
        match target {
            ClusterTarget::User {
                connection_type,
                user_id,
            } => self.send_local_with_origin(&connection_type, &user_id, frame, origin_node_id),
            ClusterTarget::Client {
                connection_type,
                user_id,
                client_id,
            } => {
                let client_key = ClientRouteKey::new(connection_type, user_id, client_id);
                self.send_local_to_client_key_with_origin(&client_key, frame, origin_node_id)
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
                Ok(usize::from(
                    self.enqueue_with_ack_origin(&session, frame, origin_node_id)
                        .is_ok(),
                ))
            }
            ClusterTarget::Broadcast { connection_type } => self
                .broadcast_local_by_connection_type_with_origin(
                    &connection_type,
                    frame,
                    origin_node_id,
                ),
            ClusterTarget::BroadcastAll => self.broadcast_local_with_origin(frame, origin_node_id),
            ClusterTarget::Ack {
                session_id,
                message_id,
                stage,
                client_time,
            } => {
                let update =
                    self.inner
                        .acks
                        .acknowledge(&session_id, &message_id, stage, client_time)?;
                Ok(usize::from(update.updated))
            }
        }
    }
}

use std::collections::HashMap;

use crate::cluster::{ClusterEnvelope, ClusterTarget, Route};
use crate::error::{Result, RustWingError};
use crate::identity::{ClientId, ConnectionType, SessionId, UserId};
use crate::lifecycle::DisconnectCause;
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
        let sent = self
            .send_local_to_client_key_async(&client_key, frame.clone())
            .await?;

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
            let sent = usize::from(self.enqueue_async(&session, frame).await.is_ok());
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

    // Send a frame to one local client key and await failed-session cleanup 向本地客户端键发送一帧并等待失败会话清理
    async fn send_local_to_client_key_async(
        &self,
        client_key: &ClientRouteKey,
        frame: OutboundFrame,
    ) -> Result<usize> {
        let sessions = self.inner.registry.sessions_for_client_key(client_key);
        Ok(self.enqueue_local_sessions_async(sessions, frame).await)
    }

    // Queue one frame and update runtime counters 入队一帧并更新运行计数
    pub(super) fn enqueue(&self, session: &Session, frame: OutboundFrame) -> Result<()> {
        if frame.kind == FrameKind::Close {
            let cause = DisconnectCause::ServerRequested {
                reason: String::from_utf8_lossy(&frame.payload).into_owned(),
            };
            self.remove_local_session_with_cause(session, cause);
            return Ok(());
        }

        if let Err(error) = session.enqueue(frame) {
            self.inner.stats.record_outbound_frame_failed();
            let cause = enqueue_failure_cause(&error);
            self.remove_local_session_with_cause(session, cause);
            return Err(error);
        }
        self.inner.stats.record_outbound_frame_enqueued();
        Ok(())
    }

    // Queue one frame and await complete cleanup when the session can no longer write 入队一帧并在会话无法继续写入时等待完整清理
    async fn enqueue_async(&self, session: &Session, frame: OutboundFrame) -> Result<()> {
        if frame.kind == FrameKind::Close {
            let cause = close_frame_cause(&frame, None);
            self.unregister_with_cause(session, cause).await?;
            return Ok(());
        }

        if let Err(error) = session.enqueue(frame) {
            self.inner.stats.record_outbound_frame_failed();
            let cause = enqueue_failure_cause(&error);
            self.unregister_with_cause(session, cause).await?;
            return Err(error);
        }
        self.inner.stats.record_outbound_frame_enqueued();
        Ok(())
    }

    // Queue one frame for local sessions and await failed-session cleanup 向本地会话入队一帧并等待失败会话清理
    async fn enqueue_local_sessions_async(
        &self,
        sessions: Vec<Session>,
        frame: OutboundFrame,
    ) -> usize {
        let mut sent = 0;
        for session in sessions {
            if self.enqueue_async(&session, frame.clone()).await.is_ok() {
                sent += 1;
            }
        }
        sent
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
        let sessions = self.sessions_for_user(&connection_type, &user_id);
        let sent = self
            .enqueue_local_sessions_async(sessions, frame.clone())
            .await;

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

    // Deliver a received cluster envelope locally and await complete session cleanup 在本地投递集群信封并等待会话完整清理
    pub async fn handle_cluster_envelope_async(&self, envelope: ClusterEnvelope) -> Result<usize> {
        let disconnect_cause = envelope.disconnect_cause.clone();
        let target = envelope.target.clone();
        let frame = envelope.into_frame();
        if frame.kind == FrameKind::Close {
            let cause = close_frame_cause(&frame, disconnect_cause);
            let sessions = self.sessions_for_cluster_target(&target)?;
            return self
                .unregister_local_sessions_with_cause(sessions, cause)
                .await;
        }

        let sessions = self.sessions_for_cluster_target(&target)?;
        Ok(self.enqueue_local_sessions_async(sessions, frame).await)
    }

    // Deliver a received cluster envelope locally 在本地投递收到的集群信封
    pub fn handle_cluster_envelope(&self, envelope: ClusterEnvelope) -> Result<usize> {
        let disconnect_cause = envelope.disconnect_cause.clone();
        let target = envelope.target.clone();
        let frame = envelope.into_frame();
        if frame.kind == FrameKind::Close {
            let cause = close_frame_cause(&frame, disconnect_cause);
            let sessions = self.sessions_for_cluster_target(&target)?;
            let removed = sessions
                .into_iter()
                .filter(|session| self.remove_local_session_with_cause(session, cause.clone()))
                .count();
            return Ok(removed);
        }

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
                Ok(usize::from(self.enqueue(&session, frame).is_ok()))
            }
            ClusterTarget::Broadcast { connection_type } => {
                self.broadcast_local_by_connection_type(&connection_type, frame)
            }
            ClusterTarget::BroadcastAll => self.broadcast_local(frame),
        }
    }

    // Resolve local sessions addressed by one cluster target 解析一个集群目标对应的本地会话
    fn sessions_for_cluster_target(&self, target: &ClusterTarget) -> Result<Vec<Session>> {
        match target {
            ClusterTarget::User {
                connection_type,
                user_id,
            } => Ok(self.sessions_for_user(connection_type, user_id)),
            ClusterTarget::Client {
                connection_type,
                user_id,
                client_id,
            } => {
                let client_key = ClientRouteKey::new(
                    connection_type.clone(),
                    user_id.clone(),
                    client_id.clone(),
                );
                Ok(self.inner.registry.sessions_for_client_key(&client_key))
            }
            ClusterTarget::Session { session_id } => {
                Ok(self.get_session(session_id)?.into_iter().collect())
            }
            ClusterTarget::Broadcast { connection_type } => Ok(self
                .inner
                .registry
                .sessions_for_connection_type(connection_type)),
            ClusterTarget::BroadcastAll => Ok(self.all_sessions()),
        }
    }

    // Remove local sessions with one exact cause and await Presence cleanup 使用同一精确原因移除本地会话并等待 Presence 清理
    async fn unregister_local_sessions_with_cause(
        &self,
        sessions: Vec<Session>,
        cause: DisconnectCause,
    ) -> Result<usize> {
        let mut removed = 0;
        for session in sessions {
            removed += usize::from(self.unregister_with_cause(&session, cause.clone()).await?);
        }
        Ok(removed)
    }
}

// Convert a close frame and optional cluster metadata into one exact cause 将关闭帧和可选集群元数据转换为精确原因
fn close_frame_cause(frame: &OutboundFrame, cause: Option<DisconnectCause>) -> DisconnectCause {
    cause.unwrap_or_else(|| DisconnectCause::ServerRequested {
        reason: String::from_utf8_lossy(&frame.payload).into_owned(),
    })
}

// Convert a local enqueue failure into a typed disconnect cause 将本地入队失败转换为类型化断开原因
pub(super) fn enqueue_failure_cause(error: &RustWingError) -> DisconnectCause {
    match error {
        RustWingError::QueueFull => DisconnectCause::OutboundQueueFull,
        RustWingError::SessionClosed => DisconnectCause::OutboundReceiverClosed,
        _ => DisconnectCause::TransportError {
            message: error.to_string(),
        },
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
        let sessions = self
            .inner
            .registry
            .sessions_for_connection_type(&connection_type);
        let sent = self
            .enqueue_local_sessions_async(sessions, frame.clone())
            .await;
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
        let sent = self
            .enqueue_local_sessions_async(self.all_sessions(), frame.clone())
            .await;
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

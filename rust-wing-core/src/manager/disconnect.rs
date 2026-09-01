use std::collections::HashSet;

use crate::cluster::{ClusterEnvelope, Route};
use crate::error::Result;
use crate::identity::{ClientId, ConnectionType, SessionId, UserId};
use crate::lifecycle::DisconnectCause;
use crate::protocol::OutboundFrame;

use super::registry::ClientRouteKey;
use super::{DeliveryReport, RustWing};

impl RustWing {
    // Disconnect all default-system sessions of one user 断开默认连接体系中某个用户的全部会话
    pub async fn disconnect_user(
        &self,
        user_id: impl Into<UserId>,
        reason: impl Into<Vec<u8>>,
    ) -> Result<DeliveryReport> {
        self.disconnect_user_in(ConnectionType::default(), user_id, reason)
            .await
    }

    // Disconnect all sessions of one user in a connection system 断开某个连接体系中某个用户的全部会话
    pub async fn disconnect_user_in(
        &self,
        connection_type: impl Into<ConnectionType>,
        user_id: impl Into<UserId>,
        reason: impl Into<Vec<u8>>,
    ) -> Result<DeliveryReport> {
        let connection_type = connection_type.into();
        let user_id = user_id.into();
        let reason = reason.into();
        let cause = server_requested_cause(&reason);
        let local_sessions = self
            .disconnect_local_user(&connection_type, &user_id, cause.clone())
            .await?;
        let remote_nodes = self
            .disconnect_remote_user_routes(&connection_type, &user_id, None, reason, cause)
            .await?;
        let report = DeliveryReport {
            local_sessions,
            remote_nodes,
            remote_failures: 0,
        };
        self.inner
            .stats
            .record_disconnect(report.local_sessions, report.remote_nodes);
        Ok(report)
    }

    // Disconnect default-system sessions in one user client slot 断开默认连接体系中某个用户客户端槽位的会话
    pub async fn disconnect_client<C>(
        &self,
        user_id: impl Into<UserId>,
        client_id: Option<C>,
        reason: impl Into<Vec<u8>>,
    ) -> Result<DeliveryReport>
    where
        C: Into<ClientId>,
    {
        self.disconnect_client_in(ConnectionType::default(), user_id, client_id, reason)
            .await
    }

    // Disconnect sessions in one user client slot inside a connection system 断开某个连接体系中某个用户客户端槽位的会话
    pub async fn disconnect_client_in<C>(
        &self,
        connection_type: impl Into<ConnectionType>,
        user_id: impl Into<UserId>,
        client_id: Option<C>,
        reason: impl Into<Vec<u8>>,
    ) -> Result<DeliveryReport>
    where
        C: Into<ClientId>,
    {
        let connection_type = connection_type.into();
        let user_id = user_id.into();
        let client_id = client_id.map(Into::into);
        let reason = reason.into();
        let cause = server_requested_cause(&reason);
        let local_sessions = self
            .disconnect_local_client(&connection_type, &user_id, client_id.clone(), cause.clone())
            .await?;
        let remote_nodes = self
            .disconnect_remote_user_routes(
                &connection_type,
                &user_id,
                Some(client_id),
                reason,
                cause,
            )
            .await?;
        let report = DeliveryReport {
            local_sessions,
            remote_nodes,
            remote_failures: 0,
        };
        self.inner
            .stats
            .record_disconnect(report.local_sessions, report.remote_nodes);
        Ok(report)
    }

    // Disconnect one exact session locally or through the cluster 本地或经由集群断开一条精确会话
    pub async fn disconnect_session(
        &self,
        session_id: &SessionId,
        reason: impl Into<Vec<u8>>,
    ) -> Result<DeliveryReport> {
        let reason = reason.into();
        let cause = server_requested_cause(&reason);
        if let Some(session) = self.get_session(session_id)? {
            let removed = self.unregister_with_cause(&session, cause).await?;
            let report = DeliveryReport {
                local_sessions: usize::from(removed),
                remote_nodes: 0,
                remote_failures: 0,
            };
            self.inner
                .stats
                .record_disconnect(report.local_sessions, report.remote_nodes);
            return Ok(report);
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
            cluster
                .presence
                .remove(&route.connection_type, &route.user_id, &route.session_id)
                .await?;
            return Ok(DeliveryReport::default());
        }

        self.publish_disconnect(&route, reason, cause).await?;
        cluster
            .presence
            .remove(&route.connection_type, &route.user_id, &route.session_id)
            .await?;
        let report = DeliveryReport {
            local_sessions: 0,
            remote_nodes: 1,
            remote_failures: 0,
        };
        self.inner
            .stats
            .record_disconnect(report.local_sessions, report.remote_nodes);
        Ok(report)
    }

    // Disconnect every session in one connection system 断开某个连接体系中的全部会话
    pub async fn disconnect_all_in(
        &self,
        connection_type: impl Into<ConnectionType>,
        reason: impl Into<Vec<u8>>,
    ) -> Result<DeliveryReport> {
        let connection_type = connection_type.into();
        let reason = reason.into();
        let cause = server_requested_cause(&reason);
        let sessions = self
            .inner
            .registry
            .sessions_for_connection_type(&connection_type);
        let mut local_sessions = 0;
        for session in &sessions {
            local_sessions +=
                usize::from(self.unregister_with_cause(session, cause.clone()).await?);
        }
        let routes = self
            .cluster_routes_for_disconnect(Some(&connection_type))
            .await?;
        let remote_nodes = self.disconnect_remote_routes(routes, reason, cause).await?;
        let report = DeliveryReport {
            local_sessions,
            remote_nodes,
            remote_failures: 0,
        };
        self.inner
            .stats
            .record_disconnect(report.local_sessions, report.remote_nodes);
        Ok(report)
    }

    // Disconnect every local and remote session 断开全部本地与远端会话
    pub async fn disconnect_all(&self, reason: impl Into<Vec<u8>>) -> Result<DeliveryReport> {
        let reason = reason.into();
        let cause = server_requested_cause(&reason);
        let sessions = self.all_sessions();
        let mut local_sessions = 0;
        for session in &sessions {
            local_sessions +=
                usize::from(self.unregister_with_cause(session, cause.clone()).await?);
        }
        let routes = self.cluster_routes_for_disconnect(None).await?;
        let remote_nodes = self.disconnect_remote_routes(routes, reason, cause).await?;
        let report = DeliveryReport {
            local_sessions,
            remote_nodes,
            remote_failures: 0,
        };
        self.inner
            .stats
            .record_disconnect(report.local_sessions, report.remote_nodes);
        Ok(report)
    }

    // Close routes displaced by an atomic local claim 关闭原子仲裁中被替换的路由
    pub(super) async fn close_displaced_cluster_sessions(&self, routes: Vec<Route>) -> Result<()> {
        if self.inner.cluster.is_none() || !self.inner.config.cluster.enabled {
            return Ok(());
        }

        for route in routes {
            if route.node_id == self.inner.config.node_id {
                if let Some(displaced) = self.get_session(&route.session_id)? {
                    self.unregister_with_cause_unlocked(&displaced, DisconnectCause::Replaced)
                        .await?;
                }
                continue;
            }

            self.publish_cluster_envelope(
                &route.node_id,
                ClusterEnvelope::new_for_session(
                    route.session_id.clone(),
                    OutboundFrame::close("replaced by a newer connection"),
                )
                .with_disconnect_cause(DisconnectCause::Replaced),
            )
            .await?;
        }

        Ok(())
    }

    // Disconnect local sessions for one user 断开某个用户的本地会话
    async fn disconnect_local_user(
        &self,
        connection_type: &ConnectionType,
        user_id: &UserId,
        cause: DisconnectCause,
    ) -> Result<usize> {
        let sessions = self.sessions_for_user(connection_type, user_id);
        let mut removed = 0;
        for session in &sessions {
            removed += usize::from(self.unregister_with_cause(session, cause.clone()).await?);
        }
        Ok(removed)
    }

    // Disconnect local sessions for one user-client key 断开某个用户客户端键的本地会话
    async fn disconnect_local_client(
        &self,
        connection_type: &ConnectionType,
        user_id: &UserId,
        client_id: Option<ClientId>,
        cause: DisconnectCause,
    ) -> Result<usize> {
        let client_key = ClientRouteKey::new(connection_type.clone(), user_id.clone(), client_id);
        let sessions = self.inner.registry.sessions_for_client_key(&client_key);
        let mut removed = 0;
        for session in &sessions {
            removed += usize::from(self.unregister_with_cause(session, cause.clone()).await?);
        }
        Ok(removed)
    }

    // Disconnect matching remote user routes through cluster messages 通过集群消息断开匹配的远端用户路由
    async fn disconnect_remote_user_routes(
        &self,
        connection_type: &ConnectionType,
        user_id: &UserId,
        client_id: Option<Option<ClientId>>,
        reason: Vec<u8>,
        cause: DisconnectCause,
    ) -> Result<usize> {
        let Some(cluster) = &self.inner.cluster else {
            return Ok(0);
        };
        if !self.inner.config.cluster.enabled {
            return Ok(0);
        }

        let routes = cluster.presence.locate(connection_type, user_id).await?;
        let mut remote_nodes = HashSet::new();
        for route in routes {
            if let Some(expected_client_id) = &client_id {
                if route.client_id != *expected_client_id {
                    continue;
                }
            }
            if route.node_id == self.inner.config.node_id {
                if let Some(session) = self.get_session(&route.session_id)? {
                    self.unregister_with_cause(&session, cause.clone()).await?;
                } else {
                    cluster
                        .presence
                        .remove(&route.connection_type, &route.user_id, &route.session_id)
                        .await?;
                }
                continue;
            }

            self.publish_disconnect(&route, reason.clone(), cause.clone())
                .await?;
            cluster
                .presence
                .remove(&route.connection_type, &route.user_id, &route.session_id)
                .await?;
            remote_nodes.insert(route.node_id);
        }
        Ok(remote_nodes.len())
    }

    // List cluster routes for disconnect operations 列出断开操作使用的集群路由
    async fn cluster_routes_for_disconnect(
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

    // Disconnect already selected remote routes 断开已经筛选出的远端路由
    async fn disconnect_remote_routes(
        &self,
        routes: Vec<Route>,
        reason: Vec<u8>,
        cause: DisconnectCause,
    ) -> Result<usize> {
        let Some(cluster) = &self.inner.cluster else {
            return Ok(0);
        };
        if !self.inner.config.cluster.enabled {
            return Ok(0);
        }

        let mut remote_nodes = HashSet::new();
        for route in routes {
            if route.node_id == self.inner.config.node_id {
                if let Some(session) = self.get_session(&route.session_id)? {
                    self.unregister_with_cause(&session, cause.clone()).await?;
                } else {
                    cluster
                        .presence
                        .remove(&route.connection_type, &route.user_id, &route.session_id)
                        .await?;
                }
                continue;
            }

            self.publish_disconnect(&route, reason.clone(), cause.clone())
                .await?;
            cluster
                .presence
                .remove(&route.connection_type, &route.user_id, &route.session_id)
                .await?;
            remote_nodes.insert(route.node_id);
        }
        Ok(remote_nodes.len())
    }

    // Publish a remote session close envelope 发布远端会话关闭信封
    async fn publish_disconnect(
        &self,
        route: &Route,
        reason: Vec<u8>,
        cause: DisconnectCause,
    ) -> Result<()> {
        self.publish_cluster_envelope(
            &route.node_id,
            ClusterEnvelope::new_for_session(
                route.session_id.clone(),
                OutboundFrame::close(reason),
            )
            .with_disconnect_cause(cause),
        )
        .await
    }
}

// Convert a close-frame payload into a typed server request cause 将关闭帧负载转换为类型化服务端请求原因
fn server_requested_cause(reason: &[u8]) -> DisconnectCause {
    DisconnectCause::ServerRequested {
        reason: String::from_utf8_lossy(reason).into_owned(),
    }
}

// Check whether one route is replaced by a newer local session 检查路由是否被新的本地会话替换

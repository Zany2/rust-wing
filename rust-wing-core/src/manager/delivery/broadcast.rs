use std::collections::HashMap;

use crate::cluster::{ClusterEnvelope, ClusterTarget, Route};
use crate::error::Result;
use crate::identity::{ConnectionType, NodeId};
use crate::protocol::{FrameKind, OutboundFrame};

use super::super::{DeliveryReport, RustWing};

impl RustWing {
    // Broadcast a frame to every local session 广播一帧到所有本地会话
    pub fn broadcast_local(&self, frame: OutboundFrame) -> Result<usize> {
        self.broadcast_local_with_origin(frame, None)
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
                ClusterEnvelope::new_for_broadcast(connection_type, frame.clone())
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
                ClusterEnvelope::new_for_broadcast_all(frame.clone())
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

    // Broadcast a frame to every local session of one user 广播一帧到某个用户的全部本地会话
    // Broadcast a frame to every local session in one connection system 广播一帧到某个连接体系的全部本地会话
    pub fn broadcast_local_by_connection_type(
        &self,
        connection_type: &ConnectionType,
        frame: OutboundFrame,
    ) -> Result<usize> {
        self.broadcast_local_by_connection_type_with_origin(connection_type, frame, None)
    }

    // Broadcast a frame locally with optional acknowledgement origin 本地广播时可附带确认发起节点
    pub(super) fn broadcast_local_with_origin(
        &self,
        frame: OutboundFrame,
        origin_node_id: Option<NodeId>,
    ) -> Result<usize> {
        let sessions = self.all_sessions();
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

    // Broadcast a frame in one connection system with optional acknowledgement origin 在连接体系内广播时可附带确认发起节点
    pub(super) fn broadcast_local_by_connection_type_with_origin(
        &self,
        connection_type: &ConnectionType,
        frame: OutboundFrame,
        origin_node_id: Option<NodeId>,
    ) -> Result<usize> {
        let sessions = self
            .inner
            .registry
            .sessions_for_connection_type(connection_type);
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

    // Load distributed broadcast routes for acknowledgement tracking 加载分布式广播路由以追踪确认
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
        frame: &OutboundFrame,
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
        for (node_id, node_routes) in grouped {
            match self
                .publish_cluster_envelope(&node_id, envelope.clone())
                .await
            {
                Ok(()) => {
                    for route in node_routes {
                        self.track_remote_ack(&route, frame)?;
                    }
                    remote_nodes += 1;
                }
                Err(_) => {
                    remote_failures += 1;
                }
            }
        }

        Ok((remote_nodes, remote_failures))
    }

    // Track a remote route when a frame requires acknowledgement 当帧需要确认时追踪远端路由
    fn track_remote_ack(&self, route: &Route, frame: &OutboundFrame) -> Result<()> {
        if let Some(message_id) = &frame.message_id {
            self.inner.acks.track(
                route.session_id.clone(),
                message_id.clone(),
                self.inner.config.ack_ttl,
            )?;
        }
        Ok(())
    }

    // Publish grouped remote routes and keep going after node failures 按节点分组发布远端路由并在节点失败后继续
    pub(super) async fn handle_cluster_broadcast(
        &self,
        envelope: ClusterEnvelope,
    ) -> Result<usize> {
        let origin_node_id = envelope.origin_node_id.clone();
        let target = envelope.target.clone();
        let frame = envelope.into_frame();
        match target {
            ClusterTarget::Broadcast { connection_type } => self
                .broadcast_local_by_connection_type_with_origin(
                    &connection_type,
                    frame,
                    origin_node_id,
                ),
            ClusterTarget::BroadcastAll => self.broadcast_local_with_origin(frame, origin_node_id),
            _ => Ok(0),
        }
    }
}

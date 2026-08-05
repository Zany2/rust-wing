use std::sync::atomic::{AtomicU64, Ordering};

use crate::error::Result;
use crate::identity::NodeId;

use super::RustWing;

// Runtime counters shared by manager internals 管理器内部共享的运行计数器
#[derive(Default)]
pub(super) struct RuntimeStats {
    // Local outbound frames accepted by session queues 本地会话队列接受的出站帧数量
    outbound_frames_enqueued_total: AtomicU64,
    // Local outbound frames rejected by session queues 本地会话队列拒绝的出站帧数量
    outbound_frames_failed_total: AtomicU64,
    // Cluster publishes that completed successfully 集群发布成功完成的数量
    cluster_publishes_success_total: AtomicU64,
    // Cluster publishes that failed 集群发布失败的数量
    cluster_publishes_failed_total: AtomicU64,
    // Liveness probes successfully queued by maintenance 维护任务成功入队的存活探测数量
    maintenance_probes_sent_total: AtomicU64,
    // Sessions removed by managed maintenance 托管维护任务移除的会话数量
    maintenance_sessions_reaped_total: AtomicU64,
    // Local sessions disconnected by explicit APIs 显式断开接口移除的本地会话数量
    disconnected_local_sessions_total: AtomicU64,
    // Remote nodes notified by explicit disconnect APIs 显式断开接口通知的远端节点数量
    disconnected_remote_nodes_total: AtomicU64,
}

// Runtime statistics snapshot 运行统计快照
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct StatsSnapshot {
    // Current node identifier 当前节点标识
    pub node_id: NodeId,
    // Active local session count 当前本地活跃会话数
    pub local_connections: usize,
    // Unique local user count 当前本地去重用户数
    pub local_users: usize,
    // Live cluster nodes visible to the configured presence store 当前在线状态存储可见的集群节点数
    pub cluster_nodes: usize,
    // Live cluster routes visible to the configured presence store 当前在线状态存储可见的集群路由数
    pub cluster_routes: usize,
    // Local outbound frames accepted by session queues 本地会话队列接受的出站帧总数
    pub outbound_frames_enqueued_total: u64,
    // Local outbound frames rejected by session queues 本地会话队列拒绝的出站帧总数
    pub outbound_frames_failed_total: u64,
    // Cluster publishes that completed successfully 集群发布成功总数
    pub cluster_publishes_success_total: u64,
    // Cluster publishes that failed 集群发布失败总数
    pub cluster_publishes_failed_total: u64,
    // Liveness probes successfully queued by maintenance 维护任务成功入队的存活探测总数
    pub maintenance_probes_sent_total: u64,
    // Sessions removed by managed maintenance 托管维护任务移除的会话总数
    pub maintenance_sessions_reaped_total: u64,
    // Local sessions disconnected by explicit APIs 显式断开接口移除的本地会话总数
    pub disconnected_local_sessions_total: u64,
    // Remote nodes notified by explicit disconnect APIs 显式断开接口通知的远端节点总数
    pub disconnected_remote_nodes_total: u64,
}

impl RustWing {
    // Capture a lightweight runtime statistics snapshot 捕获轻量运行统计快照
    pub fn stats_snapshot(&self) -> Result<StatsSnapshot> {
        let cluster_enabled = self.inner.cluster.is_some() && self.inner.config.cluster.enabled;
        let local_connections = self.connection_count()?;
        Ok(self.inner.stats.snapshot(
            self.inner.config.node_id.clone(),
            local_connections,
            self.inner.registry.user_count(),
            usize::from(cluster_enabled),
            if cluster_enabled {
                local_connections
            } else {
                0
            },
        ))
    }

    // Capture a detailed runtime statistics snapshot with live cluster visibility 捕获包含实时集群可见性的详细运行统计快照
    pub async fn detailed_stats_snapshot(&self) -> Result<StatsSnapshot> {
        let cluster_enabled = self.inner.cluster.is_some() && self.inner.config.cluster.enabled;
        let local_connections = self.connection_count()?;
        let cluster_nodes = if cluster_enabled {
            self.list_cluster_nodes().await?.len()
        } else {
            0
        };
        let cluster_routes = if cluster_enabled {
            self.list_all_cluster_routes().await?.len()
        } else {
            0
        };
        Ok(self.inner.stats.snapshot(
            self.inner.config.node_id.clone(),
            local_connections,
            self.inner.registry.user_count(),
            cluster_nodes,
            cluster_routes,
        ))
    }
}

impl RuntimeStats {
    // Record one local outbound frame accepted by a queue 记录一个已被本地队列接受的出站帧
    pub(super) fn record_outbound_frame_enqueued(&self) {
        self.outbound_frames_enqueued_total
            .fetch_add(1, Ordering::Relaxed);
    }

    // Record one local outbound frame rejected by a queue 记录一个被本地队列拒绝的出站帧
    pub(super) fn record_outbound_frame_failed(&self) {
        self.outbound_frames_failed_total
            .fetch_add(1, Ordering::Relaxed);
    }

    // Record one successful cluster publish 记录一次成功的集群发布
    pub(super) fn record_cluster_publish_success(&self) {
        self.cluster_publishes_success_total
            .fetch_add(1, Ordering::Relaxed);
    }

    // Record one failed cluster publish 记录一次失败的集群发布
    pub(super) fn record_cluster_publish_failed(&self) {
        self.cluster_publishes_failed_total
            .fetch_add(1, Ordering::Relaxed);
    }

    // Record one successfully queued liveness probe 记录一个成功入队的存活探测
    pub(super) fn record_maintenance_probe_sent(&self) {
        self.maintenance_probes_sent_total
            .fetch_add(1, Ordering::Relaxed);
    }

    // Record sessions reaped by managed maintenance 记录托管维护移除的会话数量
    pub(super) fn record_maintenance_sessions_reaped(&self, count: usize) {
        self.maintenance_sessions_reaped_total
            .fetch_add(count as u64, Ordering::Relaxed);
    }

    // Record sessions and nodes disconnected by explicit APIs 记录显式断开接口移除的会话和通知的节点
    pub(super) fn record_disconnect(&self, local_sessions: usize, remote_nodes: usize) {
        self.disconnected_local_sessions_total
            .fetch_add(local_sessions as u64, Ordering::Relaxed);
        self.disconnected_remote_nodes_total
            .fetch_add(remote_nodes as u64, Ordering::Relaxed);
    }

    // Capture counter values for a public snapshot 捕获计数器值用于公开快照
    pub(super) fn snapshot(
        &self,
        node_id: NodeId,
        local_connections: usize,
        local_users: usize,
        cluster_nodes: usize,
        cluster_routes: usize,
    ) -> StatsSnapshot {
        StatsSnapshot {
            node_id,
            local_connections,
            local_users,
            cluster_nodes,
            cluster_routes,
            outbound_frames_enqueued_total: self
                .outbound_frames_enqueued_total
                .load(Ordering::Relaxed),
            outbound_frames_failed_total: self.outbound_frames_failed_total.load(Ordering::Relaxed),
            cluster_publishes_success_total: self
                .cluster_publishes_success_total
                .load(Ordering::Relaxed),
            cluster_publishes_failed_total: self
                .cluster_publishes_failed_total
                .load(Ordering::Relaxed),
            maintenance_probes_sent_total: self
                .maintenance_probes_sent_total
                .load(Ordering::Relaxed),
            maintenance_sessions_reaped_total: self
                .maintenance_sessions_reaped_total
                .load(Ordering::Relaxed),
            disconnected_local_sessions_total: self
                .disconnected_local_sessions_total
                .load(Ordering::Relaxed),
            disconnected_remote_nodes_total: self
                .disconnected_remote_nodes_total
                .load(Ordering::Relaxed),
        }
    }
}

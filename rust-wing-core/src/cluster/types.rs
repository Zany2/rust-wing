use serde::{Deserialize, Serialize};

use crate::identity::{ClientId, ConnectionType, NodeId, SessionId, UserId};
use crate::protocol::{FrameKind, OutboundFrame};

// Cluster routing entry 集群路由条目
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Route {
    // Routed connection system identifier 被路由的连接体系标识
    pub connection_type: ConnectionType,
    // Routed user identifier 被路由的用户标识
    pub user_id: UserId,
    // Optional routed client identifier 可选路由客户端标识
    pub client_id: Option<ClientId>,
    // Active session identifier 活跃会话标识
    pub session_id: SessionId,
    // Owning node identifier 所属节点标识
    pub node_id: NodeId,
}

// Cluster node lease registration result 集群节点租约注册结果
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeLease {
    // The node id lease was acquired 节点标识租约已获取
    Acquired,
    // The same runtime instance refreshed its lease 同一运行实例刷新了租约
    Refreshed,
    // Another runtime instance already owns this node id 另一个运行实例已占用该节点标识
    Conflict,
}

// Cross-node delivery target 跨节点投递目标
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClusterTarget {
    // Target one user's sessions 定向到一个用户的会话
    User {
        // Target connection system identifier 目标连接体系标识
        connection_type: ConnectionType,
        // Target user identifier 目标用户标识
        user_id: UserId,
    },
    // Target one client slot for a user 定向到用户的一个客户端槽位
    Client {
        // Target connection system identifier 目标连接体系标识
        connection_type: ConnectionType,
        // Target user identifier 目标用户标识
        user_id: UserId,
        // Optional target client identifier 可选目标客户端标识
        client_id: Option<ClientId>,
    },
    // Target one exact session 定向到一条精确会话
    Session {
        // Target session identifier 目标会话标识
        session_id: SessionId,
    },
    // Target every session in one connection system 定向到一个连接体系的全部会话
    Broadcast {
        // Target connection system identifier 目标连接体系标识
        connection_type: ConnectionType,
    },
    // Target every session on the receiving node 定向到接收节点的全部会话
    BroadcastAll,
}

// Cross-node frame envelope 跨节点帧信封
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterEnvelope {
    // Delivery target carried by the envelope 信封携带的投递目标
    pub target: ClusterTarget,
    // Outbound frame kind 出站帧类型
    pub frame_kind: FrameKind,
    // Original frame payload 原始帧负载
    pub payload: Vec<u8>,
}

impl ClusterEnvelope {
    // Convert a user-targeted frame into a cluster envelope 将用户定向帧转换为集群信封
    pub fn new(connection_type: ConnectionType, user_id: UserId, frame: OutboundFrame) -> Self {
        Self {
            target: ClusterTarget::User {
                connection_type,
                user_id,
            },
            frame_kind: frame.kind,
            payload: frame.payload,
        }
    }

    // Convert a client-targeted frame into a cluster envelope 将客户端定向帧转换为集群信封
    pub fn new_for_client(
        connection_type: ConnectionType,
        user_id: UserId,
        client_id: Option<ClientId>,
        frame: OutboundFrame,
    ) -> Self {
        Self {
            target: ClusterTarget::Client {
                connection_type,
                user_id,
                client_id,
            },
            frame_kind: frame.kind,
            payload: frame.payload,
        }
    }

    // Convert a session-targeted frame into a cluster envelope 将会话定向帧转换为集群信封
    pub fn new_for_session(session_id: SessionId, frame: OutboundFrame) -> Self {
        Self {
            target: ClusterTarget::Session { session_id },
            frame_kind: frame.kind,
            payload: frame.payload,
        }
    }

    // Convert a connection-system broadcast into a cluster envelope 将连接体系广播转换为集群信封
    pub fn new_for_broadcast(connection_type: ConnectionType, frame: OutboundFrame) -> Self {
        Self {
            target: ClusterTarget::Broadcast { connection_type },
            frame_kind: frame.kind,
            payload: frame.payload,
        }
    }

    // Convert a global broadcast into a cluster envelope 将全局广播转换为集群信封
    pub fn new_for_broadcast_all(frame: OutboundFrame) -> Self {
        Self {
            target: ClusterTarget::BroadcastAll,
            frame_kind: frame.kind,
            payload: frame.payload,
        }
    }

    // Recover the original outbound frame 还原原始出站帧
    pub fn into_frame(self) -> OutboundFrame {
        OutboundFrame {
            kind: self.frame_kind,
            payload: self.payload,
        }
    }
}

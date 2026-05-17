// Public cluster module 公共集群模块
pub mod cluster;
// Public config module 公共配置模块
pub mod config;
// Public error module 公共错误模块
pub mod error;
// Public identity module 公共身份模块
pub mod identity;
// Public manager module 公共管理模块
pub mod manager;
// Public protocol module 公共协议模块
pub mod protocol;
// Public session module 公共会话模块
pub mod session;

// Re-export cluster APIs 重导出集群接口
pub use cluster::{
    Cluster, ClusterEnvelope, MemoryPresenceStore, NodePublisher, PresenceStore, Route,
};
// Re-export config APIs 重导出配置接口
pub use config::{ClusterBackendConfig, ClusterConfig, ConnectionPolicy, RustWingConfig};
// Re-export error APIs 重导出错误接口
pub use error::{Result, RustWingError};
// Re-export identity APIs 重导出身份接口
pub use identity::{DeviceId, Identity, NodeId, SessionId, UserId};
// Re-export manager API 重导出管理接口
pub use manager::RustWing;
// Re-export protocol APIs 重导出协议接口
pub use protocol::{
    FrameKind, HeartbeatAckData, HeartbeatData, MessageType, OutboundFrame, WsMessage,
};
// Re-export session APIs 重导出会话接口
pub use session::{AcceptedSession, Session, SessionSnapshot};

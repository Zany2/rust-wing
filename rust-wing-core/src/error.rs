use thiserror::Error;

// Recoverable runtime errors 可恢复的运行时错误
#[derive(Debug, Error)]
pub enum RustWingError {
    // Runtime lifecycle does not currently allow the requested operation 当前运行时生命周期不允许执行所请求操作
    #[error("runtime is not ready: {0}")]
    RuntimeNotReady(String),
    // Session queue cannot accept more frames 会话队列无法接收更多帧
    #[error("session write queue is full")]
    QueueFull,
    // Session has already been closed 会话已经关闭
    #[error("session is closed")]
    SessionClosed,
    // Requested session does not exist 请求的会话不存在
    #[error("session not found")]
    SessionNotFound,
    // Cluster subsystem failed 集群子系统失败
    #[error("cluster transport error: {0}")]
    Cluster(String),
    // Configuration contains an invalid value 配置包含无效值
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),
    // Selected backend is known but unavailable 已选择的后端已知但当前不可用
    #[error("cluster backend is not available yet: {0}")]
    BackendUnavailable(String),
    // JSON serialization failed JSON 序列化失败
    #[error("serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
}

// Crate-wide result alias crate 级结果别名
pub type Result<T> = std::result::Result<T, RustWingError>;

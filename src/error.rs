use thiserror::Error;

#[derive(Debug, Error)]
pub enum RustWingError {
    #[error("session write queue is full")]
    QueueFull,
    #[error("session is closed")]
    SessionClosed,
    #[error("session not found")]
    SessionNotFound,
    #[error("cluster transport error: {0}")]
    Cluster(String),
    #[error("serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
}

pub type Result<T> = std::result::Result<T, RustWingError>;

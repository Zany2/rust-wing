use std::sync::Arc;

use rust_wing_core::{AckStage, RustWing};
use serde::Deserialize;

use crate::auth::AxumSendApiGuard;

// Shared state for the send API 发送接口共享状态
#[derive(Clone)]
pub struct AxumSendApiState {
    // Shared RustWing manager 共享 RustWing 管理器
    pub wing: RustWing,
    // Access guard for external callers 外部调用方的访问保护
    pub guard: Arc<dyn AxumSendApiGuard>,
}

// Request body for sending to one user 向单个用户发送的请求体
#[derive(Debug, Deserialize)]
pub struct SendToUserRequest {
    #[serde(default)]
    pub connection_type: Option<String>,
    pub user_id: String,
    pub message: String,
    #[serde(default)]
    pub require_ack: bool,
    #[serde(default)]
    pub message_id: Option<String>,
}

// Request body for sending to one client slot 向单个客户端槽位发送的请求体
#[derive(Debug, Deserialize)]
pub struct SendToClientRequest {
    #[serde(default)]
    pub connection_type: Option<String>,
    pub user_id: String,
    #[serde(default)]
    pub client_id: Option<String>,
    pub message: String,
    #[serde(default)]
    pub require_ack: bool,
    #[serde(default)]
    pub message_id: Option<String>,
}

// Request body for sending to one exact session 向精确会话发送的请求体
#[derive(Debug, Deserialize)]
pub struct SendToSessionRequest {
    pub session_id: String,
    pub message: String,
    #[serde(default)]
    pub require_ack: bool,
    #[serde(default)]
    pub message_id: Option<String>,
}

// Request body for broadcasting to one connection system 向单连接体系广播的请求体
#[derive(Debug, Deserialize)]
pub struct BroadcastRequest {
    #[serde(default)]
    pub connection_type: Option<String>,
    pub message: String,
    #[serde(default)]
    pub require_ack: bool,
    #[serde(default)]
    pub message_id: Option<String>,
}

// Request body for disconnecting one user 断开单个用户的请求体
#[derive(Debug, Deserialize)]
pub struct DisconnectUserRequest {
    #[serde(default)]
    pub connection_type: Option<String>,
    pub user_id: String,
    #[serde(default)]
    pub reason: Option<String>,
}

// Request body for disconnecting one client slot 断开单个客户端槽位的请求体
#[derive(Debug, Deserialize)]
pub struct DisconnectClientRequest {
    #[serde(default)]
    pub connection_type: Option<String>,
    pub user_id: String,
    #[serde(default)]
    pub client_id: Option<String>,
    #[serde(default)]
    pub reason: Option<String>,
}

// Request body for disconnecting one exact session 断开单条会话的请求体
#[derive(Debug, Deserialize)]
pub struct DisconnectSessionRequest {
    pub session_id: String,
    #[serde(default)]
    pub reason: Option<String>,
}

// Delivery response for send APIs 发送接口响应
#[derive(Debug, serde::Serialize)]
pub struct SendApiResponse {
    pub delivered: usize,
    pub local_sessions: usize,
    pub remote_nodes: usize,
    pub remote_failures: usize,
    pub message_id: Option<String>,
}

// Request body for waiting on acknowledgement 等待确认的请求体
#[derive(Debug, Deserialize)]
pub struct WaitForAckRequest {
    pub message_id: String,
    pub stage: AckStage,
    #[serde(default)]
    pub timeout_ms: Option<u64>,
}

// Response body for acknowledgement queries 确认查询响应体
#[derive(Debug, serde::Serialize)]
pub struct AckApiResponse {
    pub message_id: String,
    pub found: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub required_stage: Option<AckStage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reached: Option<bool>,
    pub sessions: Vec<AckSessionApiResponse>,
}

// Per-session acknowledgement response 单会话确认响应
#[derive(Debug, serde::Serialize)]
pub struct AckSessionApiResponse {
    pub session_id: String,
    pub stage: Option<AckStage>,
    pub client_time: Option<i64>,
    pub server_time: Option<i64>,
}

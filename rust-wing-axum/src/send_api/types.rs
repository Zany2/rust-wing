use std::sync::Arc;

use rust_wing_core::RustWing;
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
}

// Request body for sending to one exact session 向精确会话发送的请求体
#[derive(Debug, Deserialize)]
pub struct SendToSessionRequest {
    pub session_id: String,
    pub message: String,
}

// Request body for broadcasting to one connection system 向单连接体系广播的请求体
#[derive(Debug, Deserialize)]
pub struct BroadcastRequest {
    #[serde(default)]
    pub connection_type: Option<String>,
    pub message: String,
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
}

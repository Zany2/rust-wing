use std::net::SocketAddr;

use async_trait::async_trait;
use axum::{
    Json, Router,
    extract::{Query, State, ws::WebSocketUpgrade},
    http::StatusCode,
    response::Response,
    routing::{get, post},
};
use rust_wing_axum::{AxumMessageContext, AxumMessageHandler, upgrade_with_handler};
use rust_wing_core::{
    ConnectionPolicy, Identity, OutboundFrame, Result as WingResult, RustWing, RustWingConfig,
    UserId, WsMessage,
};
use serde::{Deserialize, Serialize};
use serde_json::json;

// Shared application state 共享应用状态
#[derive(Clone)]
struct AppState {
    // RustWing manages all accepted WebSocket sessions RustWing 管理所有已接入的 WebSocket 会话
    wing: RustWing,
}

// Query parameters used by the demo WebSocket endpoint 示例 WebSocket 端点使用的查询参数
#[derive(Debug, Deserialize)]
struct WsQuery {
    // Demo user id; real applications should derive this from authentication 示例用户标识；真实应用应来自认证
    user: String,
    // Optional browser or device label 可选浏览器或设备标识
    device: Option<String>,
}

// Request body for sending a message to one user 单用户推送请求体
#[derive(Debug, Deserialize)]
struct SendToUserRequest {
    // Target user id 目标用户标识
    user: String,
    // Message content to deliver 要投递的消息内容
    message: String,
}

// Request body for broadcasting a message 广播请求体
#[derive(Debug, Deserialize)]
struct BroadcastRequest {
    // Message content to deliver 要投递的消息内容
    message: String,
}

// Generic API response 通用接口响应
#[derive(Debug, Serialize)]
struct ApiResponse<T> {
    // Whether the operation succeeded 操作是否成功
    ok: bool,
    // Response data 响应数据
    data: T,
}

// Health response 健康检查响应
#[derive(Debug, Serialize)]
struct HealthResponse {
    // Service name 服务名称
    service: &'static str,
    // Active local WebSocket connection count 当前本地 WebSocket 连接数
    connections: usize,
}

// Session response shown by the example API 示例接口展示的会话信息
#[derive(Debug, Serialize)]
struct SessionResponse {
    // Session id 会话标识
    session_id: String,
    // User id 用户标识
    user_id: String,
    // Optional device id 可选设备标识
    device_id: Option<String>,
    // Last activity timestamp in milliseconds 最近活跃时间戳，单位毫秒
    last_active_time: i64,
    // Last heartbeat timestamp in milliseconds 最近心跳时间戳，单位毫秒
    last_heartbeat_time: i64,
    // Whether the session is already closed 会话是否已关闭
    closed: bool,
}

// Delivery response for push APIs 推送接口响应
#[derive(Debug, Serialize)]
struct DeliveryResponse {
    // Number of sessions that accepted the outbound frame 接收出站帧的会话数量
    delivered: usize,
}

#[tokio::main]
async fn main() {
    // Use multi-session mode so one user can open several browser tabs 示例允许同一用户打开多个标签页
    let wing = RustWing::new(RustWingConfig {
        connection_policy: ConnectionPolicy::Multi,
        ..RustWingConfig::default()
    });
    let app = build_router(AppState { wing });
    let address = SocketAddr::from(([127, 0, 0, 1], 3000));

    println!("rust-wing example listening on http://{address}");
    let listener = tokio::net::TcpListener::bind(address)
        .await
        .expect("bind example listener");
    axum::serve(listener, app)
        .await
        .expect("run example server");
}

// Build the example HTTP router 构建示例 HTTP 路由
fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/health", get(health))
        .route("/sessions/{user}", get(list_user_sessions))
        .route("/send", post(send_to_user))
        .route("/broadcast", post(broadcast))
        .route("/ws", get(ws_handler))
        .with_state(state)
}

// Return a small route guide 返回简短路由说明
async fn index() -> Json<ApiResponse<serde_json::Value>> {
    Json(ApiResponse {
        ok: true,
        data: json!({
            "name": "rust-wing-example",
            "routes": {
                "GET /health": "查看连接数量",
                "GET /sessions/{user}": "查看用户会话",
                "POST /send": { "user": "alice", "message": "hello alice" },
                "POST /broadcast": { "message": "hello everyone" },
                "GET /ws?user=alice&device=browser": "建立 WebSocket 连接"
            }
        }),
    })
}

// Report current service status 返回当前服务状态
async fn health(State(state): State<AppState>) -> Json<ApiResponse<HealthResponse>> {
    let connections = state.wing.connection_count().unwrap_or_default();

    Json(ApiResponse {
        ok: true,
        data: HealthResponse {
            service: "rust-wing-example",
            connections,
        },
    })
}

// List active sessions for one user 列出某个用户的活跃会话
async fn list_user_sessions(
    State(state): State<AppState>,
    axum::extract::Path(user): axum::extract::Path<String>,
) -> Json<ApiResponse<Vec<SessionResponse>>> {
    let sessions = state
        .wing
        .list_user_sessions(&UserId::from(user.as_str()))
        .unwrap_or_default()
        .into_iter()
        .map(|session| SessionResponse {
            session_id: session.id.into_string(),
            user_id: session.user_id.into_string(),
            device_id: session.device_id.map(|device| device.into_string()),
            last_active_time: session.last_active_time,
            last_heartbeat_time: session.last_heartbeat_time,
            closed: session.closed,
        })
        .collect();

    Json(ApiResponse {
        ok: true,
        data: sessions,
    })
}

// Send a JSON event to one user 向指定用户发送 JSON 事件
async fn send_to_user(
    State(state): State<AppState>,
    Json(request): Json<SendToUserRequest>,
) -> Result<Json<ApiResponse<DeliveryResponse>>, (StatusCode, String)> {
    let frame = WsMessage::event(
        "server_message",
        json!({
            "message": request.message,
            "target": request.user,
        }),
    )
    .map_err(api_error)?
    .to_text_frame()
    .map_err(api_error)?;

    let delivered = state
        .wing
        .send_to_user(request.user, frame)
        .await
        .map_err(api_error)?;

    Ok(Json(ApiResponse {
        ok: true,
        data: DeliveryResponse { delivered },
    }))
}

// Broadcast a text frame to all local sessions 向所有本地会话广播文本帧
async fn broadcast(
    State(state): State<AppState>,
    Json(request): Json<BroadcastRequest>,
) -> Result<Json<ApiResponse<DeliveryResponse>>, (StatusCode, String)> {
    let delivered = state
        .wing
        .broadcast_local(OutboundFrame::text(request.message))
        .map_err(api_error)?;

    Ok(Json(ApiResponse {
        ok: true,
        data: DeliveryResponse { delivered },
    }))
}

// Upgrade an HTTP request into a RustWing-managed WebSocket 将 HTTP 请求升级为 RustWing 管理的 WebSocket
async fn ws_handler(
    ws: WebSocketUpgrade,
    Query(query): Query<WsQuery>,
    State(state): State<AppState>,
) -> Response {
    let mut identity = Identity::new(query.user);
    if let Some(device) = query.device {
        identity = identity.with_device(device);
    }

    upgrade_with_handler(ws, state.wing, identity, EchoMessageHandler)
}

// Demo handler for non-heartbeat WebSocket text messages 非心跳 WebSocket 文本消息示例处理器
#[derive(Clone, Copy)]
struct EchoMessageHandler;

#[async_trait]
impl AxumMessageHandler for EchoMessageHandler {
    // Echo client text back through RustWing's outbound queue 通过 RustWing 出站队列回显客户端文本
    async fn handle_text(&self, context: AxumMessageContext, text: String) -> WingResult<()> {
        let message = WsMessage::event(
            "echo",
            json!({
                "session_id": context.session.id().as_str(),
                "user_id": context.session.user_id().as_str(),
                "text": text,
            }),
        )?;
        context.session.enqueue(message.to_text_frame()?)?;
        Ok(())
    }
}

// Convert framework errors to HTTP errors 将框架错误转换为 HTTP 错误
fn api_error(error: impl std::fmt::Display) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
}

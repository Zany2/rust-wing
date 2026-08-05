use std::net::SocketAddr;

use async_trait::async_trait;
use axum::{
    Json, Router,
    extract::{State, ws::WebSocketUpgrade},
    http::{HeaderMap, Uri},
    response::Response,
    routing::get,
};
use rust_wing_axum::{
    ApiKeySendApiGuard, AxumAuthContext, AxumAuthError, AxumAuthenticator, AxumMessageContext,
    AxumMessageHandler, send_api_router, send_api_router_unprotected, upgrade_with_auth,
};
use rust_wing_core::{
    ConnectionPolicy, Identity, Result as WingResult, RustWing, RustWingConfig, UserId, WsMessage,
};
use serde::Serialize;
use serde_json::json;

#[cfg(feature = "redis")]
use std::sync::Arc;

#[cfg(feature = "redis")]
use rust_wing_adapter::{RedisRustWing, redis_rust_wing_from_config};

// Shared application state 共享应用状态
#[derive(Clone)]
struct AppState {
    // Runtime keeps the core manager and optional Redis worker 运行时持有核心管理器和可选 Redis 工作者
    runtime: RuntimeState,
}

// Runtime variants used by the example 示例使用的运行时变体
#[derive(Clone)]
enum RuntimeState {
    // Local in-memory runtime 本地内存运行时
    Local(RustWing),
    // Redis-backed managed runtime Redis 托管运行时
    #[cfg(feature = "redis")]
    Redis(Arc<RedisRustWing>),
}

impl RuntimeState {
    // Borrow the active RustWing manager 借用当前激活的 RustWing 管理器
    fn wing(&self) -> RustWing {
        match self {
            Self::Local(wing) => wing.clone(),
            #[cfg(feature = "redis")]
            Self::Redis(runtime) => runtime.wing_clone(),
        }
    }
}

impl AppState {
    // Borrow the active RustWing manager 借用当前激活的 RustWing 管理器
    fn wing(&self) -> RustWing {
        self.runtime.wing()
    }
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
    // Unique local user count 当前本地去重用户数
    users: usize,
    // Cluster nodes visible in the lightweight snapshot 轻量快照中可见的集群节点数
    cluster_nodes: usize,
    // Cluster routes visible in the lightweight snapshot 轻量快照中可见的集群路由数
    cluster_routes: usize,
}

// Session response shown by the example API 示例接口展示的会话信息
#[derive(Debug, Serialize)]
struct SessionResponse {
    // Session id 会话标识
    session_id: String,
    // Connection system id 连接体系标识
    connection_type: String,
    // User id 用户标识
    user_id: String,
    // Optional client id 可选客户端标识
    client_id: Option<String>,
    // Last activity timestamp in milliseconds 最近活跃时间戳，单位毫秒
    last_active_time: i64,
    // Last heartbeat timestamp in milliseconds 最近心跳时间戳，单位毫秒
    last_heartbeat_time: i64,
    // Whether the session is already closed 会话是否已关闭
    closed: bool,
}

#[tokio::main]
async fn main() {
    let state = build_state().await.expect("build rust-wing runtime");
    let app = build_router(state);
    let address = SocketAddr::from(([127, 0, 0, 1], 3000));

    println!("rust-wing example listening on http://{address}");
    let listener = tokio::net::TcpListener::bind(address)
        .await
        .expect("bind example listener");
    axum::serve(listener, app)
        .await
        .expect("run example server");
}

async fn build_state() -> rust_wing_core::Result<AppState> {
    let config =
        RustWingConfig::default().with_default_connection_policy(ConnectionPolicy::MultiSession);

    #[cfg(feature = "redis")]
    {
        if let Ok(url) = std::env::var("RUST_WING_REDIS_URL") {
            let runtime = redis_rust_wing_from_config(config.clone(), url).await?;
            return Ok(AppState {
                runtime: RuntimeState::Redis(Arc::new(runtime)),
            });
        }
    }

    Ok(AppState {
        runtime: RuntimeState::Local(RustWing::from_config(config).await?),
    })
}

// Build the example HTTP router 构建示例 HTTP 路由
fn build_router(state: AppState) -> Router {
    // This example mounts the unprotected send API for local learning only 示例仅为本地学习挂载无保护发送接口
    let send_api = match std::env::var("RUST_WING_SEND_API_KEY") {
        Ok(api_key) if !api_key.trim().is_empty() => {
            send_api_router(state.wing(), ApiKeySendApiGuard::new(api_key))
        }
        _ => send_api_router_unprotected(state.wing()),
    };

    Router::new()
        .route("/", get(index))
        .route("/health", get(health))
        .route("/sessions/{user}", get(list_user_sessions))
        .route("/ws", get(ws_handler))
        .with_state(state)
        .merge(send_api)
}

// Return a small route guide 返回简短路由说明
async fn index() -> Json<ApiResponse<serde_json::Value>> {
    Json(ApiResponse {
        ok: true,
        data: json!({
            "name": "rust-wing-example",
            "send_api_auth": "set RUST_WING_SEND_API_KEY and pass x-api-key in production",
            "routes": {
                "GET /health": "查看连接数量",
                "GET /stats": "查看运行统计",
                "GET /cluster/nodes": "查看集群节点",
                "GET /cluster/routes": "查看全部集群路由",
                "GET /systems/{connection_type}/routes": "查看指定连接体系的集群路由",
                "GET /sessions/{user}": "查看用户会话",
                "POST /send/user": { "user_id": "alice", "message": "hello alice" },
                "POST /broadcast/all": { "message": "hello everyone" },
                "GET /ws?user=alice&client=browser": "建立 WebSocket 连接"
            }
        }),
    })
}

// Report current service status 返回当前服务状态
async fn health(State(state): State<AppState>) -> Json<ApiResponse<HealthResponse>> {
    let stats = state.wing().stats_snapshot().ok();

    Json(ApiResponse {
        ok: true,
        data: HealthResponse {
            service: "rust-wing-example",
            connections: stats
                .as_ref()
                .map(|stats| stats.local_connections)
                .unwrap_or_default(),
            users: stats
                .as_ref()
                .map(|stats| stats.local_users)
                .unwrap_or_default(),
            cluster_nodes: stats
                .as_ref()
                .map(|stats| stats.cluster_nodes)
                .unwrap_or_default(),
            cluster_routes: stats.map(|stats| stats.cluster_routes).unwrap_or_default(),
        },
    })
}

// List active sessions for one user 列出某个用户的活跃会话
async fn list_user_sessions(
    State(state): State<AppState>,
    axum::extract::Path(user): axum::extract::Path<String>,
) -> Json<ApiResponse<Vec<SessionResponse>>> {
    let sessions = state
        .wing()
        .list_user_sessions(&UserId::from(user.as_str()))
        .unwrap_or_default()
        .into_iter()
        .map(|session| SessionResponse {
            session_id: session.id.into_string(),
            connection_type: session.connection_type.into_string(),
            user_id: session.user_id.into_string(),
            client_id: session
                .client_id
                .map(|client: rust_wing_core::ClientId| client.into_string()),
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

// Upgrade an HTTP request into a RustWing-managed WebSocket 将 HTTP 请求升级为 RustWing 管理的 WebSocket
async fn ws_handler(
    ws: WebSocketUpgrade,
    headers: HeaderMap,
    uri: Uri,
    State(state): State<AppState>,
) -> Response {
    upgrade_with_auth(
        ws,
        state.wing(),
        AxumAuthContext { headers, uri },
        DemoAuthenticator,
        EchoMessageHandler,
    )
    .await
}

// Demo authenticator that resolves user identity from request data 示例认证器，从请求数据解析用户身份
struct DemoAuthenticator;

#[async_trait]
impl AxumAuthenticator for DemoAuthenticator {
    // Resolve the user identity from a demo header or query parameter 从示例请求头或查询参数解析用户身份
    async fn authenticate(
        &self,
        context: AxumAuthContext,
    ) -> std::result::Result<Identity, AxumAuthError> {
        let query = context.query().unwrap_or_default();
        let user_id = context
            .headers
            .get("x-user-id")
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned)
            .or_else(|| extract_query_value(query, "user"))
            .ok_or_else(|| AxumAuthError::unauthorized("missing x-user-id"))?;

        let mut identity = Identity::default_connection(user_id);
        if let Some(client) = extract_query_value(query, "client") {
            identity = identity.with_client(client);
        }
        Ok(identity)
    }
}

// Extract a single query parameter from a raw query string 从原始查询字符串提取单个参数
fn extract_query_value(query: &str, key: &str) -> Option<String> {
    query.split('&').find_map(|pair| {
        let (name, value) = pair.split_once('=')?;
        (name == key).then(|| value.to_owned())
    })
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

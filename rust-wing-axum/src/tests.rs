use axum::body::{Body, to_bytes};
use axum::http::{Method, Request};
use rust_wing_core::cluster::NoopPublisher;
use rust_wing_core::{
    Cluster, ClusterConfig, ConnectionPolicy, FrameKind, MemoryPresenceStore, RustWingConfig,
};
use serde_json::json;
use tokio::sync::oneshot;
use tower::ServiceExt;

use super::*;

// Text frames preserve their UTF-8 payload 文本帧会保留 UTF-8 负载
#[test]
fn text_frame_converts_to_axum_message() {
    let message = axum_message_from_frame(OutboundFrame::text("hello"));

    assert!(matches!(message, Message::Text(text) if text == "hello"));
}

// Binary frames preserve their raw payload 二进制帧会保留原始负载
#[test]
fn binary_frame_converts_to_axum_message() {
    let message = axum_message_from_frame(OutboundFrame::binary([1, 2, 3]));

    assert!(matches!(message, Message::Binary(bytes) if bytes.as_ref() == [1, 2, 3]));
}

// Ping frames become WebSocket ping control frames Ping 帧会转换成 WebSocket Ping 控制帧
#[test]
fn ping_frame_converts_to_axum_message() {
    let message = axum_message_from_frame(OutboundFrame::ping([1, 2, 3]));

    assert!(matches!(message, Message::Ping(bytes) if bytes.as_ref() == [1, 2, 3]));
}

// Heartbeat timestamp prefers structured data 心跳时间优先使用结构化数据
#[test]
fn heartbeat_time_prefers_data_payload() {
    let message = WsMessage {
        version: 1,
        message_type: MessageType::Heartbeat,
        event: Some(HEARTBEAT_EVENT.into()),
        request_id: None,
        trace_id: None,
        seq: None,
        client_time: Some(1),
        code: None,
        message: None,
        server_time: None,
        data: Some(json!({ "client_time": 42 })),
    };

    assert_eq!(heartbeat_client_time(&message), Some(42));
}

// Heartbeat messages still receive built-in acknowledgements 心跳消息仍会收到内置确认响应
#[tokio::test]
async fn heartbeat_message_returns_ack_frame() {
    let wing = RustWing::new(RustWingConfig::default());
    let mut accepted = wing.accept_user("alice").await.unwrap();
    let context = AxumMessageContext {
        wing,
        session: accepted.session.clone(),
    };
    let heartbeat = WsMessage {
        version: 1,
        message_type: MessageType::Heartbeat,
        event: Some(HEARTBEAT_EVENT.into()),
        request_id: Some("request-1".into()),
        trace_id: None,
        seq: None,
        client_time: Some(42),
        code: None,
        message: None,
        server_time: None,
        data: None,
    };

    let handled = handle_text_message(
        context,
        &NoopAxumMessageHandler,
        serde_json::to_string(&heartbeat).unwrap(),
    )
    .await
    .unwrap();
    let frame = accepted.outbound.recv().await.unwrap();
    let ack: WsMessage = serde_json::from_slice(&frame.payload).unwrap();

    assert!(handled);
    assert_eq!(frame.kind, FrameKind::Text);
    assert_eq!(ack.message_type, MessageType::HeartbeatAck);
    assert_eq!(ack.event.as_deref(), Some(HEARTBEAT_EVENT));
    assert_eq!(ack.request_id.as_deref(), Some("request-1"));
    assert_eq!(ack.data.unwrap()["client_heartbeat_time"], 42);
}

// Auth context exposes the raw query string 认证上下文会暴露原始查询字符串
#[test]
fn auth_context_exposes_query() {
    let context = AxumAuthContext {
        headers: HeaderMap::new(),
        uri: "/ws?token=abc".parse().unwrap(),
    };

    assert_eq!(context.query(), Some("token=abc"));
}

// Auth errors convert into HTTP responses 认证错误会转换为 HTTP 响应
#[test]
fn auth_error_converts_to_response() {
    let response = AxumAuthError::unauthorized("missing token").into_response();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

// Reader task is cancelled when the writer task exits first 写任务先退出时读任务会被取消
#[tokio::test]
async fn writer_exit_cancels_reader_task() {
    let outbound_task = tokio::spawn(async {});
    let (cancelled_tx, cancelled_rx) = oneshot::channel();
    let inbound_task = tokio::spawn(async move {
        let _guard = CancelNotify(cancelled_tx);
        std::future::pending::<()>().await;
    });

    wait_for_socket_tasks(outbound_task, inbound_task).await;

    assert!(cancelled_rx.await.is_ok());
}

// Writer task is cancelled when the reader task exits first 读任务先退出时写任务会被取消
#[tokio::test]
async fn reader_exit_cancels_writer_task() {
    let (cancelled_tx, cancelled_rx) = oneshot::channel();
    let outbound_task = tokio::spawn(async move {
        let _guard = CancelNotify(cancelled_tx);
        std::future::pending::<()>().await;
    });
    let inbound_task = tokio::spawn(async {});

    wait_for_socket_tasks(outbound_task, inbound_task).await;

    assert!(cancelled_rx.await.is_ok());
}

// Send API delivers to default-system users 发送接口会投递给默认连接体系用户
#[tokio::test]
async fn send_api_delivers_to_default_user() {
    let wing = RustWing::new(
        RustWingConfig::default().with_default_connection_policy(ConnectionPolicy::MultiSession),
    );
    let mut accepted = wing.accept_user("alice").await.unwrap();
    let app = send_api_router_unprotected(wing);
    let request = json!({
        "user_id": "alice",
        "message": "hello"
    });

    let response = app
        .oneshot(json_request("/send/user", request))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let payload = response_json(response).await;
    assert_eq!(payload["local_sessions"], 1);
    assert_eq!(payload["remote_failures"], 0);
    let frame = accepted.outbound.recv().await.unwrap();
    assert_eq!(frame.payload, b"hello");
}

// Send API guard can reject external callers 发送接口保护器可以拒绝外部调用方
#[tokio::test]
async fn send_api_guard_can_reject_request() {
    let wing = RustWing::new(RustWingConfig::default());
    let app = send_api_router(wing, RejectGuard);

    let response = app
        .clone()
        .oneshot(json_request(
            "/send/user",
            json!({ "user_id": "alice", "message": "hello" }),
        ))
        .await
        .unwrap();
    let disconnect_response = app
        .oneshot(json_request(
            "/disconnect/user",
            json!({ "user_id": "alice", "reason": "test" }),
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(disconnect_response.status(), StatusCode::UNAUTHORIZED);
}

// System path overrides the request connection type 体系路径会覆盖请求体中的连接体系
#[tokio::test]
async fn api_key_send_api_guard_checks_header() {
    let wing = RustWing::new(RustWingConfig::default());
    let app = send_api_router(wing, ApiKeySendApiGuard::new("secret"));
    let request = json_request_with_api_key(
        "/send/user",
        json!({ "user_id": "alice", "message": "hello" }),
        "secret",
    );

    let ok = app.clone().oneshot(request).await.unwrap();
    let missing = app
        .clone()
        .oneshot(json_request(
            "/send/user",
            json!({ "user_id": "alice", "message": "hello" }),
        ))
        .await
        .unwrap();
    let wrong = app
        .oneshot(json_request_with_api_key(
            "/send/user",
            json!({ "user_id": "alice", "message": "hello" }),
            "wrong",
        ))
        .await
        .unwrap();

    assert_eq!(ok.status(), StatusCode::OK);
    assert_eq!(missing.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(wrong.status(), StatusCode::FORBIDDEN);
}

// System path overrides the request connection type 体系路径会覆盖请求体中的连接体系
#[tokio::test]
async fn send_api_system_path_targets_connection_type() {
    let wing = RustWing::new(
        RustWingConfig::default().with_connection_policy("game", ConnectionPolicy::MultiSession),
    );
    let mut accepted = wing
        .accept(Identity::new("game", "alice").with_client("browser"))
        .await
        .unwrap();
    let app = send_api_router_unprotected(wing);

    let response = app
        .oneshot(json_request(
            "/systems/game/send/user",
            json!({
                "connection_type": "admin",
                "user_id": "alice",
                "message": "game notice"
            }),
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let frame = accepted.outbound.recv().await.unwrap();
    assert_eq!(frame.payload, b"game notice");
}

// Disconnect API removes default-system user sessions 断开接口会移除默认连接体系用户会话
#[tokio::test]
async fn disconnect_api_removes_default_user() {
    let wing = RustWing::new(
        RustWingConfig::default().with_default_connection_policy(ConnectionPolicy::MultiSession),
    );
    let mut first = wing.accept_user("alice").await.unwrap();
    let mut second = wing.accept_user("alice").await.unwrap();
    let app = send_api_router_unprotected(wing);

    let response = app
        .oneshot(json_request(
            "/disconnect/user",
            json!({ "user_id": "alice", "reason": "logout" }),
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let payload = response_json(response).await;
    assert_eq!(payload["local_sessions"], 2);
    assert_eq!(payload["remote_failures"], 0);
    assert_eq!(first.outbound.recv().await.unwrap().kind, FrameKind::Close);
    assert_eq!(second.outbound.recv().await.unwrap().kind, FrameKind::Close);
}

// Disconnect system path overrides the request connection type 断开体系路径会覆盖请求体中的连接体系
#[tokio::test]
async fn disconnect_api_system_path_targets_connection_type() {
    let wing = RustWing::new(
        RustWingConfig::default().with_connection_policy("game", ConnectionPolicy::MultiSession),
    );
    let mut accepted = wing
        .accept(Identity::new("game", "alice").with_client("browser"))
        .await
        .unwrap();
    let app = send_api_router_unprotected(wing);

    let response = app
        .oneshot(json_request(
            "/systems/game/disconnect/user",
            json!({
                "connection_type": "admin",
                "user_id": "alice",
                "reason": "kick"
            }),
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let payload = response_json(response).await;
    assert_eq!(payload["local_sessions"], 1);
    assert_eq!(
        accepted.outbound.recv().await.unwrap().kind,
        FrameKind::Close
    );
}

// Stats API reports local runtime counters 统计接口会返回本地运行计数
#[tokio::test]
async fn stats_api_reports_runtime_snapshot() {
    let wing = RustWing::new(
        RustWingConfig::default().with_default_connection_policy(ConnectionPolicy::MultiSession),
    );
    let node_id = wing.config().node_id.as_str().to_owned();
    let _first = wing.accept_user("alice").await.unwrap();
    let _second = wing.accept_user("alice").await.unwrap();
    wing.send_to_user("alice", OutboundFrame::text("stats"))
        .await
        .unwrap();
    let app = send_api_router_unprotected(wing);

    let response = app.oneshot(get_request("/stats")).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let payload = response_json(response).await;
    assert_eq!(payload["node_id"], node_id);
    assert_eq!(payload["local_connections"], 2);
    assert_eq!(payload["local_users"], 1);
    assert_eq!(payload["outbound_frames_enqueued_total"], 2);
}

// Cluster status API reports routes 集群状态接口会返回路由
#[tokio::test]
async fn cluster_status_api_reports_routes() {
    let cluster = Cluster::new(MemoryPresenceStore::new(), NoopPublisher);
    let wing = RustWing::with_cluster_checked(
        RustWingConfig {
            cluster: ClusterConfig {
                enabled: true,
                ..ClusterConfig::default()
            },
            ..RustWingConfig::default()
        },
        Some(cluster),
    )
    .await
    .unwrap();
    let accepted = wing
        .accept(Identity::new("default", "alice").with_client("browser"))
        .await
        .unwrap();
    let app = send_api_router_unprotected(wing);

    let nodes = app
        .clone()
        .oneshot(get_request("/cluster/nodes"))
        .await
        .unwrap();
    let routes = app
        .clone()
        .oneshot(get_request("/cluster/routes"))
        .await
        .unwrap();
    let system_routes = app
        .oneshot(get_request("/systems/default/routes"))
        .await
        .unwrap();

    assert_eq!(nodes.status(), StatusCode::OK);
    assert_eq!(routes.status(), StatusCode::OK);
    assert_eq!(system_routes.status(), StatusCode::OK);
    let route_payload = response_json(routes).await;
    assert_eq!(route_payload.as_array().unwrap().len(), 1);
    assert_eq!(
        route_payload[0]["session_id"],
        accepted.session.id().as_str()
    );
    let system_payload = response_json(system_routes).await;
    assert_eq!(system_payload, route_payload);
}

// Cluster status API uses the same guard as send APIs 集群状态接口复用发送接口保护器
#[tokio::test]
async fn cluster_status_api_guard_can_reject_request() {
    let wing = RustWing::new(RustWingConfig::default());
    let app = send_api_router(wing, RejectGuard);

    let response = app.oneshot(get_request("/cluster/routes")).await.unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

// Sends a signal when dropped by task cancellation 任务取消触发丢弃时发送信号
struct CancelNotify(oneshot::Sender<()>);

impl Drop for CancelNotify {
    fn drop(&mut self) {
        let (replacement, _rx) = oneshot::channel();
        let sender = std::mem::replace(&mut self.0, replacement);
        let _ = sender.send(());
    }
}

// Guard that rejects every request 拒绝全部请求的保护器
struct RejectGuard;

#[async_trait]
impl AxumSendApiGuard for RejectGuard {
    // Reject the request with unauthorized 拒绝请求并返回未授权
    async fn authorize(&self, _headers: &HeaderMap) -> std::result::Result<(), AxumAuthError> {
        Err(AxumAuthError::unauthorized("no"))
    }
}

fn json_request(path: &str, body: serde_json::Value) -> Request<Body> {
    Request::builder()
        .method(Method::POST)
        .uri(path)
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap()
}

fn json_request_with_api_key(path: &str, body: serde_json::Value, api_key: &str) -> Request<Body> {
    Request::builder()
        .method(Method::POST)
        .uri(path)
        .header("content-type", "application/json")
        .header("x-api-key", api_key)
        .body(Body::from(body.to_string()))
        .unwrap()
}

fn get_request(path: &str) -> Request<Body> {
    Request::builder()
        .method(Method::GET)
        .uri(path)
        .body(Body::empty())
        .unwrap()
}

async fn response_json(response: Response) -> serde_json::Value {
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

mod handlers;
mod types;

pub use types::{
    AckApiResponse, AckSessionApiResponse, AxumSendApiState, BroadcastRequest, SendApiResponse,
    SendToClientRequest, SendToSessionRequest, SendToUserRequest, WaitForAckRequest,
};

use axum::Router;
use axum::routing::{get, post};
use handlers::{
    broadcast, broadcast_all, disconnect_client, disconnect_session, disconnect_system_client,
    disconnect_system_user, disconnect_user, get_ack_snapshot, get_cluster_nodes,
    get_cluster_routes, get_stats, get_system_cluster_routes, send_to_client, send_to_session,
    send_to_system_broadcast, send_to_system_user, send_to_user, wait_for_ack,
};
// Build a router for external message sending 构建外部消息发送路由器
pub fn send_api_router(
    wing: rust_wing_core::RustWing,
    guard: impl crate::auth::AxumSendApiGuard,
) -> Router {
    send_api_router_with_state(AxumSendApiState {
        wing,
        guard: std::sync::Arc::new(guard),
    })
}

// Build an unprotected router for trusted local development 本地开发使用的无保护路由器
pub fn send_api_router_unprotected(wing: rust_wing_core::RustWing) -> Router {
    send_api_router(wing, crate::auth::AllowAllSendApiGuard)
}

// Build a router from prepared send API state 通过已准备状态构建发送接口路由器
pub fn send_api_router_with_state(state: AxumSendApiState) -> Router {
    Router::new()
        .route("/send/user", post(send_to_user))
        .route("/send/client", post(send_to_client))
        .route("/send/session", post(send_to_session))
        .route("/broadcast", post(broadcast))
        .route("/broadcast/all", post(broadcast_all))
        .route("/disconnect/user", post(disconnect_user))
        .route("/disconnect/client", post(disconnect_client))
        .route("/disconnect/session", post(disconnect_session))
        .route("/ack/{message_id}", get(get_ack_snapshot))
        .route("/ack/wait", post(wait_for_ack))
        .route("/stats", get(get_stats))
        .route("/cluster/nodes", get(get_cluster_nodes))
        .route("/cluster/routes", get(get_cluster_routes))
        .route(
            "/systems/{connection_type}/send/user",
            post(send_to_system_user),
        )
        .route(
            "/systems/{connection_type}/broadcast",
            post(send_to_system_broadcast),
        )
        .route(
            "/systems/{connection_type}/disconnect/user",
            post(disconnect_system_user),
        )
        .route(
            "/systems/{connection_type}/disconnect/client",
            post(disconnect_system_client),
        )
        .route(
            "/systems/{connection_type}/routes",
            get(get_system_cluster_routes),
        )
        .with_state(state)
}

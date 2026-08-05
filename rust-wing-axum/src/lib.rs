mod auth;
mod send_api;
mod websocket;

pub use auth::{
    AllowAllSendApiGuard, ApiKeySendApiGuard, AxumAuthContext, AxumAuthError, AxumAuthenticator,
    AxumSendApiGuard,
};
pub use send_api::{
    AxumSendApiState, BroadcastRequest, SendApiResponse, SendToClientRequest, SendToSessionRequest,
    SendToUserRequest, send_api_router, send_api_router_unprotected, send_api_router_with_state,
};
pub use websocket::{
    AxumMessageContext, AxumMessageHandler, NoopAxumMessageHandler, upgrade, upgrade_with_auth,
    upgrade_with_auth_default_handler, upgrade_with_handler,
};

#[cfg(test)]
use async_trait::async_trait;
#[cfg(test)]
use axum::extract::ws::Message;
#[cfg(test)]
use axum::http::{HeaderMap, StatusCode};
#[cfg(test)]
use axum::response::{IntoResponse, Response};
#[cfg(test)]
use rust_wing_core::{HEARTBEAT_EVENT, Identity, MessageType, OutboundFrame, RustWing, WsMessage};
#[cfg(test)]
use websocket::{
    axum_message_from_frame, handle_text_message, heartbeat_client_time, wait_for_socket_tasks,
};

#[cfg(test)]
mod tests;

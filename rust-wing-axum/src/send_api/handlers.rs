use axum::Json;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use rust_wing_core::{ConnectionType, OutboundFrame, SessionId, UserId};

use crate::auth::AxumAuthError;

use super::types::{
    AxumSendApiState, BroadcastRequest, DisconnectClientRequest, DisconnectSessionRequest,
    DisconnectUserRequest, SendApiResponse, SendToClientRequest, SendToSessionRequest,
    SendToUserRequest,
};

pub(super) async fn send_to_user(
    State(state): State<AxumSendApiState>,
    headers: HeaderMap,
    Json(request): Json<SendToUserRequest>,
) -> std::result::Result<Json<SendApiResponse>, (StatusCode, String)> {
    authorize_send_api(&state, &headers).await?;
    let connection_type = request.connection_type.map(ConnectionType::from);
    let frame = OutboundFrame::text(request.message);
    let user_id = UserId::from(request.user_id.as_str());
    let report = match connection_type {
        Some(connection_type) => {
            state
                .wing
                .send_to_user_in(connection_type, user_id, frame)
                .await
        }
        None => state.wing.send_to_user(user_id, frame).await,
    }
    .map_err(send_api_error)?;
    Ok(Json(send_response(report)))
}

pub(super) async fn send_to_client(
    State(state): State<AxumSendApiState>,
    headers: HeaderMap,
    Json(request): Json<SendToClientRequest>,
) -> std::result::Result<Json<SendApiResponse>, (StatusCode, String)> {
    authorize_send_api(&state, &headers).await?;
    let connection_type = request.connection_type.map(ConnectionType::from);
    let frame = OutboundFrame::text(request.message);
    let user_id = UserId::from(request.user_id.as_str());
    let report = match connection_type {
        Some(connection_type) => {
            state
                .wing
                .send_to_client_in(connection_type, user_id, request.client_id, frame)
                .await
        }
        None => {
            state
                .wing
                .send_to_client(user_id, request.client_id, frame)
                .await
        }
    }
    .map_err(send_api_error)?;
    Ok(Json(send_response(report)))
}

pub(super) async fn send_to_session(
    State(state): State<AxumSendApiState>,
    headers: HeaderMap,
    Json(request): Json<SendToSessionRequest>,
) -> std::result::Result<Json<SendApiResponse>, (StatusCode, String)> {
    authorize_send_api(&state, &headers).await?;
    let frame = OutboundFrame::text(request.message);
    let report = state
        .wing
        .send_to_session(&SessionId::from(request.session_id.as_str()), frame)
        .await
        .map_err(send_api_error)?;
    Ok(Json(send_response(report)))
}

pub(super) async fn broadcast(
    State(state): State<AxumSendApiState>,
    headers: HeaderMap,
    Json(request): Json<BroadcastRequest>,
) -> std::result::Result<Json<SendApiResponse>, (StatusCode, String)> {
    authorize_send_api(&state, &headers).await?;
    let frame = OutboundFrame::text(request.message);
    let report = match request.connection_type {
        Some(connection_type) => {
            state
                .wing
                .broadcast_in(ConnectionType::from(connection_type), frame)
                .await
        }
        None => state.wing.broadcast(frame).await,
    }
    .map_err(send_api_error)?;
    Ok(Json(send_response(report)))
}

pub(super) async fn broadcast_all(
    State(state): State<AxumSendApiState>,
    headers: HeaderMap,
    Json(request): Json<BroadcastRequest>,
) -> std::result::Result<Json<SendApiResponse>, (StatusCode, String)> {
    authorize_send_api(&state, &headers).await?;
    let frame = OutboundFrame::text(request.message);
    let report = state
        .wing
        .broadcast_all(frame)
        .await
        .map_err(send_api_error)?;
    Ok(Json(send_response(report)))
}

pub(super) async fn disconnect_user(
    State(state): State<AxumSendApiState>,
    headers: HeaderMap,
    Json(request): Json<DisconnectUserRequest>,
) -> std::result::Result<Json<SendApiResponse>, (StatusCode, String)> {
    authorize_send_api(&state, &headers).await?;
    let connection_type = request.connection_type.map(ConnectionType::from);
    let report = match connection_type {
        Some(connection_type) => {
            state
                .wing
                .disconnect_user_in(
                    connection_type,
                    UserId::from(request.user_id.as_str()),
                    disconnect_reason(request.reason),
                )
                .await
        }
        None => {
            state
                .wing
                .disconnect_user(
                    UserId::from(request.user_id.as_str()),
                    disconnect_reason(request.reason),
                )
                .await
        }
    }
    .map_err(send_api_error)?;
    Ok(Json(send_response(report)))
}

pub(super) async fn disconnect_client(
    State(state): State<AxumSendApiState>,
    headers: HeaderMap,
    Json(request): Json<DisconnectClientRequest>,
) -> std::result::Result<Json<SendApiResponse>, (StatusCode, String)> {
    authorize_send_api(&state, &headers).await?;
    let connection_type = request.connection_type.map(ConnectionType::from);
    let report = match connection_type {
        Some(connection_type) => {
            state
                .wing
                .disconnect_client_in(
                    connection_type,
                    UserId::from(request.user_id.as_str()),
                    request.client_id,
                    disconnect_reason(request.reason),
                )
                .await
        }
        None => {
            state
                .wing
                .disconnect_client(
                    UserId::from(request.user_id.as_str()),
                    request.client_id,
                    disconnect_reason(request.reason),
                )
                .await
        }
    }
    .map_err(send_api_error)?;
    Ok(Json(send_response(report)))
}

pub(super) async fn disconnect_session(
    State(state): State<AxumSendApiState>,
    headers: HeaderMap,
    Json(request): Json<DisconnectSessionRequest>,
) -> std::result::Result<Json<SendApiResponse>, (StatusCode, String)> {
    authorize_send_api(&state, &headers).await?;
    let report = state
        .wing
        .disconnect_session(
            &SessionId::from(request.session_id.as_str()),
            disconnect_reason(request.reason),
        )
        .await
        .map_err(send_api_error)?;
    Ok(Json(send_response(report)))
}

pub(super) async fn get_stats(
    State(state): State<AxumSendApiState>,
    headers: HeaderMap,
) -> std::result::Result<Json<rust_wing_core::StatsSnapshot>, (StatusCode, String)> {
    authorize_send_api(&state, &headers).await?;
    state
        .wing
        .detailed_stats_snapshot()
        .await
        .map(Json)
        .map_err(send_api_error)
}

pub(super) async fn get_cluster_nodes(
    State(state): State<AxumSendApiState>,
    headers: HeaderMap,
) -> std::result::Result<Json<Vec<rust_wing_core::NodeId>>, (StatusCode, String)> {
    authorize_send_api(&state, &headers).await?;
    state
        .wing
        .list_cluster_nodes()
        .await
        .map(Json)
        .map_err(send_api_error)
}

pub(super) async fn get_cluster_routes(
    State(state): State<AxumSendApiState>,
    headers: HeaderMap,
) -> std::result::Result<Json<Vec<rust_wing_core::Route>>, (StatusCode, String)> {
    authorize_send_api(&state, &headers).await?;
    state
        .wing
        .list_all_cluster_routes()
        .await
        .map(Json)
        .map_err(send_api_error)
}

pub(super) async fn get_system_cluster_routes(
    State(state): State<AxumSendApiState>,
    headers: HeaderMap,
    axum::extract::Path(connection_type): axum::extract::Path<String>,
) -> std::result::Result<Json<Vec<rust_wing_core::Route>>, (StatusCode, String)> {
    authorize_send_api(&state, &headers).await?;
    state
        .wing
        .list_cluster_routes(&ConnectionType::from(connection_type))
        .await
        .map(Json)
        .map_err(send_api_error)
}

pub(super) async fn send_to_system_user(
    State(state): State<AxumSendApiState>,
    headers: HeaderMap,
    axum::extract::Path(connection_type): axum::extract::Path<String>,
    Json(mut request): Json<SendToUserRequest>,
) -> std::result::Result<Json<SendApiResponse>, (StatusCode, String)> {
    request.connection_type = Some(connection_type);
    send_to_user(State(state), headers, Json(request)).await
}

pub(super) async fn send_to_system_broadcast(
    State(state): State<AxumSendApiState>,
    headers: HeaderMap,
    axum::extract::Path(connection_type): axum::extract::Path<String>,
    Json(mut request): Json<BroadcastRequest>,
) -> std::result::Result<Json<SendApiResponse>, (StatusCode, String)> {
    authorize_send_api(&state, &headers).await?;
    request.connection_type = Some(connection_type);
    let connection_type = ConnectionType::from(request.connection_type.unwrap());
    let frame = OutboundFrame::text(request.message);
    let report = state
        .wing
        .broadcast_in(connection_type, frame)
        .await
        .map_err(send_api_error)?;
    Ok(Json(send_response(report)))
}

pub(super) async fn disconnect_system_user(
    State(state): State<AxumSendApiState>,
    headers: HeaderMap,
    axum::extract::Path(connection_type): axum::extract::Path<String>,
    Json(mut request): Json<DisconnectUserRequest>,
) -> std::result::Result<Json<SendApiResponse>, (StatusCode, String)> {
    request.connection_type = Some(connection_type);
    disconnect_user(State(state), headers, Json(request)).await
}

pub(super) async fn disconnect_system_client(
    State(state): State<AxumSendApiState>,
    headers: HeaderMap,
    axum::extract::Path(connection_type): axum::extract::Path<String>,
    Json(mut request): Json<DisconnectClientRequest>,
) -> std::result::Result<Json<SendApiResponse>, (StatusCode, String)> {
    request.connection_type = Some(connection_type);
    disconnect_client(State(state), headers, Json(request)).await
}

async fn authorize_send_api(
    state: &AxumSendApiState,
    headers: &HeaderMap,
) -> std::result::Result<(), (StatusCode, String)> {
    state
        .guard
        .authorize(headers)
        .await
        .map_err(auth_error_response)?;
    Ok(())
}

fn disconnect_reason(reason: Option<String>) -> String {
    reason.unwrap_or_else(|| "disconnected".into())
}

fn send_response(report: rust_wing_core::DeliveryReport) -> SendApiResponse {
    SendApiResponse {
        delivered: report.delivered(),
        local_sessions: report.local_sessions,
        remote_nodes: report.remote_nodes,
        remote_failures: report.remote_failures,
    }
}

fn auth_error_response(error: AxumAuthError) -> (StatusCode, String) {
    (error.status, error.message)
}

fn send_api_error(error: impl std::fmt::Display) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
}

use std::time::Duration;

use axum::Json;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use rust_wing_core::{
    AckStage, ConnectionType, MessageId, OutboundFrame, RustWing, SessionId, UserId,
};

use crate::auth::AxumAuthError;

use super::types::{
    AckApiResponse, AckSessionApiResponse, AxumSendApiState, BroadcastRequest,
    DisconnectClientRequest, DisconnectSessionRequest, DisconnectUserRequest, SendApiResponse,
    SendToClientRequest, SendToSessionRequest, SendToUserRequest, WaitForAckRequest,
};

const DEFAULT_ACK_WAIT_TIMEOUT_MS: u64 = 5_000;
const MAX_ACK_WAIT_TIMEOUT_MS: u64 = 30_000;

pub(super) async fn send_to_user(
    State(state): State<AxumSendApiState>,
    headers: HeaderMap,
    Json(request): Json<SendToUserRequest>,
) -> std::result::Result<Json<SendApiResponse>, (StatusCode, String)> {
    authorize_send_api(&state, &headers).await?;
    let connection_type = request.connection_type.map(ConnectionType::from);
    let (frame, message_id) = frame_from_send_request(
        &state.wing,
        request.message,
        request.require_ack,
        request.message_id.as_deref(),
    );
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
    Ok(Json(send_response(report, message_id)))
}

pub(super) async fn send_to_client(
    State(state): State<AxumSendApiState>,
    headers: HeaderMap,
    Json(request): Json<SendToClientRequest>,
) -> std::result::Result<Json<SendApiResponse>, (StatusCode, String)> {
    authorize_send_api(&state, &headers).await?;
    let connection_type = request.connection_type.map(ConnectionType::from);
    let (frame, message_id) = frame_from_send_request(
        &state.wing,
        request.message,
        request.require_ack,
        request.message_id.as_deref(),
    );
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
    Ok(Json(send_response(report, message_id)))
}

pub(super) async fn send_to_session(
    State(state): State<AxumSendApiState>,
    headers: HeaderMap,
    Json(request): Json<SendToSessionRequest>,
) -> std::result::Result<Json<SendApiResponse>, (StatusCode, String)> {
    authorize_send_api(&state, &headers).await?;
    let (frame, message_id) = frame_from_send_request(
        &state.wing,
        request.message,
        request.require_ack,
        request.message_id.as_deref(),
    );
    let report = state
        .wing
        .send_to_session(&SessionId::from(request.session_id.as_str()), frame)
        .await
        .map_err(send_api_error)?;
    Ok(Json(send_response(report, message_id)))
}

pub(super) async fn broadcast(
    State(state): State<AxumSendApiState>,
    headers: HeaderMap,
    Json(request): Json<BroadcastRequest>,
) -> std::result::Result<Json<SendApiResponse>, (StatusCode, String)> {
    authorize_send_api(&state, &headers).await?;
    let (frame, message_id) = frame_from_send_request(
        &state.wing,
        request.message,
        request.require_ack,
        request.message_id.as_deref(),
    );
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
    Ok(Json(send_response(report, message_id)))
}

pub(super) async fn broadcast_all(
    State(state): State<AxumSendApiState>,
    headers: HeaderMap,
    Json(request): Json<BroadcastRequest>,
) -> std::result::Result<Json<SendApiResponse>, (StatusCode, String)> {
    authorize_send_api(&state, &headers).await?;
    let (frame, message_id) = frame_from_send_request(
        &state.wing,
        request.message,
        request.require_ack,
        request.message_id.as_deref(),
    );
    let report = state
        .wing
        .broadcast_all(frame)
        .await
        .map_err(send_api_error)?;
    Ok(Json(send_response(report, message_id)))
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
    Ok(Json(send_response(report, None)))
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
    Ok(Json(send_response(report, None)))
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
    Ok(Json(send_response(report, None)))
}

pub(super) async fn get_ack_snapshot(
    State(state): State<AxumSendApiState>,
    headers: HeaderMap,
    axum::extract::Path(message_id): axum::extract::Path<String>,
) -> std::result::Result<Json<AckApiResponse>, (StatusCode, String)> {
    authorize_send_api(&state, &headers).await?;
    let message_id = MessageId::from(message_id);
    let snapshot = state
        .wing
        .ack_snapshot(&message_id)
        .map_err(send_api_error)?;
    Ok(Json(ack_response(&message_id, snapshot, None)))
}

pub(super) async fn wait_for_ack(
    State(state): State<AxumSendApiState>,
    headers: HeaderMap,
    Json(request): Json<WaitForAckRequest>,
) -> std::result::Result<Json<AckApiResponse>, (StatusCode, String)> {
    authorize_send_api(&state, &headers).await?;
    let message_id = MessageId::from(request.message_id);
    let stage = request.stage;
    let snapshot = state
        .wing
        .wait_for_ack(&message_id, stage, ack_wait_timeout(request.timeout_ms))
        .await
        .map_err(send_api_error)?;
    Ok(Json(ack_response(&message_id, snapshot, Some(stage))))
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
    let (frame, message_id) = frame_from_send_request(
        &state.wing,
        request.message,
        request.require_ack,
        request.message_id.as_deref(),
    );
    let report = state
        .wing
        .broadcast_in(connection_type, frame)
        .await
        .map_err(send_api_error)?;
    Ok(Json(send_response(report, message_id)))
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

fn frame_from_send_request(
    wing: &RustWing,
    message: String,
    require_ack: bool,
    message_id: Option<&str>,
) -> (OutboundFrame, Option<String>) {
    if !require_ack {
        return (OutboundFrame::text(message), None);
    }
    let message_id = message_id
        .map(MessageId::from)
        .unwrap_or_else(|| wing.next_message_id());
    let message_id_text = message_id.as_str().to_owned();
    (
        OutboundFrame::text(message).require_ack(message_id),
        Some(message_id_text),
    )
}

fn disconnect_reason(reason: Option<String>) -> String {
    reason.unwrap_or_else(|| "disconnected".into())
}

fn send_response(
    report: rust_wing_core::DeliveryReport,
    message_id: Option<String>,
) -> SendApiResponse {
    SendApiResponse {
        delivered: report.delivered(),
        local_sessions: report.local_sessions,
        remote_nodes: report.remote_nodes,
        remote_failures: report.remote_failures,
        message_id,
    }
}

fn ack_response(
    message_id: &MessageId,
    snapshot: Option<rust_wing_core::AckSnapshot>,
    required_stage: Option<AckStage>,
) -> AckApiResponse {
    let found = snapshot.is_some();
    let reached =
        required_stage.and_then(|stage| snapshot.as_ref().map(|snapshot| snapshot.reached(stage)));
    let sessions = snapshot
        .map(|snapshot| {
            snapshot
                .sessions
                .into_iter()
                .map(|session| AckSessionApiResponse {
                    session_id: session.session_id.into_string(),
                    stage: session.stage,
                    client_time: session.client_time,
                    server_time: session.server_time,
                })
                .collect()
        })
        .unwrap_or_default();

    AckApiResponse {
        message_id: message_id.as_str().to_owned(),
        found,
        required_stage,
        reached,
        sessions,
    }
}

fn ack_wait_timeout(timeout_ms: Option<u64>) -> Duration {
    Duration::from_millis(
        timeout_ms
            .unwrap_or(DEFAULT_ACK_WAIT_TIMEOUT_MS)
            .min(MAX_ACK_WAIT_TIMEOUT_MS),
    )
}

fn auth_error_response(error: AxumAuthError) -> (StatusCode, String) {
    (error.status, error.message)
}

fn send_api_error(error: impl std::fmt::Display) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
}

use rust_wing_core::{DeliveryReport, Result, RustWing};

use super::types::{ExternalMessage, ExternalMessageConsumerStats};

// Decode one JSON broker message 解析一条 JSON 外部消息
pub fn external_message_from_json(payload: impl AsRef<[u8]>) -> Result<ExternalMessage> {
    Ok(serde_json::from_slice(payload.as_ref())?)
}

// Decode and deliver one broker payload while updating consumer counters 解析并投递一条消息组件负载，同时更新消费计数
pub async fn process_external_message_payload(
    wing: &RustWing,
    stats: &ExternalMessageConsumerStats,
    payload: impl AsRef<[u8]>,
) -> Option<DeliveryReport> {
    stats.record_received();
    let message = match external_message_from_json(payload) {
        Ok(message) => {
            stats.record_decoded();
            message
        }
        Err(_) => {
            stats.record_decode_failed();
            return None;
        }
    };

    match deliver_external_message(wing, message).await {
        Ok(report) => {
            stats.record_delivered();
            Some(report)
        }
        Err(_) => {
            stats.record_deliver_failed();
            None
        }
    }
}

// Deliver one external broker message through RustWing 通过 RustWing 投递一条外部消息
pub async fn deliver_external_message(
    wing: &RustWing,
    message: ExternalMessage,
) -> Result<DeliveryReport> {
    let target = message.target.clone();
    let frame = message.into_frame();
    match target {
        super::types::ExternalMessageTarget::User {
            connection_type,
            user_id,
        } => match connection_type {
            Some(connection_type) => wing.send_to_user_in(connection_type, user_id, frame).await,
            None => wing.send_to_user(user_id, frame).await,
        },
        super::types::ExternalMessageTarget::Client {
            connection_type,
            user_id,
            client_id,
        } => match connection_type {
            Some(connection_type) => {
                wing.send_to_client_in(connection_type, user_id, client_id, frame)
                    .await
            }
            None => wing.send_to_client(user_id, client_id, frame).await,
        },
        super::types::ExternalMessageTarget::Session { session_id } => {
            wing.send_to_session(&session_id, frame).await
        }
        super::types::ExternalMessageTarget::Broadcast { connection_type } => match connection_type
        {
            Some(connection_type) => wing.broadcast_in(connection_type, frame).await,
            None => wing.broadcast(frame).await,
        },
        super::types::ExternalMessageTarget::BroadcastAll => wing.broadcast_all(frame).await,
        super::types::ExternalMessageTarget::DisconnectUser {
            connection_type,
            user_id,
            reason,
        } => {
            let reason = external_disconnect_reason(reason);
            match connection_type {
                Some(connection_type) => {
                    wing.disconnect_user_in(connection_type, user_id, reason)
                        .await
                }
                None => wing.disconnect_user(user_id, reason).await,
            }
        }
        super::types::ExternalMessageTarget::DisconnectClient {
            connection_type,
            user_id,
            client_id,
            reason,
        } => {
            let reason = external_disconnect_reason(reason);
            match connection_type {
                Some(connection_type) => {
                    wing.disconnect_client_in(connection_type, user_id, client_id, reason)
                        .await
                }
                None => wing.disconnect_client(user_id, client_id, reason).await,
            }
        }
        super::types::ExternalMessageTarget::DisconnectSession { session_id, reason } => {
            wing.disconnect_session(&session_id, external_disconnect_reason(reason))
                .await
        }
    }
}

fn external_disconnect_reason(reason: Option<String>) -> String {
    reason.unwrap_or_else(|| "disconnected by external message".into())
}

use std::sync::Arc;

use async_trait::async_trait;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::response::{IntoResponse, Response};
use futures_util::{SinkExt, StreamExt};
use rust_wing_core::{
    FrameKind, HEARTBEAT_EVENT, HeartbeatData, Identity, MessageType, OutboundFrame, Result,
    RustWing, RustWingError, Session, WsMessage, now_millis,
};

// Context passed to application message handlers 传递给应用消息处理器的上下文
#[derive(Clone)]
pub struct AxumMessageContext {
    // Shared RustWing manager 共享的 RustWing 管理器
    pub wing: RustWing,
    // Current accepted session 当前已接收的会话
    pub session: Session,
}

// Application message callback contract 应用消息回调契约
#[async_trait]
pub trait AxumMessageHandler: Send + Sync + 'static {
    // Handle a non-heartbeat text message 处理非心跳文本消息
    async fn handle_text(&self, _context: AxumMessageContext, _text: String) -> Result<()> {
        Ok(())
    }

    // Handle a non-heartbeat binary message 处理非心跳二进制消息
    async fn handle_binary(&self, _context: AxumMessageContext, _bytes: Vec<u8>) -> Result<()> {
        Ok(())
    }
}

// Default message handler that ignores business payloads 默认忽略业务负载的消息处理器
#[derive(Debug, Clone, Copy, Default)]
pub struct NoopAxumMessageHandler;

#[async_trait]
impl AxumMessageHandler for NoopAxumMessageHandler {}

// Upgrade an authenticated Axum WebSocket with the default handler 使用默认处理器升级已认证的 Axum WebSocket
pub fn upgrade(ws: WebSocketUpgrade, wing: RustWing, identity: Identity) -> Response {
    upgrade_with_handler(ws, wing, identity, NoopAxumMessageHandler)
}

// Upgrade an authenticated Axum WebSocket with an application handler 使用应用处理器升级已认证的 Axum WebSocket
pub fn upgrade_with_handler<H>(
    ws: WebSocketUpgrade,
    wing: RustWing,
    identity: Identity,
    handler: H,
) -> Response
where
    H: AxumMessageHandler,
{
    let handler = Arc::new(handler);
    ws.on_upgrade(move |socket| handle_socket(socket, wing, identity, handler))
        .into_response()
}

// Run one upgraded WebSocket until either side closes 运行一个已升级 WebSocket 直到任意一侧关闭
async fn handle_socket<H>(socket: WebSocket, wing: RustWing, identity: Identity, handler: Arc<H>)
where
    H: AxumMessageHandler,
{
    // Register the authenticated connection before splitting the socket 拆分 socket 前先注册已认证连接
    let Ok(accepted) = wing.accept(identity).await else {
        return;
    };

    let session = accepted.session.clone();
    let mut outbound = accepted.outbound;
    let (mut sender, mut receiver) = socket.split();

    // Forward RustWing outbound frames to the WebSocket writer 将 RustWing 出站帧转发到 WebSocket 写端
    let outbound_task = tokio::spawn(async move {
        while let Some(frame) = outbound.recv().await {
            if sender.send(axum_message_from_frame(frame)).await.is_err() {
                break;
            }
        }
    });

    let inbound_wing = wing.clone();
    let inbound_session = session.clone();
    let inbound_handler = handler.clone();

    // Read inbound WebSocket messages and update RustWing state 读取入站 WebSocket 消息并更新 RustWing 状态
    let inbound_task = tokio::spawn(async move {
        let context = AxumMessageContext {
            wing: inbound_wing,
            session: inbound_session,
        };

        while let Some(message) = receiver.next().await {
            let Ok(message) = message else {
                break;
            };
            match handle_inbound_message(context.clone(), inbound_handler.as_ref(), message).await {
                Ok(true) => {}
                Ok(false) | Err(_) => break,
            }
        }
    });

    // Stop when either task finishes so disconnect cleanup happens promptly 任意任务结束后立即做断开清理
    tokio::select! {
        _ = outbound_task => {}
        _ = inbound_task => {}
    }

    let _ = wing.unregister(&session).await;
}

// Handle one inbound Axum WebSocket message 处理一条入站 Axum WebSocket 消息
async fn handle_inbound_message<H>(
    context: AxumMessageContext,
    handler: &H,
    message: Message,
) -> Result<bool>
where
    H: AxumMessageHandler,
{
    match message {
        Message::Text(text) => handle_text_message(context, handler, text.to_string()).await,
        Message::Binary(bytes) => {
            // Binary frames count as activity before application handling 二进制帧在应用处理前计为活跃
            context.wing.touch(&context.session).await?;
            handler.handle_binary(context, bytes.to_vec()).await?;
            Ok(true)
        }
        Message::Ping(_) | Message::Pong(_) => {
            // WebSocket control traffic keeps the session active WebSocket 控制帧会保持会话活跃
            context.wing.touch(&context.session).await?;
            Ok(true)
        }
        Message::Close(_) => Ok(false),
    }
}

// Handle text frames, including built-in heartbeat messages 处理文本帧，包括内置心跳消息
async fn handle_text_message<H>(
    context: AxumMessageContext,
    handler: &H,
    text: String,
) -> Result<bool>
where
    H: AxumMessageHandler,
{
    // Try the RustWing protocol path before handing text to the application 先尝试 RustWing 协议路径再交给应用
    if let Ok(message) = serde_json::from_str::<WsMessage>(&text) {
        if is_heartbeat_message(&message) {
            let ack = context
                .wing
                .handle_heartbeat(&context.session, heartbeat_client_time(&message))
                .await?;
            let ack_message = WsMessage {
                version: message.version,
                message_type: MessageType::HeartbeatAck,
                event: Some(HEARTBEAT_EVENT.into()),
                request_id: message.request_id,
                trace_id: message.trace_id,
                seq: message.seq,
                client_time: message.client_time,
                code: None,
                message: None,
                server_time: Some(now_millis()),
                data: Some(serde_json::to_value(ack)?),
            };
            context.session.enqueue(ack_message.to_text_frame()?)?;
            return Ok(true);
        }
    }

    // Non-heartbeat text counts as activity before application handling 非心跳文本在应用处理前计为活跃
    context.wing.touch(&context.session).await?;
    handler.handle_text(context, text).await?;
    Ok(true)
}

// Convert a RustWing outbound frame into an Axum WebSocket message 转换 RustWing 出站帧为 Axum WebSocket 消息
fn axum_message_from_frame(frame: OutboundFrame) -> Message {
    match frame.kind {
        FrameKind::Text => {
            Message::Text(String::from_utf8_lossy(&frame.payload).into_owned().into())
        }
        FrameKind::Binary => Message::Binary(frame.payload.into()),
        FrameKind::Close => Message::Close(None),
    }
}

// Detect whether a protocol message is a heartbeat 判断协议消息是否为心跳
fn is_heartbeat_message(message: &WsMessage) -> bool {
    message.message_type == MessageType::Heartbeat
        || message.event.as_deref() == Some(HEARTBEAT_EVENT)
}

// Extract the client heartbeat timestamp from a message 从消息中提取客户端心跳时间
fn heartbeat_client_time(message: &WsMessage) -> Option<i64> {
    message
        .data
        .clone()
        .and_then(|data| serde_json::from_value::<HeartbeatData>(data).ok())
        .and_then(|data| data.client_time)
        .or(message.client_time)
}

// Map adapter-local errors to core errors 将适配器局部错误映射为核心错误
#[allow(dead_code)]
fn axum_error(message: impl Into<String>) -> RustWingError {
    RustWingError::Cluster(message.into())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

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
}

use std::sync::Arc;

use async_trait::async_trait;
use axum::extract::ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade, close_code};
use axum::response::{IntoResponse, Response};
use futures_util::{SinkExt, StreamExt};
use rust_wing_core::{
    DisconnectCause, FrameKind, HEARTBEAT_EVENT, HeartbeatData, Identity, MessageType,
    OutboundFrame, Result, RustWing, RustWingError, Session, WsMessage, now_millis,
};
use tokio::task::JoinHandle;

use crate::auth::{AxumAuthContext, AxumAuthenticator};

// Context passed to application message handlers 传递给应用消息处理器的上下文
#[derive(Clone)]
pub struct AxumMessageContext {
    // Shared RustWing manager 共享 RustWing 管理器
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

// Upgrade an authenticated Axum WebSocket after custom authentication using an application handler 先执行自定义认证再使用应用处理器升级已认证的 Axum WebSocket
pub async fn upgrade_with_auth<A, H>(
    ws: WebSocketUpgrade,
    wing: RustWing,
    context: AxumAuthContext,
    authenticator: A,
    handler: H,
) -> Response
where
    A: AxumAuthenticator,
    H: AxumMessageHandler,
{
    match authenticator.authenticate(context).await {
        Ok(identity) => upgrade_with_handler(ws, wing, identity, handler),
        Err(error) => error.into_response(),
    }
}

// Upgrade an authenticated Axum WebSocket after custom authentication using the default handler 先执行自定义认证再使用默认处理器升级已认证的 Axum WebSocket
pub async fn upgrade_with_auth_default_handler<A>(
    ws: WebSocketUpgrade,
    wing: RustWing,
    context: AxumAuthContext,
    authenticator: A,
) -> Response
where
    A: AxumAuthenticator,
{
    upgrade_with_auth(ws, wing, context, authenticator, NoopAxumMessageHandler).await
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
    let mut close_signal = session.subscribe_close();
    let outbound_task = tokio::spawn(async move {
        loop {
            let pending_close = { close_signal.borrow_and_update().clone() };
            if let Some(reason) = pending_close {
                let reason = String::from_utf8_lossy(&reason).into_owned();
                let close = OutboundFrame::close(reason.clone());
                if sender.send(axum_message_from_frame(close)).await.is_err() {
                    return DisconnectCause::TransportError {
                        message: "websocket close write failed".into(),
                    };
                }
                return DisconnectCause::ServerRequested { reason };
            }
            tokio::select! {
                biased;
                changed = close_signal.changed() => {
                    if changed.is_err() {
                        return DisconnectCause::TransportError {
                            message: "session close signal closed".into(),
                        };
                    }
                }
                frame = outbound.recv() => {
                    let Some(frame) = frame else {
                        return DisconnectCause::TransportError {
                            message: "session outbound queue closed".into(),
                        };
                    };
                    let close_reason = (frame.kind == FrameKind::Close)
                        .then(|| String::from_utf8_lossy(&frame.payload).into_owned());
                    if sender.send(axum_message_from_frame(frame)).await.is_err() {
                        return DisconnectCause::TransportError {
                            message: "websocket write failed".into(),
                        };
                    }
                    if let Some(reason) = close_reason {
                        return DisconnectCause::ServerRequested { reason };
                    }
                }
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

        loop {
            let message = match receiver.next().await {
                Some(Ok(message)) => message,
                Some(Err(error)) => {
                    return DisconnectCause::TransportError {
                        message: error.to_string(),
                    };
                }
                None => {
                    return DisconnectCause::ClientClosed {
                        code: None,
                        reason: None,
                    };
                }
            };
            if let Message::Close(frame) = message {
                return client_close_cause(frame);
            }
            match handle_inbound_message(context.clone(), inbound_handler.as_ref(), message).await {
                Ok(true) => {}
                Ok(false) => {
                    return DisconnectCause::ClientClosed {
                        code: None,
                        reason: None,
                    };
                }
                Err(error) => {
                    return inbound_error_cause(error);
                }
            }
        }
    });

    // Stop when either task finishes so disconnect cleanup happens promptly 任意任务结束后立即做断开清理
    let cause = wait_for_socket_tasks(outbound_task, inbound_task).await;

    let _ = wing.unregister_with_cause(&session, cause).await;
}

// Wait until one socket task exits, preserve its cause, and cancel the remaining task 等待一个 socket 任务退出并保留原因后取消剩余任务
pub(crate) async fn wait_for_socket_tasks(
    mut outbound_task: JoinHandle<DisconnectCause>,
    mut inbound_task: JoinHandle<DisconnectCause>,
) -> DisconnectCause {
    tokio::select! {
        result = &mut outbound_task => {
            // The writer ended first, so stop the reader promptly 写端先结束时立即停止读端
            abort_and_wait(inbound_task).await;
            socket_task_cause(result, "websocket writer task failed")
        }
        result = &mut inbound_task => {
            // The reader ended first, so stop the writer promptly 读端先结束时立即停止写端
            abort_and_wait(outbound_task).await;
            socket_task_cause(result, "websocket reader task failed")
        }
    }
}

// Convert one WebSocket task result into a disconnect cause 将 WebSocket 任务结果转换为断开原因
fn socket_task_cause(
    result: std::result::Result<DisconnectCause, tokio::task::JoinError>,
    context: &str,
) -> DisconnectCause {
    result.unwrap_or_else(|error| DisconnectCause::TransportError {
        message: format!("{context}: {error}"),
    })
}

// Abort a socket task and observe its cancellation 取消 socket 任务并等待其完成取消
async fn abort_and_wait<T>(task: JoinHandle<T>) {
    task.abort();
    let _ = task.await;
}

// Convert an Axum close frame into a typed client cause 将 Axum 关闭帧转换为类型化客户端原因
fn client_close_cause(frame: Option<CloseFrame>) -> DisconnectCause {
    match frame {
        Some(frame) => DisconnectCause::ClientClosed {
            code: Some(frame.code),
            reason: (!frame.reason.is_empty()).then(|| frame.reason.to_string()),
        },
        None => DisconnectCause::ClientClosed {
            code: None,
            reason: None,
        },
    }
}

// Preserve typed queue failures raised while handling inbound messages 保留入站消息处理期间产生的类型化队列失败
fn inbound_error_cause(error: RustWingError) -> DisconnectCause {
    match error {
        RustWingError::QueueFull => DisconnectCause::OutboundQueueFull,
        RustWingError::SessionClosed => DisconnectCause::OutboundReceiverClosed,
        error => DisconnectCause::ApplicationError {
            message: error.to_string(),
        },
    }
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
pub(crate) async fn handle_text_message<H>(
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
pub(crate) fn axum_message_from_frame(frame: OutboundFrame) -> Message {
    match frame.kind {
        FrameKind::Text => {
            Message::Text(String::from_utf8_lossy(&frame.payload).into_owned().into())
        }
        FrameKind::Binary => Message::Binary(frame.payload.into()),
        FrameKind::Ping => Message::Ping(frame.payload.into()),
        FrameKind::Pong => Message::Pong(frame.payload.into()),
        FrameKind::Close => Message::Close(Some(CloseFrame {
            code: close_code::NORMAL,
            reason: String::from_utf8_lossy(&frame.payload).into_owned().into(),
        })),
    }
}

// Detect whether a protocol message is a heartbeat 判断协议消息是否为心跳
fn is_heartbeat_message(message: &WsMessage) -> bool {
    message.message_type == MessageType::Heartbeat
        || message.event.as_deref() == Some(HEARTBEAT_EVENT)
}

// Extract the client heartbeat timestamp from a message 从消息中提取客户端心跳时间
pub(crate) fn heartbeat_client_time(message: &WsMessage) -> Option<i64> {
    message
        .data
        .clone()
        .and_then(|data| serde_json::from_value::<HeartbeatData>(data).ok())
        .and_then(|data| data.client_time)
        .or(message.client_time)
}

use serde::{Deserialize, Serialize};

use crate::session::SessionSnapshot;

// Reason that removed one accepted session 移除一条已接收会话的原因
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DisconnectCause {
    // Client closed the WebSocket 客户端关闭 WebSocket
    ClientClosed {
        // Optional WebSocket close code 可选 WebSocket 关闭码
        code: Option<u16>,
        // Optional client-provided close reason 可选客户端关闭原因
        reason: Option<String>,
    },
    // Server API explicitly requested disconnection 服务端接口主动请求断开
    ServerRequested {
        // Human-readable server reason 可读的服务端原因
        reason: String,
    },
    // A newer session replaced this session 新会话替换了当前会话
    Replaced,
    // Session did not recover after liveness probing 会话在存活探测后仍未恢复
    HeartbeatTimeout,
    // WebSocket transport failed WebSocket 传输失败
    TransportError {
        // Human-readable transport error 可读的传输错误
        message: String,
    },
    // The bounded outbound queue could not accept another frame 有界出站队列无法接收更多帧
    OutboundQueueFull,
    // The outbound frame receiver is no longer available 出站帧接收端已不可用
    OutboundReceiverClosed,
    // Application inbound handler failed 应用入站处理器失败
    ApplicationError {
        // Human-readable application error 可读的应用错误
        message: String,
    },
    // Runtime shutdown removed the session 运行时关闭并移除会话
    RuntimeShutdown,
    // Caller used the generic unregister API 调用方使用通用注销接口
    Unregistered,
}

impl DisconnectCause {
    // Build the close-frame reason associated with this cause 构建与断开原因对应的关闭帧原因
    pub fn close_reason(&self) -> String {
        match self {
            Self::ClientClosed { reason, .. } => reason
                .clone()
                .unwrap_or_else(|| "client closed connection".into()),
            Self::ServerRequested { reason } => reason.clone(),
            Self::Replaced => "replaced by a newer connection".into(),
            Self::HeartbeatTimeout => "heartbeat timeout".into(),
            Self::TransportError { message } => message.clone(),
            Self::OutboundQueueFull => "outbound queue full".into(),
            Self::OutboundReceiverClosed => "outbound receiver closed".into(),
            Self::ApplicationError { message } => message.clone(),
            Self::RuntimeShutdown => "runtime shutdown".into(),
            Self::Unregistered => "unregistered".into(),
        }
    }
}

// Non-blocking notification about an accepted session lifecycle change 已接收会话生命周期变化的非阻塞通知
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SessionEvent {
    // Session completed local and distributed registration 会话已完成本地与分布式注册
    Connected {
        // Connected session snapshot 已连接会话快照
        session: SessionSnapshot,
    },
    // Session was removed from the local registry 会话已从本地注册表移除
    Disconnected {
        // Final session snapshot 最终会话快照
        session: SessionSnapshot,
        // Exact removal cause 精确移除原因
        cause: DisconnectCause,
    },
}

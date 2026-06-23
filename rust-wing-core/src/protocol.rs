use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::Result;
use crate::identity::MessageId;

// Default wire protocol version 默认线上协议版本
pub const DEFAULT_PROTOCOL_VERSION: u16 = 1;
// Heartbeat event name 心跳事件名称
pub const HEARTBEAT_EVENT: &str = "client_report";
// Delivery acknowledgement event name 投递确认事件名称
pub const ACK_EVENT: &str = "ack";

// Logical message category 逻辑消息类别
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageType {
    // Client heartbeat 客户端心跳上报
    Heartbeat,
    // Heartbeat acknowledgement 心跳确认
    HeartbeatAck,
    // Delivery acknowledgement 投递确认
    Ack,
    // Business event 业务事件
    Event,
    // Server-side system event 服务端系统事件
    System,
    // Error response 错误响应
    Error,
}

// Delivery acknowledgement stage 投递确认阶段
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AckStage {
    // Client received the message 客户端已收到消息
    ClientReceived,
    // Client business logic processed the message 客户端业务逻辑已处理消息
    BusinessProcessed,
}

// WebSocket frame category WebSocket 帧类别
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FrameKind {
    // UTF-8 text payload UTF-8 文本负载
    Text,
    // Binary payload 二进制负载
    Binary,
    // Ping control frame Ping 控制帧
    Ping,
    // Pong control frame Pong 控制帧
    Pong,
    // Close control frame 关闭控制帧
    Close,
}

// Serialized protocol message 序列化协议消息
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WsMessage {
    // Protocol version 协议版本
    #[serde(
        default = "default_version",
        skip_serializing_if = "is_default_version"
    )]
    pub version: u16,
    // Message category 消息类别
    #[serde(rename = "type")]
    pub message_type: MessageType,
    // Event name for event-like messages 事件类消息名称
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event: Option<String>,
    // Client request correlation id 客户端请求关联标识
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    // Server message id for optional acknowledgement 服务端消息确认使用的消息标识
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message_id: Option<MessageId>,
    // Distributed tracing id 分布式追踪标识
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trace_id: Option<String>,
    // Optional sequence number 可选序列号
    #[serde(skip_serializing_if = "Option::is_none")]
    pub seq: Option<i64>,
    // Client-reported timestamp 客户端上报时间戳
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_time: Option<i64>,
    // Optional business status code 可选业务状态码
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<i32>,
    // Optional human-readable message 可选可读消息
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    // Server-side timestamp 服务端时间戳
    #[serde(skip_serializing_if = "Option::is_none")]
    pub server_time: Option<i64>,
    // Arbitrary structured payload 任意结构化负载
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

// Heartbeat request payload 心跳请求负载
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeartbeatData {
    // Client heartbeat timestamp 客户端心跳时间戳
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_time: Option<i64>,
}

// Heartbeat response payload 心跳响应负载
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeartbeatAckData {
    // Echoed client heartbeat time 回显客户端心跳时间
    pub client_heartbeat_time: i64,
    // Current server heartbeat time 当前服务端心跳时间
    pub server_heartbeat_time: i64,
    // Last accepted heartbeat time 最近一次已接收心跳时间
    pub last_heartbeat_time: i64,
    // Configured heartbeat interval 配置的心跳间隔
    pub heartbeat_interval_ms: u64,
    // Configured heartbeat timeout 配置的心跳超时
    pub heartbeat_timeout_ms: u64,
}

// Delivery acknowledgement payload 投递确认负载
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AckData {
    // Acknowledged message identifier 已确认的消息标识
    pub message_id: MessageId,
    // Acknowledgement stage 确认阶段
    pub stage: AckStage,
    // Optional client-side timestamp 可选客户端时间戳
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_time: Option<i64>,
}

// Frame queued for outbound delivery 待发送的出站帧
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutboundFrame {
    // Frame kind 帧类别
    pub kind: FrameKind,
    // Frame payload 帧负载
    pub payload: Vec<u8>,
    // Optional message id expected to be acknowledged 期望被确认的可选消息标识
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message_id: Option<MessageId>,
}

impl WsMessage {
    // Build a business event message 构建业务事件消息
    pub fn event(event: impl Into<String>, data: impl Serialize) -> Result<Self> {
        Ok(Self {
            version: DEFAULT_PROTOCOL_VERSION,
            message_type: MessageType::Event,
            event: Some(event.into()),
            request_id: None,
            message_id: None,
            trace_id: None,
            seq: None,
            client_time: None,
            code: None,
            message: None,
            server_time: Some(now_millis()),
            data: Some(serde_json::to_value(data)?),
        })
    }

    // Build a server system message 构建服务端系统消息
    pub fn system(event: impl Into<String>, data: impl Serialize) -> Result<Self> {
        Ok(Self {
            version: DEFAULT_PROTOCOL_VERSION,
            message_type: MessageType::System,
            event: Some(event.into()),
            request_id: None,
            message_id: None,
            trace_id: None,
            seq: None,
            client_time: None,
            code: None,
            message: None,
            server_time: Some(now_millis()),
            data: Some(serde_json::to_value(data)?),
        })
    }

    // Serialize the message as a text frame 将消息序列化为文本帧
    pub fn to_text_frame(&self) -> Result<OutboundFrame> {
        Ok(OutboundFrame {
            kind: FrameKind::Text,
            payload: serde_json::to_vec(self)?,
            message_id: self.message_id.clone(),
        })
    }
}

impl OutboundFrame {
    // Build a text frame 构建文本帧
    pub fn text(payload: impl Into<Vec<u8>>) -> Self {
        Self {
            kind: FrameKind::Text,
            payload: payload.into(),
            message_id: None,
        }
    }

    // Build a binary frame 构建二进制帧
    pub fn binary(payload: impl Into<Vec<u8>>) -> Self {
        Self {
            kind: FrameKind::Binary,
            payload: payload.into(),
            message_id: None,
        }
    }

    // Build a ping frame 构建 Ping 帧
    pub fn ping(payload: impl Into<Vec<u8>>) -> Self {
        Self {
            kind: FrameKind::Ping,
            payload: payload.into(),
            message_id: None,
        }
    }

    // Build a pong frame 构建 Pong 帧
    pub fn pong(payload: impl Into<Vec<u8>>) -> Self {
        Self {
            kind: FrameKind::Pong,
            payload: payload.into(),
            message_id: None,
        }
    }

    // Build a close frame 构建关闭帧
    pub fn close(reason: impl Into<Vec<u8>>) -> Self {
        Self {
            kind: FrameKind::Close,
            payload: reason.into(),
            message_id: None,
        }
    }

    // Mark this frame as requiring acknowledgement 标记该帧需要确认
    pub fn require_ack(mut self, message_id: impl Into<MessageId>) -> Self {
        self.message_id = Some(message_id.into());
        self
    }
}

// Provide the serde default version 提供 serde 默认版本
fn default_version() -> u16 {
    DEFAULT_PROTOCOL_VERSION
}

// Skip serializing the implicit default version 跳过隐式默认版本序列化
fn is_default_version(version: &u16) -> bool {
    *version == DEFAULT_PROTOCOL_VERSION
}

// Read the current Unix timestamp in milliseconds 读取当前 Unix 毫秒时间戳
pub fn now_millis() -> i64 {
    // Return zero only when the system clock predates the epoch 仅在系统时间早于纪元时返回零
    let Ok(duration) = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) else {
        return 0;
    };
    // Convert the timestamp to the wire format 将时间戳转换为线上协议格式
    duration.as_millis() as i64
}

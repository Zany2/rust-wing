use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::Result;

pub const DEFAULT_PROTOCOL_VERSION: u16 = 1;
pub const HEARTBEAT_EVENT: &str = "client_report";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageType {
    Heartbeat,
    HeartbeatAck,
    Event,
    System,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FrameKind {
    Text,
    Binary,
    Close,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WsMessage {
    #[serde(
        default = "default_version",
        skip_serializing_if = "is_default_version"
    )]
    pub version: u16,
    #[serde(rename = "type")]
    pub message_type: MessageType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trace_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub seq: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_time: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub server_time: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeartbeatData {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_time: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeartbeatAckData {
    pub client_heartbeat_time: i64,
    pub server_heartbeat_time: i64,
    pub last_heartbeat_time: i64,
    pub heartbeat_interval_ms: u64,
    pub heartbeat_timeout_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutboundFrame {
    pub kind: FrameKind,
    pub payload: Vec<u8>,
}

impl WsMessage {
    pub fn event(event: impl Into<String>, data: impl Serialize) -> Result<Self> {
        Ok(Self {
            version: DEFAULT_PROTOCOL_VERSION,
            message_type: MessageType::Event,
            event: Some(event.into()),
            request_id: None,
            trace_id: None,
            seq: None,
            client_time: None,
            code: None,
            message: None,
            server_time: Some(now_millis()),
            data: Some(serde_json::to_value(data)?),
        })
    }

    pub fn system(event: impl Into<String>, data: impl Serialize) -> Result<Self> {
        Ok(Self {
            version: DEFAULT_PROTOCOL_VERSION,
            message_type: MessageType::System,
            event: Some(event.into()),
            request_id: None,
            trace_id: None,
            seq: None,
            client_time: None,
            code: None,
            message: None,
            server_time: Some(now_millis()),
            data: Some(serde_json::to_value(data)?),
        })
    }

    pub fn to_text_frame(&self) -> Result<OutboundFrame> {
        Ok(OutboundFrame {
            kind: FrameKind::Text,
            payload: serde_json::to_vec(self)?,
        })
    }
}

impl OutboundFrame {
    pub fn text(payload: impl Into<Vec<u8>>) -> Self {
        Self {
            kind: FrameKind::Text,
            payload: payload.into(),
        }
    }

    pub fn binary(payload: impl Into<Vec<u8>>) -> Self {
        Self {
            kind: FrameKind::Binary,
            payload: payload.into(),
        }
    }

    pub fn close(reason: impl Into<Vec<u8>>) -> Self {
        Self {
            kind: FrameKind::Close,
            payload: reason.into(),
        }
    }
}

fn default_version() -> u16 {
    DEFAULT_PROTOCOL_VERSION
}

fn is_default_version(version: &u16) -> bool {
    *version == DEFAULT_PROTOCOL_VERSION
}

pub fn now_millis() -> i64 {
    let Ok(duration) = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) else {
        return 0;
    };
    duration.as_millis() as i64
}

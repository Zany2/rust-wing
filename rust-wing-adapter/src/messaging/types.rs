use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use rust_wing_core::{ClientId, ConnectionType, OutboundFrame, SessionId, UserId};
use serde::{Deserialize, Serialize};

// External broker message target 外部消息目标
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ExternalMessageTarget {
    // Send to all sessions of one user 向单个用户的全部会话发送
    User {
        #[serde(default)]
        connection_type: Option<ConnectionType>,
        user_id: UserId,
    },
    // Send to sessions of one user-client pair 向单个用户客户端组合发送
    Client {
        #[serde(default)]
        connection_type: Option<ConnectionType>,
        user_id: UserId,
        #[serde(default)]
        client_id: Option<ClientId>,
    },
    // Send to one exact session 向一条精确会话发送
    Session {
        session_id: SessionId,
    },
    // Broadcast inside one connection system 在一个连接体系内广播
    Broadcast {
        #[serde(default)]
        connection_type: Option<ConnectionType>,
    },
    // Broadcast across all connection systems 跨全部连接体系广播
    BroadcastAll,
    // Disconnect all sessions of one user 断开某个用户的全部会话
    DisconnectUser {
        #[serde(default)]
        connection_type: Option<ConnectionType>,
        user_id: UserId,
        #[serde(default)]
        reason: Option<String>,
    },
    // Disconnect sessions of one user-client pair 断开某个用户客户端组合的会话
    DisconnectClient {
        #[serde(default)]
        connection_type: Option<ConnectionType>,
        user_id: UserId,
        #[serde(default)]
        client_id: Option<ClientId>,
        #[serde(default)]
        reason: Option<String>,
    },
    // Disconnect one exact session 断开一条精确会话
    DisconnectSession {
        session_id: SessionId,
        #[serde(default)]
        reason: Option<String>,
    },
}

// External broker message payload 外部消息负载
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "data", rename_all = "snake_case")]
pub enum ExternalMessagePayload {
    // UTF-8 text payload UTF-8 文本负载
    Text(String),
    // Binary payload 二进制负载
    Binary(Vec<u8>),
}

impl Default for ExternalMessagePayload {
    fn default() -> Self {
        Self::Text(String::new())
    }
}

// Message consumed from an external broker 外部消息组件消费到的消息
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExternalMessage {
    pub target: ExternalMessageTarget,
    #[serde(default)]
    pub payload: ExternalMessagePayload,
}

// Shared consumer counters for external broker integrations 外部消息组件集成共享的消费计数器
#[derive(Debug, Clone, Default)]
pub struct ExternalMessageConsumerStats {
    inner: Arc<ExternalMessageConsumerStatsInner>,
}

// Snapshot of external broker consumer counters 外部消息组件消费计数快照
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct ExternalMessageConsumerStatsSnapshot {
    pub received: u64,
    pub decoded: u64,
    pub delivered: u64,
    pub decode_failed: u64,
    pub deliver_failed: u64,
}

// Atomic storage for external message consumer counters 外部消息消费计数的原子存储
#[derive(Debug, Default)]
struct ExternalMessageConsumerStatsInner {
    received: AtomicU64,
    decoded: AtomicU64,
    delivered: AtomicU64,
    decode_failed: AtomicU64,
    deliver_failed: AtomicU64,
}

impl ExternalMessageTarget {
    // Build a default-system user target 构建默认连接体系用户目标
    pub fn user(user_id: impl Into<UserId>) -> Self {
        Self::User {
            connection_type: None,
            user_id: user_id.into(),
        }
    }

    // Build a user target in one connection system 构建指定连接体系用户目标
    pub fn system_user(
        connection_type: impl Into<ConnectionType>,
        user_id: impl Into<UserId>,
    ) -> Self {
        Self::User {
            connection_type: Some(connection_type.into()),
            user_id: user_id.into(),
        }
    }

    // Build a default-system client target 构建默认连接体系客户端目标
    pub fn client<C>(user_id: impl Into<UserId>, client_id: Option<C>) -> Self
    where
        C: Into<ClientId>,
    {
        Self::Client {
            connection_type: None,
            user_id: user_id.into(),
            client_id: client_id.map(Into::into),
        }
    }

    // Build an exact session target 构建精确会话目标
    pub fn session(session_id: impl Into<SessionId>) -> Self {
        Self::Session {
            session_id: session_id.into(),
        }
    }

    // Build a default-system broadcast target 构建默认连接体系广播目标
    pub fn broadcast() -> Self {
        Self::Broadcast {
            connection_type: None,
        }
    }

    // Build a broadcast target in one connection system 构建指定连接体系广播目标
    pub fn system_broadcast(connection_type: impl Into<ConnectionType>) -> Self {
        Self::Broadcast {
            connection_type: Some(connection_type.into()),
        }
    }

    // Build a default-system user disconnect target 构建默认连接体系用户断开目标
    pub fn disconnect_user(user_id: impl Into<UserId>, reason: impl Into<String>) -> Self {
        Self::DisconnectUser {
            connection_type: None,
            user_id: user_id.into(),
            reason: Some(reason.into()),
        }
    }

    // Build a user disconnect target in one connection system 构建指定连接体系用户断开目标
    pub fn system_disconnect_user(
        connection_type: impl Into<ConnectionType>,
        user_id: impl Into<UserId>,
        reason: impl Into<String>,
    ) -> Self {
        Self::DisconnectUser {
            connection_type: Some(connection_type.into()),
            user_id: user_id.into(),
            reason: Some(reason.into()),
        }
    }

    // Build a default-system client disconnect target 构建默认连接体系客户端断开目标
    pub fn disconnect_client<C>(
        user_id: impl Into<UserId>,
        client_id: Option<C>,
        reason: impl Into<String>,
    ) -> Self
    where
        C: Into<ClientId>,
    {
        Self::DisconnectClient {
            connection_type: None,
            user_id: user_id.into(),
            client_id: client_id.map(Into::into),
            reason: Some(reason.into()),
        }
    }

    // Build a client disconnect target in one connection system 构建指定连接体系客户端断开目标
    pub fn system_disconnect_client<C>(
        connection_type: impl Into<ConnectionType>,
        user_id: impl Into<UserId>,
        client_id: Option<C>,
        reason: impl Into<String>,
    ) -> Self
    where
        C: Into<ClientId>,
    {
        Self::DisconnectClient {
            connection_type: Some(connection_type.into()),
            user_id: user_id.into(),
            client_id: client_id.map(Into::into),
            reason: Some(reason.into()),
        }
    }

    // Build an exact session disconnect target 构建精确会话断开目标
    pub fn disconnect_session(session_id: impl Into<SessionId>, reason: impl Into<String>) -> Self {
        Self::DisconnectSession {
            session_id: session_id.into(),
            reason: Some(reason.into()),
        }
    }

    // Build a global broadcast target 构建全局广播目标
    pub fn broadcast_all() -> Self {
        Self::BroadcastAll
    }
}

impl ExternalMessage {
    // Build a text message for an external broker target 构建外部消息组件文本消息
    pub fn text(target: ExternalMessageTarget, payload: impl Into<String>) -> Self {
        Self {
            target,
            payload: ExternalMessagePayload::Text(payload.into()),
        }
    }

    // Build a binary message for an external broker target 构建外部消息组件二进制消息
    pub fn binary(target: ExternalMessageTarget, payload: impl Into<Vec<u8>>) -> Self {
        Self {
            target,
            payload: ExternalMessagePayload::Binary(payload.into()),
        }
    }

    pub(crate) fn into_frame(self) -> OutboundFrame {
        match self.payload {
            ExternalMessagePayload::Text(payload) => OutboundFrame::text(payload),
            ExternalMessagePayload::Binary(payload) => OutboundFrame::binary(payload),
        }
    }
}

impl ExternalMessageConsumerStats {
    // Capture the current consumer counter values 捕获当前消费计数值
    pub fn snapshot(&self) -> ExternalMessageConsumerStatsSnapshot {
        ExternalMessageConsumerStatsSnapshot {
            received: self.inner.received.load(Ordering::Relaxed),
            decoded: self.inner.decoded.load(Ordering::Relaxed),
            delivered: self.inner.delivered.load(Ordering::Relaxed),
            decode_failed: self.inner.decode_failed.load(Ordering::Relaxed),
            deliver_failed: self.inner.deliver_failed.load(Ordering::Relaxed),
        }
    }

    pub(crate) fn record_received(&self) {
        self.inner.received.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_decoded(&self) {
        self.inner.decoded.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_delivered(&self) {
        self.inner.delivered.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_decode_failed(&self) {
        self.inner.decode_failed.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_deliver_failed(&self) {
        self.inner.deliver_failed.fetch_add(1, Ordering::Relaxed);
    }
}

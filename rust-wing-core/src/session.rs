use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::time::{Duration, SystemTime};

use serde::Serialize;
use tokio::sync::{mpsc, watch};

use crate::error::{Result, RustWingError};
use crate::identity::{ClientId, ConnectionType, Identity, NodeId, SessionId, UserId};
use crate::protocol::{OutboundFrame, now_millis};

// Shareable live session handle 可共享的活跃会话句柄
#[derive(Debug, Clone)]
pub struct Session {
    // Shared mutable session state 共享的可变会话状态
    inner: Arc<SessionInner>,
}

// Internal session state 内部会话状态
#[derive(Debug)]
struct SessionInner {
    // Stable session identifier 稳定会话标识
    id: SessionId,
    // Owning node identifier 所属节点标识
    node_id: NodeId,
    // Logical client identity 逻辑客户端身份
    identity: Identity,
    // Initial connection time 初始连接时间
    connected_at: SystemTime,
    // Non-blocking outbound sender 非阻塞出站发送端
    sender: mpsc::Sender<OutboundFrame>,
    // Latest activity time 最新活跃时间
    last_active_time: AtomicI64,
    // Latest accepted heartbeat time 最新已接收心跳时间
    last_heartbeat_time: AtomicI64,
    // Latest client-reported heartbeat time 最新客户端上报心跳时间
    client_heartbeat_time: AtomicI64,
    // Latest liveness probe send time 最新存活探测发送时间
    last_probe_time: AtomicI64,
    // Whether a liveness probe is waiting for activity 是否存在等待活跃响应的存活探测
    probe_pending: AtomicBool,
    // Closed-state flag 关闭状态标记
    closed: AtomicBool,
    // Close reason signal independent from the bounded frame queue 独立于有界帧队列的关闭原因信号
    close_signal: watch::Sender<Option<Vec<u8>>>,
}

// New session plus its outbound receiver 新会话及其出站接收端
#[derive(Debug)]
pub struct AcceptedSession {
    // Accepted session handle 已接收的会话句柄
    pub session: Session,
    // Frames waiting to be written 待写出的帧
    pub outbound: mpsc::Receiver<OutboundFrame>,
}

// Immutable session view 不可变会话视图
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SessionSnapshot {
    // Session identifier 会话标识
    pub id: SessionId,
    // Owning node identifier 所属节点标识
    pub node_id: NodeId,
    // Connection system identifier 连接体系标识
    pub connection_type: ConnectionType,
    // Owning user identifier 所属用户标识
    pub user_id: UserId,
    // Optional client identifier 可选客户端标识
    pub client_id: Option<ClientId>,
    // Connection establishment time 建立连接时间
    pub connected_at: SystemTime,
    // Latest activity time 最新活跃时间
    pub last_active_time: i64,
    // Latest accepted heartbeat time 最新已接收心跳时间
    pub last_heartbeat_time: i64,
    // Latest client heartbeat time 最新客户端心跳时间
    pub client_heartbeat_time: i64,
    // Latest liveness probe send time 最新存活探测发送时间
    pub last_probe_time: i64,
    // Whether a liveness probe is waiting for activity 是否存在等待活跃响应的存活探测
    pub probe_pending: bool,
    // Closed-state snapshot 关闭状态快照
    pub closed: bool,
}

impl AcceptedSession {
    // Create a session and its bounded outbound queue 创建会话及其有界出站队列
    pub(crate) fn new(node_id: NodeId, identity: Identity, capacity: usize) -> Self {
        // Allocate the bounded channel used by writers 分配写端使用的有界通道
        let (sender, outbound) = mpsc::channel(capacity);
        // Keep close notifications deliverable even when the frame queue is full 即使帧队列已满也保持关闭通知可投递
        let (close_signal, _) = watch::channel(None);
        // Capture the initial activity timestamp 记录初始活跃时间戳
        let now = now_millis();
        // Assemble the shared session state 组装共享会话状态
        let session = Session {
            inner: Arc::new(SessionInner {
                id: SessionId::generate(&node_id),
                node_id,
                identity,
                connected_at: SystemTime::now(),
                sender,
                last_active_time: AtomicI64::new(now),
                last_heartbeat_time: AtomicI64::new(0),
                client_heartbeat_time: AtomicI64::new(0),
                last_probe_time: AtomicI64::new(0),
                probe_pending: AtomicBool::new(false),
                closed: AtomicBool::new(false),
                close_signal,
            }),
        };

        // Return both the handle and receive side 同时返回句柄和接收端
        Self { session, outbound }
    }
}

impl Session {
    // Borrow the session identifier 借用会话标识
    pub fn id(&self) -> &SessionId {
        &self.inner.id
    }

    // Borrow the owning node identifier 借用所属节点标识
    pub fn node_id(&self) -> &NodeId {
        &self.inner.node_id
    }

    // Borrow the logical identity 借用逻辑身份
    pub fn identity(&self) -> &Identity {
        &self.inner.identity
    }

    // Borrow the connection system identifier 借用连接体系标识
    pub fn connection_type(&self) -> &ConnectionType {
        &self.inner.identity.connection_type
    }

    // Borrow the owning user identifier 借用所属用户标识
    pub fn user_id(&self) -> &UserId {
        &self.inner.identity.user_id
    }

    // Borrow the optional client identifier 借用可选客户端标识
    pub fn client_id(&self) -> Option<&ClientId> {
        self.inner.identity.client_id.as_ref()
    }

    // Record generic activity 记录通用活跃状态
    pub fn mark_active(&self) {
        self.inner
            .last_active_time
            .store(now_millis(), Ordering::Relaxed);
        self.clear_probe();
    }

    // Record a heartbeat and optional client timestamp 记录心跳及可选客户端时间戳
    pub fn mark_heartbeat(&self, client_heartbeat_time: Option<i64>) {
        // Capture one consistent server timestamp 记录一个一致的服务端时间戳
        let now = now_millis();
        // Heartbeats also count as general activity 心跳同样代表一般活跃
        self.inner.last_active_time.store(now, Ordering::Relaxed);
        self.clear_probe();
        // Persist the server-side heartbeat time 保存服务端心跳时间
        self.inner.last_heartbeat_time.store(now, Ordering::Relaxed);
        // Update the client-side heartbeat time when provided 若提供则更新客户端心跳时间
        if let Some(client_time) = client_heartbeat_time {
            self.inner
                .client_heartbeat_time
                .store(client_time, Ordering::Relaxed);
        }
    }

    // Queue a frame without blocking the caller 以非阻塞方式将帧放入队列
    pub fn enqueue(&self, frame: OutboundFrame) -> Result<()> {
        // Reject writes after closure 关闭后拒绝写入
        if self.is_closed() {
            return Err(RustWingError::SessionClosed);
        }

        // Apply bounded backpressure through try_send 通过 try_send 施加有界背压
        match self.inner.sender.try_send(frame) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(_)) => {
                // Let the owning manager decide how to terminate the session 由所属管理器决定如何终止会话
                Err(RustWingError::QueueFull)
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                // Report receiver loss without mutating lifecycle ownership 报告接收端丢失且不越权修改生命周期
                Err(RustWingError::SessionClosed)
            }
        }
    }

    // Mark the session closed and notify the writer 标记会话关闭并通知写端
    pub fn close(&self, reason: impl Into<Vec<u8>>) {
        // Only the first closer emits a close frame 仅首次关闭者发送关闭帧
        let already_closed = self.inner.closed.swap(true, Ordering::AcqRel);
        if !already_closed {
            let reason = reason.into();
            let _ = self
                .inner
                .sender
                .try_send(OutboundFrame::close(reason.clone()));
            self.inner.close_signal.send_replace(Some(reason));
        }
    }

    // Subscribe to a close reason that cannot be blocked by frame backpressure 订阅不会被帧背压阻塞的关闭原因
    pub fn subscribe_close(&self) -> watch::Receiver<Option<Vec<u8>>> {
        self.inner.close_signal.subscribe()
    }

    // Read the current closed state 读取当前关闭状态
    pub fn is_closed(&self) -> bool {
        self.inner.closed.load(Ordering::Acquire)
    }

    // Check whether the session exceeded the inactivity timeout 检查会话是否超过不活跃超时
    pub fn is_inactive(&self, timeout: Duration) -> bool {
        // Convert the timeout once into the session clock unit 将超时时长转换为会话时钟单位
        let timeout_ms = timeout.as_millis().min(i64::MAX as u128) as i64;
        // Compare elapsed wall-clock activity time against the threshold 将已流逝活跃时间与阈值比较
        now_millis().saturating_sub(self.inner.last_active_time.load(Ordering::Relaxed))
            >= timeout_ms
    }

    // Mark that a liveness probe was sent 标记已经发送存活探测
    pub(crate) fn mark_probe_sent(&self) -> i64 {
        let now = now_millis();
        self.inner.last_probe_time.store(now, Ordering::Relaxed);
        self.inner.probe_pending.store(true, Ordering::Release);
        now
    }

    // Clear any pending liveness probe 清除待确认的存活探测
    pub(crate) fn clear_probe(&self) {
        self.inner.probe_pending.store(false, Ordering::Release);
        self.inner.last_probe_time.store(0, Ordering::Relaxed);
    }

    // Read whether a liveness probe is pending 读取是否存在待确认存活探测
    pub(crate) fn probe_pending(&self) -> bool {
        self.inner.probe_pending.load(Ordering::Acquire)
    }

    // Check whether the pending liveness probe has expired 检查待确认存活探测是否已超时
    pub(crate) fn probe_expired(&self, timeout: Duration) -> bool {
        if !self.probe_pending() {
            return false;
        }
        let timeout_ms = timeout.as_millis().min(i64::MAX as u128) as i64;
        now_millis().saturating_sub(self.inner.last_probe_time.load(Ordering::Relaxed))
            >= timeout_ms
    }

    // Capture an immutable snapshot of live state 捕获活跃状态的不可变快照
    pub fn snapshot(&self) -> SessionSnapshot {
        SessionSnapshot {
            id: self.inner.id.clone(),
            node_id: self.inner.node_id.clone(),
            connection_type: self.inner.identity.connection_type.clone(),
            user_id: self.inner.identity.user_id.clone(),
            client_id: self.inner.identity.client_id.clone(),
            connected_at: self.inner.connected_at,
            last_active_time: self.inner.last_active_time.load(Ordering::Relaxed),
            last_heartbeat_time: self.inner.last_heartbeat_time.load(Ordering::Relaxed),
            client_heartbeat_time: self.inner.client_heartbeat_time.load(Ordering::Relaxed),
            last_probe_time: self.inner.last_probe_time.load(Ordering::Relaxed),
            probe_pending: self.probe_pending(),
            closed: self.is_closed(),
        }
    }
}

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use rust_wing_core::{NodeId, Result, RustWing};
use serde::Serialize;

// Managed node subscriber lifecycle state 托管节点订阅器的生命周期状态
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeSubscriberStatus {
    // Subscription setup has not completed 订阅尚未完成初始化
    Starting,
    // The subscriber is ready to consume node messages 订阅器已就绪并可消费节点消息
    Running,
    // The subscriber is restoring a lost broker connection 订阅器正在恢复断开的消息组件连接
    Reconnecting,
    // The subscriber stopped because of an unrecoverable error 订阅器因不可恢复错误停止
    Failed(String),
    // The subscriber was stopped intentionally 订阅器已被主动停止
    Stopped,
}

impl NodeSubscriberStatus {
    // Check whether the subscriber can consume node messages 检查订阅器是否可以消费节点消息
    pub fn is_ready(&self) -> bool {
        matches!(self, Self::Running)
    }
}

// Shared node subscriber counters 共享的节点订阅器计数器
#[derive(Clone, Default)]
pub struct NodeSubscriberStats {
    inner: Arc<NodeSubscriberStatsInner>,
}

#[derive(Default)]
struct NodeSubscriberStatsInner {
    messages_received_total: AtomicU64,
    decode_failed_total: AtomicU64,
    delivery_failed_total: AtomicU64,
    consume_failed_total: AtomicU64,
    reconnects_total: AtomicU64,
}

// Point-in-time node subscriber counter values 节点订阅器计数器的时间点快照
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize)]
pub struct NodeSubscriberStatsSnapshot {
    // Broker messages received by the subscriber 订阅器收到的消息组件消息总数
    pub messages_received_total: u64,
    // Messages rejected because the cluster envelope was invalid 集群信封无效而拒绝的消息总数
    pub decode_failed_total: u64,
    // Valid envelopes that failed local delivery 本地投递失败的有效信封总数
    pub delivery_failed_total: u64,
    // Broker stream or subscription errors 消息组件消费流或订阅错误总数
    pub consume_failed_total: u64,
    // Reconnect cycles entered by the subscriber 订阅器进入重连流程的总次数
    pub reconnects_total: u64,
}

impl NodeSubscriberStats {
    // Capture the current subscriber counters 获取当前订阅器计数快照
    pub fn snapshot(&self) -> NodeSubscriberStatsSnapshot {
        NodeSubscriberStatsSnapshot {
            messages_received_total: self.inner.messages_received_total.load(Ordering::Relaxed),
            decode_failed_total: self.inner.decode_failed_total.load(Ordering::Relaxed),
            delivery_failed_total: self.inner.delivery_failed_total.load(Ordering::Relaxed),
            consume_failed_total: self.inner.consume_failed_total.load(Ordering::Relaxed),
            reconnects_total: self.inner.reconnects_total.load(Ordering::Relaxed),
        }
    }

    // Record one broker message 记录一条消息组件消息
    #[cfg(any(
        feature = "redis",
        feature = "nats",
        all(feature = "kafka", not(windows)),
        test
    ))]
    pub(crate) fn record_received(&self) {
        self.inner
            .messages_received_total
            .fetch_add(1, Ordering::Relaxed);
    }

    // Record one invalid cluster envelope 记录一个无效集群信封
    #[cfg(any(
        feature = "redis",
        feature = "nats",
        all(feature = "kafka", not(windows)),
        test
    ))]
    pub(crate) fn record_decode_failed(&self) {
        self.inner
            .decode_failed_total
            .fetch_add(1, Ordering::Relaxed);
    }

    // Record one local delivery failure 记录一次本地投递失败
    #[cfg(any(
        feature = "redis",
        feature = "nats",
        all(feature = "kafka", not(windows)),
        test
    ))]
    pub(crate) fn record_delivery_failed(&self) {
        self.inner
            .delivery_failed_total
            .fetch_add(1, Ordering::Relaxed);
    }

    // Record one broker consumption failure 记录一次消息组件消费失败
    #[cfg(any(
        feature = "redis",
        feature = "nats",
        all(feature = "kafka", not(windows)),
        test
    ))]
    pub(crate) fn record_consume_failed(&self) {
        self.inner
            .consume_failed_total
            .fetch_add(1, Ordering::Relaxed);
    }

    // Record one reconnect cycle 记录一次重连流程
    #[cfg(any(feature = "redis", test))]
    pub(crate) fn record_reconnect(&self) {
        self.inner.reconnects_total.fetch_add(1, Ordering::Relaxed);
    }
}

// Running node subscriber owned by a managed runtime 托管运行时持有的运行中节点订阅器
#[async_trait]
pub trait ManagedNodeSubscriber: Send {
    // Return the latest subscriber lifecycle state 返回最新的订阅器生命周期状态
    fn status(&self) -> NodeSubscriberStatus;

    // Return a point-in-time subscriber counter snapshot 返回订阅器计数器的时间点快照
    fn stats(&self) -> NodeSubscriberStatsSnapshot;

    // Stop the subscriber and wait for its background task 停止订阅器并等待后台任务结束
    async fn shutdown(self: Box<Self>) -> Result<()>;
}

// Node subscriber adapter that establishes readiness before returning 建立就绪状态后才返回的节点订阅适配器
#[async_trait]
pub trait NodeSubscriberAdapter: Send + Sync {
    // Start the subscriber for the manager's configured node 启动管理器当前节点的订阅器
    async fn start_current_node(&self, wing: RustWing) -> Result<Box<dyn ManagedNodeSubscriber>> {
        self.start_for_node(wing.config().node_id.clone(), wing)
            .await
    }

    // Start one node subscriber and return only after subscription setup succeeds 启动指定节点订阅器并在订阅成功后返回
    async fn start_for_node(
        &self,
        node_id: NodeId,
        wing: RustWing,
    ) -> Result<Box<dyn ManagedNodeSubscriber>>;
}

#[cfg(test)]
mod tests {
    use super::NodeSubscriberStats;

    // Subscriber counters keep each error class independent 订阅器计数器会独立记录各类错误
    #[test]
    fn subscriber_stats_track_error_classes() {
        let stats = NodeSubscriberStats::default();
        stats.record_received();
        stats.record_decode_failed();
        stats.record_delivery_failed();
        stats.record_consume_failed();
        stats.record_reconnect();

        let snapshot = stats.snapshot();
        assert_eq!(snapshot.messages_received_total, 1);
        assert_eq!(snapshot.decode_failed_total, 1);
        assert_eq!(snapshot.delivery_failed_total, 1);
        assert_eq!(snapshot.consume_failed_total, 1);
        assert_eq!(snapshot.reconnects_total, 1);
    }
}

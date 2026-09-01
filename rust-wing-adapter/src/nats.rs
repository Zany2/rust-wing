use async_trait::async_trait;
use futures_util::StreamExt;
use rust_wing_core::{ClusterEnvelope, NodeId, Result, RustWing, RustWingError};
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::messaging::{ExternalMessageConsumerStats, process_external_message_payload};
use crate::{
    ManagedNodeSubscriber, NodePublisherAdapter, NodeSubscriberAdapter, NodeSubscriberStats,
    NodeSubscriberStatsSnapshot, NodeSubscriberStatus,
};

// NATS node message transport configuration NATS 节点消息传输配置
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NatsPublisherConfig {
    // NATS seed URLs, for example ["nats://nats-1:4222", "nats://nats-2:4222"] NATS 种子地址列表
    pub urls: Vec<String>,
    // Subject prefix shared by all node subjects 所有节点主题的统一前缀
    pub subject_prefix: String,
}

// NATS-backed node publisher adapter NATS 节点消息发布适配器
#[derive(Clone)]
pub struct NatsNodePublisherAdapter {
    // Shared reconnecting NATS client 共享且支持重连的 NATS 客户端
    client: async_nats::Client,
    // Runtime publisher configuration 运行期发布配置
    config: NatsPublisherConfig,
}

// NATS-backed node subscriber adapter NATS 节点消息订阅适配器
#[derive(Clone)]
pub struct NatsNodeSubscriberAdapter {
    // Shared reconnecting NATS client 共享且支持重连的 NATS 客户端
    client: async_nats::Client,
    // Runtime subscriber configuration 运行期订阅配置
    config: NatsPublisherConfig,
}

// Running NATS node subscriber handle 正在运行的 NATS 节点订阅句柄
pub struct NatsNodeSubscriberHandle {
    // Stop signal sent to the subscriber task 发送给订阅任务的停止信号
    stop: watch::Sender<bool>,
    // Background subscriber task 后台订阅任务
    task: JoinHandle<Result<()>>,
    // Latest subscriber lifecycle state 最新订阅器生命周期状态
    status: watch::Receiver<NodeSubscriberStatus>,
    // Shared subscriber counters 共享的订阅器计数器
    stats: NodeSubscriberStats,
}

// NATS external message consumer configuration NATS 外部消息消费者配置
#[derive(Debug, Clone)]
pub struct NatsExternalMessageConsumerConfig {
    // NATS seed URLs, for example ["nats://nats-1:4222", "nats://nats-2:4222"] NATS 种子地址列表
    pub urls: Vec<String>,
    // NATS subject that carries ExternalMessage JSON 承载 ExternalMessage JSON 的 NATS subject
    pub subject: String,
}

// Running NATS external message consumer handle 正在运行的 NATS 外部消息消费者句柄
pub struct NatsExternalMessageConsumerHandle {
    // Shared consumer counters 共享消费计数器
    stats: ExternalMessageConsumerStats,
    // Background task that owns the NATS subscription 持有 NATS 订阅的后台任务
    task: JoinHandle<()>,
}

impl NatsPublisherConfig {
    // Create NATS node message transport configuration 创建 NATS 节点消息传输配置
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            urls: vec![url.into()],
            subject_prefix: "rust-wing.node".into(),
        }
    }

    // Create NATS node transport configuration from multiple seed URLs 通过多个种子地址创建 NATS 节点传输配置
    pub fn from_urls(urls: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            urls: urls.into_iter().map(Into::into).collect(),
            subject_prefix: "rust-wing.node".into(),
        }
    }

    // Override the node subject prefix 覆盖节点主题前缀
    pub fn with_subject_prefix(mut self, subject_prefix: impl Into<String>) -> Self {
        self.subject_prefix = subject_prefix.into();
        self
    }

    // Validate required NATS node transport configuration 校验必填 NATS 节点传输配置
    pub fn validate(&self) -> Result<()> {
        if self.urls.is_empty() || self.urls.iter().any(|url| url.trim().is_empty()) {
            return Err(RustWingError::InvalidConfig(
                "nats publisher urls cannot be empty".into(),
            ));
        }
        validate_nats_subject(&self.subject_prefix, "nats publisher subject_prefix")
    }
}

impl NatsNodePublisherAdapter {
    // Connect the node publisher to NATS 连接 NATS 节点发布器
    pub async fn connect(config: NatsPublisherConfig) -> Result<Self> {
        config.validate()?;
        let client = async_nats::connect(config.urls.clone())
            .await
            .map_err(|error| nats_error("connect nats node publisher", error))?;
        Ok(Self { client, config })
    }

    // Borrow the effective NATS publisher configuration 借用当前 NATS 发布配置
    pub fn config(&self) -> &NatsPublisherConfig {
        &self.config
    }

    // Build the NATS subject for one node 构建单个节点的 NATS 主题
    fn subject_for_node(&self, node_id: &NodeId) -> Result<String> {
        nats_node_subject(&self.config.subject_prefix, node_id)
    }
}

#[async_trait]
impl NodePublisherAdapter for NatsNodePublisherAdapter {
    // Publish one cluster envelope to the target node subject 发布集群信封到目标节点主题
    async fn publish(&self, node_id: &NodeId, envelope: ClusterEnvelope) -> Result<()> {
        let subject = self.subject_for_node(node_id)?;
        let payload = serde_json::to_vec(&envelope)?;
        self.client
            .publish(subject, payload.into())
            .await
            .map_err(|error| nats_error("publish nats cluster envelope", error))
    }
}

impl NatsNodeSubscriberAdapter {
    // Connect the node subscriber to NATS 连接 NATS 节点订阅器
    pub async fn connect(config: NatsPublisherConfig) -> Result<Self> {
        config.validate()?;
        let client = async_nats::connect(config.urls.clone())
            .await
            .map_err(|error| nats_error("connect nats node subscriber", error))?;
        Ok(Self { client, config })
    }

    // Borrow the effective NATS subscriber configuration 借用当前 NATS 订阅配置
    pub fn config(&self) -> &NatsPublisherConfig {
        &self.config
    }

    // Consume messages for the manager's configured node 消费管理器当前节点的消息
    pub async fn run_current_node(&self, wing: RustWing) -> Result<()> {
        self.run_for_node(wing.config().node_id.clone(), wing).await
    }

    // Consume messages for one node until the subscription ends 为指定节点持续消费消息直到订阅结束
    pub async fn run_for_node(&self, node_id: NodeId, wing: RustWing) -> Result<()> {
        let (_stop_tx, stop_rx) = watch::channel(false);
        let (status_tx, _status_rx) = watch::channel(NodeSubscriberStatus::Starting);
        self.run_for_node_until_stop(
            node_id,
            wing,
            stop_rx,
            status_tx,
            NodeSubscriberStats::default(),
        )
        .await
    }

    // Spawn a subscriber task without waiting for subscription readiness 不等待订阅就绪并启动当前节点订阅任务
    pub fn spawn_current_node(&self, wing: RustWing) -> NatsNodeSubscriberHandle {
        self.spawn_for_node(wing.config().node_id.clone(), wing)
    }

    // Spawn one subscriber task without waiting for subscription readiness 不等待订阅就绪并启动指定节点订阅任务
    pub fn spawn_for_node(&self, node_id: NodeId, wing: RustWing) -> NatsNodeSubscriberHandle {
        let subscriber = self.clone();
        let (stop, stop_rx) = watch::channel(false);
        let (status_tx, status) = watch::channel(NodeSubscriberStatus::Starting);
        let stats = NodeSubscriberStats::default();
        let task_stats = stats.clone();
        let task = tokio::spawn(async move {
            subscriber
                .run_for_node_until_stop(node_id, wing, stop_rx, status_tx, task_stats)
                .await
        });
        NatsNodeSubscriberHandle {
            stop,
            task,
            status,
            stats,
        }
    }

    // Start a ready subscriber for the manager's configured node 为管理器当前节点启动已就绪的订阅器
    pub async fn start_current_node(&self, wing: RustWing) -> Result<NatsNodeSubscriberHandle> {
        self.start_for_node(wing.config().node_id.clone(), wing)
            .await
    }

    // Start a node subscriber after the NATS subscription succeeds 在 NATS 订阅成功后启动指定节点订阅器
    pub async fn start_for_node(
        &self,
        node_id: NodeId,
        wing: RustWing,
    ) -> Result<NatsNodeSubscriberHandle> {
        let subscriber = self.subscribe_for_node(&node_id).await?;
        let (stop, stop_rx) = watch::channel(false);
        let (status_tx, status) = watch::channel(NodeSubscriberStatus::Running);
        let stats = NodeSubscriberStats::default();
        let task = tokio::spawn(Self::consume_subscription(
            subscriber,
            wing,
            stop_rx,
            status_tx,
            stats.clone(),
        ));
        Ok(NatsNodeSubscriberHandle {
            stop,
            task,
            status,
            stats,
        })
    }

    // Consume messages until the subscription ends or a stop signal arrives 消费消息直到订阅结束或收到停止信号
    async fn run_for_node_until_stop(
        &self,
        node_id: NodeId,
        wing: RustWing,
        stop_rx: watch::Receiver<bool>,
        status: watch::Sender<NodeSubscriberStatus>,
        stats: NodeSubscriberStats,
    ) -> Result<()> {
        let subscriber = match self.subscribe_for_node(&node_id).await {
            Ok(subscriber) => subscriber,
            Err(error) => {
                stats.record_consume_failed();
                let _ = status.send(NodeSubscriberStatus::Failed(error.to_string()));
                return Err(error);
            }
        };
        Self::consume_subscription(subscriber, wing, stop_rx, status, stats).await
    }

    // Create a NATS subscription for one node 创建指定节点的 NATS 订阅
    async fn subscribe_for_node(&self, node_id: &NodeId) -> Result<async_nats::Subscriber> {
        let subject = nats_node_subject(&self.config.subject_prefix, node_id)?;
        let subscriber = self
            .client
            .subscribe(subject)
            .await
            .map_err(|error| nats_error("subscribe nats node subject", error))?;
        // Flush the SUB command so callers cannot publish before the server observes readiness 刷新 SUB 命令以避免调用方在服务端确认就绪前发布
        self.client
            .flush()
            .await
            .map_err(|error| nats_error("flush nats node subscription", error))?;
        Ok(subscriber)
    }

    // Consume one established NATS subscription 消费一个已经建立的 NATS 订阅
    async fn consume_subscription(
        mut subscriber: async_nats::Subscriber,
        wing: RustWing,
        mut stop_rx: watch::Receiver<bool>,
        status: watch::Sender<NodeSubscriberStatus>,
        stats: NodeSubscriberStats,
    ) -> Result<()> {
        let _ = status.send(NodeSubscriberStatus::Running);
        loop {
            tokio::select! {
                message = subscriber.next() => {
                    let Some(message) = message else {
                        stats.record_consume_failed();
                        let error = RustWingError::Cluster("nats node subscription ended".into());
                        let _ = status.send(NodeSubscriberStatus::Failed(error.to_string()));
                        return Err(error);
                    };
                    stats.record_received();
                    let envelope = match serde_json::from_slice::<ClusterEnvelope>(&message.payload) {
                        Ok(envelope) => envelope,
                        Err(_) => {
                            stats.record_decode_failed();
                            continue;
                        }
                    };
                    if wing.handle_cluster_envelope_async(envelope).await.is_err() {
                        stats.record_delivery_failed();
                    }
                }
                changed = stop_rx.changed() => {
                    if changed.is_err() || *stop_rx.borrow() {
                        let _ = status.send(NodeSubscriberStatus::Stopped);
                        return Ok(());
                    }
                }
            }
        }
    }
}

impl NatsNodeSubscriberHandle {
    // Return the latest subscriber lifecycle state 返回最新的订阅器生命周期状态
    pub fn status(&self) -> NodeSubscriberStatus {
        self.status.borrow().clone()
    }

    // Return a point-in-time subscriber counter snapshot 返回订阅器计数器的时间点快照
    pub fn stats(&self) -> NodeSubscriberStatsSnapshot {
        self.stats.snapshot()
    }

    // Ask the subscriber task to stop and wait for it 请求订阅任务停止并等待结束
    pub async fn shutdown(self) -> Result<()> {
        let _ = self.stop.send(true);
        self.task
            .await
            .map_err(|error| RustWingError::Cluster(format!("nats subscriber task: {error}")))?
    }

    // Wait for the subscriber task to finish without sending a stop signal 等待订阅任务自行结束且不发送停止信号
    pub async fn join(self) -> Result<()> {
        self.task
            .await
            .map_err(|error| RustWingError::Cluster(format!("nats subscriber task: {error}")))?
    }
}

#[async_trait]
impl NodeSubscriberAdapter for NatsNodeSubscriberAdapter {
    async fn start_for_node(
        &self,
        node_id: NodeId,
        wing: RustWing,
    ) -> Result<Box<dyn ManagedNodeSubscriber>> {
        NatsNodeSubscriberAdapter::start_for_node(self, node_id, wing)
            .await
            .map(|subscriber| Box::new(subscriber) as Box<dyn ManagedNodeSubscriber>)
    }
}

#[async_trait]
impl ManagedNodeSubscriber for NatsNodeSubscriberHandle {
    fn status(&self) -> NodeSubscriberStatus {
        NatsNodeSubscriberHandle::status(self)
    }

    fn stats(&self) -> NodeSubscriberStatsSnapshot {
        NatsNodeSubscriberHandle::stats(self)
    }

    async fn shutdown(self: Box<Self>) -> Result<()> {
        NatsNodeSubscriberHandle::shutdown(*self).await
    }
}

impl NatsExternalMessageConsumerConfig {
    // Create a NATS external message consumer configuration 创建 NATS 外部消息消费者配置
    pub fn new(url: impl Into<String>, subject: impl Into<String>) -> Self {
        Self {
            urls: vec![url.into()],
            subject: subject.into(),
        }
    }

    // Create an external consumer configuration from multiple seed URLs 通过多个种子地址创建外部消费者配置
    pub fn from_urls(
        urls: impl IntoIterator<Item = impl Into<String>>,
        subject: impl Into<String>,
    ) -> Self {
        Self {
            urls: urls.into_iter().map(Into::into).collect(),
            subject: subject.into(),
        }
    }
}

impl NatsExternalMessageConsumerHandle {
    // Borrow the shared consumer counters 借用共享消费计数器
    pub fn stats(&self) -> ExternalMessageConsumerStats {
        self.stats.clone()
    }

    // Stop the background consumer task 停止后台消费者任务
    pub fn stop(&self) {
        self.task.abort();
    }
}

// Spawn a NATS subscriber that delivers ExternalMessage JSON through RustWing 启动通过 RustWing 投递 ExternalMessage JSON 的 NATS 订阅者
pub async fn spawn_nats_external_message_consumer(
    wing: RustWing,
    config: NatsExternalMessageConsumerConfig,
) -> Result<NatsExternalMessageConsumerHandle> {
    if config.urls.is_empty() || config.urls.iter().any(|url| url.trim().is_empty()) {
        return Err(RustWingError::InvalidConfig(
            "nats external consumer urls cannot be empty".into(),
        ));
    }
    let client = async_nats::connect(config.urls)
        .await
        .map_err(|error| RustWingError::Cluster(error.to_string()))?;
    let mut subscriber = client
        .subscribe(config.subject)
        .await
        .map_err(|error| RustWingError::Cluster(error.to_string()))?;

    let stats = ExternalMessageConsumerStats::default();
    let task_stats = stats.clone();
    let task = tokio::spawn(async move {
        while let Some(message) = subscriber.next().await {
            let _ = process_external_message_payload(&wing, &task_stats, message.payload).await;
        }
    });

    Ok(NatsExternalMessageConsumerHandle { stats, task })
}

// Build the exact NATS subject for one node 构建单个节点的精确 NATS 主题
fn nats_node_subject(subject_prefix: &str, node_id: &NodeId) -> Result<String> {
    validate_nats_subject(subject_prefix, "nats publisher subject_prefix")?;
    validate_nats_subject(node_id.as_str(), "nats node_id")?;
    Ok(format!("{subject_prefix}.{}", node_id.as_str()))
}

// Reject empty or wildcard NATS subjects 拒绝空主题或带通配符的 NATS 主题
fn validate_nats_subject(value: &str, name: &str) -> Result<()> {
    if value.trim().is_empty()
        || value.split('.').any(str::is_empty)
        || value.chars().any(char::is_whitespace)
        || value.contains(['*', '>'])
    {
        return Err(RustWingError::InvalidConfig(format!(
            "{name} must be an exact NATS subject"
        )));
    }
    Ok(())
}

// Convert NATS errors into the core error type 将 NATS 错误转换为核心错误类型
fn nats_error(action: &str, error: impl std::fmt::Display) -> RustWingError {
    RustWingError::Cluster(format!("{action}: {error}"))
}

#[cfg(test)]
mod tests {
    use super::{NatsPublisherConfig, nats_node_subject};
    use rust_wing_core::NodeId;

    // NATS node subjects are scoped by node id NATS 节点主题会按节点标识隔离
    #[test]
    fn publisher_subject_includes_node_id() {
        assert_eq!(
            nats_node_subject("rust-wing.node", &NodeId::from("node-a")).unwrap(),
            "rust-wing.node.node-a"
        );
    }

    // NATS node transport rejects wildcard subject prefixes NATS 节点传输拒绝通配主题前缀
    #[test]
    fn publisher_config_rejects_wildcard_subject_prefix() {
        let config =
            NatsPublisherConfig::new("nats://127.0.0.1:4222").with_subject_prefix("rust-wing.*");
        assert!(config.validate().is_err());
    }

    // NATS node transport keeps every configured seed URL NATS 节点传输会保留全部种子地址
    #[test]
    fn publisher_config_accepts_multiple_seed_urls() {
        let config = NatsPublisherConfig::from_urls(["nats://nats-1:4222", "nats://nats-2:4222"]);

        assert!(config.validate().is_ok());
        assert_eq!(config.urls.len(), 2);
    }
}

use async_trait::async_trait;
use rust_wing_core::{NodeId, Result, RustWing, RustWingError};
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::messaging::ExternalMessageConsumerStats;
use crate::{
    ManagedNodeSubscriber, NodeSubscriberAdapter, NodeSubscriberStats, NodeSubscriberStatsSnapshot,
    NodeSubscriberStatus,
};

// Kafka node message transport configuration Kafka 节点消息传输配置
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KafkaPublisherConfig {
    // Comma-separated Kafka bootstrap servers, for example 127.0.0.1:9092,127.0.0.2:9092 Kafka 启动服务器地址，多个地址使用逗号分隔
    pub brokers: String,
    // Topic prefix shared by all node topics 所有节点主题的统一前缀
    pub topic_prefix: String,
    // Consumer group prefix shared by node subscribers 节点订阅器的统一消费组前缀
    pub consumer_group_prefix: String,
}

// Running Kafka node subscriber handle 正在运行的 Kafka 节点订阅句柄
pub struct KafkaNodeSubscriberHandle {
    // Stop signal sent to the subscriber task 发送给订阅任务的停止信号
    stop: watch::Sender<bool>,
    // Background subscriber task 后台订阅任务
    task: JoinHandle<Result<()>>,
    // Latest subscriber lifecycle state 最新订阅器生命周期状态
    status: watch::Receiver<NodeSubscriberStatus>,
    // Shared subscriber counters 共享的订阅器计数器
    stats: NodeSubscriberStats,
}

// Kafka external message consumer configuration Kafka 外部消息消费者配置
#[derive(Debug, Clone)]
pub struct KafkaExternalMessageConsumerConfig {
    // Comma-separated Kafka bootstrap servers, for example 127.0.0.1:9092,127.0.0.2:9092 Kafka 启动服务器地址，多个地址使用逗号分隔
    pub brokers: String,
    // Kafka consumer group id Kafka 消费组标识
    pub group_id: String,
    // Kafka topic that carries ExternalMessage JSON 承载 ExternalMessage JSON 的 Kafka topic
    pub topic: String,
}

// Running Kafka external message consumer handle 正在运行的 Kafka 外部消息消费者句柄
pub struct KafkaExternalMessageConsumerHandle {
    // Shared consumer counters 共享消费计数器
    stats: ExternalMessageConsumerStats,
    // Background task that owns the Kafka stream 持有 Kafka 流的后台任务
    task: Option<JoinHandle<()>>,
}

impl KafkaPublisherConfig {
    // Create Kafka node message transport configuration 创建 Kafka 节点消息传输配置
    pub fn new(brokers: impl Into<String>) -> Self {
        Self {
            brokers: brokers.into(),
            topic_prefix: "rust-wing-node".into(),
            consumer_group_prefix: "rust-wing-node".into(),
        }
    }

    // Override the node topic prefix 覆盖节点主题前缀
    pub fn with_topic_prefix(mut self, topic_prefix: impl Into<String>) -> Self {
        self.topic_prefix = topic_prefix.into();
        self
    }

    // Override the node consumer group prefix 覆盖节点消费组前缀
    pub fn with_consumer_group_prefix(mut self, consumer_group_prefix: impl Into<String>) -> Self {
        self.consumer_group_prefix = consumer_group_prefix.into();
        self
    }

    // Validate required Kafka node transport configuration 校验必填 Kafka 节点传输配置
    pub fn validate(&self) -> Result<()> {
        if self.brokers.trim().is_empty() {
            return Err(RustWingError::InvalidConfig(
                "kafka publisher brokers cannot be empty".into(),
            ));
        }
        validate_kafka_name(&self.topic_prefix, "kafka publisher topic_prefix")?;
        validate_kafka_name(
            &self.consumer_group_prefix,
            "kafka publisher consumer_group_prefix",
        )
    }
}

impl KafkaNodeSubscriberHandle {
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
            .map_err(|error| RustWingError::Cluster(format!("kafka subscriber task: {error}")))?
    }

    // Wait for the subscriber task to finish without sending a stop signal 等待订阅任务自行结束且不发送停止信号
    pub async fn join(self) -> Result<()> {
        self.task
            .await
            .map_err(|error| RustWingError::Cluster(format!("kafka subscriber task: {error}")))?
    }
}

impl KafkaExternalMessageConsumerConfig {
    // Create a Kafka external message consumer configuration 创建 Kafka 外部消息消费者配置
    pub fn new(
        brokers: impl Into<String>,
        group_id: impl Into<String>,
        topic: impl Into<String>,
    ) -> Self {
        Self {
            brokers: brokers.into(),
            group_id: group_id.into(),
            topic: topic.into(),
        }
    }
}

impl KafkaExternalMessageConsumerHandle {
    // Borrow the shared consumer counters 借用共享消费计数器
    pub fn stats(&self) -> ExternalMessageConsumerStats {
        self.stats.clone()
    }

    // Stop the background consumer task 停止后台消费者任务
    pub fn stop(&self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

#[cfg(not(windows))]
mod platform {
    use std::time::Duration;

    use async_trait::async_trait;
    use futures_util::StreamExt;
    use rdkafka::ClientConfig;
    use rdkafka::Message;
    use rdkafka::consumer::{Consumer, StreamConsumer};
    use rdkafka::producer::{FutureProducer, FutureRecord};
    use rdkafka::util::Timeout;
    use rust_wing_core::{ClusterEnvelope, NodeId, Result, RustWing, RustWingError};
    use tokio::sync::watch;

    use super::{
        KafkaExternalMessageConsumerConfig, KafkaExternalMessageConsumerHandle,
        KafkaNodeSubscriberHandle, KafkaPublisherConfig, kafka_node_consumer_group,
        kafka_node_topic,
    };
    use crate::messaging::{ExternalMessageConsumerStats, process_external_message_payload};
    use crate::{NodePublisherAdapter, NodeSubscriberStats, NodeSubscriberStatus};

    // Kafka-backed node publisher adapter Kafka 节点消息发布适配器
    #[derive(Clone)]
    pub struct KafkaNodePublisherAdapter {
        // Shared asynchronous Kafka producer 共享 Kafka 异步生产者
        producer: FutureProducer,
        // Runtime publisher configuration 运行期发布配置
        config: KafkaPublisherConfig,
    }

    // Kafka-backed node subscriber adapter Kafka 节点消息订阅适配器
    #[derive(Clone)]
    pub struct KafkaNodeSubscriberAdapter {
        // Runtime subscriber configuration 运行期订阅配置
        config: KafkaPublisherConfig,
    }

    impl KafkaNodePublisherAdapter {
        // Create a Kafka node publisher 创建 Kafka 节点发布器
        pub fn connect(config: KafkaPublisherConfig) -> Result<Self> {
            config.validate()?;
            let producer = ClientConfig::new()
                .set("bootstrap.servers", &config.brokers)
                .set("message.timeout.ms", "5000")
                .create()
                .map_err(|error| kafka_error("create kafka node publisher", error))?;
            Ok(Self { producer, config })
        }

        // Borrow the effective Kafka publisher configuration 借用当前 Kafka 发布配置
        pub fn config(&self) -> &KafkaPublisherConfig {
            &self.config
        }
    }

    #[async_trait]
    impl NodePublisherAdapter for KafkaNodePublisherAdapter {
        // Publish one cluster envelope to the target node topic 发布集群信封到目标节点主题
        async fn publish(&self, node_id: &NodeId, envelope: ClusterEnvelope) -> Result<()> {
            let topic = kafka_node_topic(&self.config.topic_prefix, node_id)?;
            let payload = serde_json::to_vec(&envelope)?;
            let record = FutureRecord::<(), [u8]>::to(&topic).payload(payload.as_slice());
            self.producer
                .send(record, Timeout::After(Duration::from_secs(5)))
                .await
                .map(|_| ())
                .map_err(|(error, _)| kafka_error("publish kafka cluster envelope", error))
        }
    }

    impl KafkaNodeSubscriberAdapter {
        // Create a Kafka node subscriber 创建 Kafka 节点订阅器
        pub fn connect(config: KafkaPublisherConfig) -> Result<Self> {
            config.validate()?;
            Ok(Self { config })
        }

        // Borrow the effective Kafka subscriber configuration 借用当前 Kafka 订阅配置
        pub fn config(&self) -> &KafkaPublisherConfig {
            &self.config
        }

        // Consume messages for the manager's configured node 消费管理器当前节点的消息
        pub async fn run_current_node(&self, wing: RustWing) -> Result<()> {
            self.run_for_node(wing.config().node_id.clone(), wing).await
        }

        // Consume messages for one node until the consumer stream ends 为指定节点持续消费消息直到消费流结束
        pub async fn run_for_node(&self, node_id: NodeId, wing: RustWing) -> Result<()> {
            let (_stop_tx, stop_rx) = watch::channel(false);
            self.run_for_node_until_stop(node_id, wing, stop_rx).await
        }

        // Start a managed subscriber task for the manager's configured node 为管理器当前节点启动托管订阅任务
        pub fn spawn_current_node(&self, wing: RustWing) -> Result<KafkaNodeSubscriberHandle> {
            self.spawn_for_node(wing.config().node_id.clone(), wing)
        }

        // Start a managed subscriber task for one node 为指定节点启动托管订阅任务
        pub fn spawn_for_node(
            &self,
            node_id: NodeId,
            wing: RustWing,
        ) -> Result<KafkaNodeSubscriberHandle> {
            let consumer = self.consumer_for_node(&node_id)?;
            let (stop, stop_rx) = watch::channel(false);
            let (status_tx, status) = watch::channel(NodeSubscriberStatus::Running);
            let stats = NodeSubscriberStats::default();
            let task = tokio::spawn(consume_node_topic(
                consumer,
                wing,
                stop_rx,
                status_tx,
                stats.clone(),
            ));
            Ok(KafkaNodeSubscriberHandle {
                stop,
                task,
                status,
                stats,
            })
        }

        // Consume messages until the consumer stream ends or a stop signal arrives 消费消息直到消费流结束或收到停止信号
        async fn run_for_node_until_stop(
            &self,
            node_id: NodeId,
            wing: RustWing,
            stop_rx: watch::Receiver<bool>,
        ) -> Result<()> {
            let consumer = self.consumer_for_node(&node_id)?;
            let (status_tx, _status_rx) = watch::channel(NodeSubscriberStatus::Running);
            consume_node_topic(
                consumer,
                wing,
                stop_rx,
                status_tx,
                NodeSubscriberStats::default(),
            )
            .await
        }

        // Create a consumer dedicated to one node topic 创建专用于单个节点主题的消费者
        fn consumer_for_node(&self, node_id: &NodeId) -> Result<StreamConsumer> {
            let topic = kafka_node_topic(&self.config.topic_prefix, node_id)?;
            let group = kafka_node_consumer_group(&self.config.consumer_group_prefix, node_id)?;
            let consumer: StreamConsumer = ClientConfig::new()
                .set("bootstrap.servers", &self.config.brokers)
                .set("group.id", group)
                .set("enable.partition.eof", "false")
                .set("enable.auto.commit", "true")
                .set("auto.offset.reset", "earliest")
                .create()
                .map_err(|error| kafka_error("create kafka node subscriber", error))?;
            consumer
                .subscribe(&[&topic])
                .map_err(|error| kafka_error("subscribe kafka node topic", error))?;
            Ok(consumer)
        }
    }

    // Consume Kafka cluster envelopes for one local node 消费当前节点的 Kafka 集群信封
    async fn consume_node_topic(
        consumer: StreamConsumer,
        wing: RustWing,
        mut stop_rx: watch::Receiver<bool>,
        status: watch::Sender<NodeSubscriberStatus>,
        stats: NodeSubscriberStats,
    ) -> Result<()> {
        let mut stream = consumer.stream();
        loop {
            tokio::select! {
                message = stream.next() => {
                    let Some(message) = message else {
                        stats.record_consume_failed();
                        let error = RustWingError::Cluster("kafka node consumer stream ended".into());
                        let _ = status.send(NodeSubscriberStatus::Failed(error.to_string()));
                        return Err(error);
                    };
                    let message = match message {
                        Ok(message) => message,
                        Err(_) => {
                            stats.record_consume_failed();
                            continue;
                        }
                    };
                    stats.record_received();
                    let payload = match message.payload() {
                        Some(payload) => payload,
                        None => {
                            stats.record_decode_failed();
                            continue;
                        }
                    };
                    let envelope = match serde_json::from_slice::<ClusterEnvelope>(payload) {
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

    // Spawn a Kafka consumer that delivers ExternalMessage JSON through RustWing 启动通过 RustWing 投递 ExternalMessage JSON 的 Kafka 消费者
    pub fn spawn_kafka_external_message_consumer(
        wing: RustWing,
        config: KafkaExternalMessageConsumerConfig,
    ) -> Result<KafkaExternalMessageConsumerHandle> {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", &config.brokers)
            .set("group.id", &config.group_id)
            .set("enable.partition.eof", "false")
            .set("enable.auto.commit", "true")
            .create()
            .map_err(|error| RustWingError::Cluster(error.to_string()))?;
        consumer
            .subscribe(&[&config.topic])
            .map_err(|error| RustWingError::Cluster(error.to_string()))?;

        let stats = ExternalMessageConsumerStats::default();
        let task_stats = stats.clone();
        let task = tokio::spawn(async move {
            let mut stream = consumer.stream();
            while let Some(message) = stream.next().await {
                let Ok(message) = message else {
                    task_stats.record_received();
                    task_stats.record_decode_failed();
                    continue;
                };
                let Some(payload) = message.payload() else {
                    task_stats.record_received();
                    task_stats.record_decode_failed();
                    continue;
                };
                let _ = process_external_message_payload(&wing, &task_stats, payload).await;
            }
        });

        Ok(KafkaExternalMessageConsumerHandle {
            stats,
            task: Some(task),
        })
    }

    // Convert Kafka errors into the core error type 将 Kafka 错误转换为核心错误类型
    fn kafka_error(action: &str, error: impl std::fmt::Display) -> RustWingError {
        RustWingError::Cluster(format!("{action}: {error}"))
    }
}

#[cfg(windows)]
mod platform {
    use async_trait::async_trait;
    use rust_wing_core::{ClusterEnvelope, NodeId, Result, RustWing, RustWingError};

    use super::{
        KafkaExternalMessageConsumerConfig, KafkaExternalMessageConsumerHandle,
        KafkaNodeSubscriberHandle, KafkaPublisherConfig,
    };
    use crate::NodePublisherAdapter;
    use crate::messaging::ExternalMessageConsumerStats;

    // Unsupported Kafka node publisher placeholder on Windows Windows 上不可用的 Kafka 节点发布器占位类型
    #[derive(Clone)]
    pub struct KafkaNodePublisherAdapter;

    // Unsupported Kafka node subscriber placeholder on Windows Windows 上不可用的 Kafka 节点订阅器占位类型
    #[derive(Clone)]
    pub struct KafkaNodeSubscriberAdapter;

    impl KafkaNodePublisherAdapter {
        // Return a clear unsupported error on Windows builds 当前 Windows 构建返回明确的不可用错误
        pub fn connect(_config: KafkaPublisherConfig) -> Result<Self> {
            Err(RustWingError::BackendUnavailable("kafka".into()))
        }
    }

    #[async_trait]
    impl NodePublisherAdapter for KafkaNodePublisherAdapter {
        // Return a clear unsupported error on Windows builds 当前 Windows 构建返回明确的不可用错误
        async fn publish(&self, _node_id: &NodeId, _envelope: ClusterEnvelope) -> Result<()> {
            Err(RustWingError::BackendUnavailable("kafka".into()))
        }
    }

    impl KafkaNodeSubscriberAdapter {
        // Return a clear unsupported error on Windows builds 当前 Windows 构建返回明确的不可用错误
        pub fn connect(_config: KafkaPublisherConfig) -> Result<Self> {
            Err(RustWingError::BackendUnavailable("kafka".into()))
        }

        // Return a clear unsupported error on Windows builds 当前 Windows 构建返回明确的不可用错误
        pub async fn run_current_node(&self, _wing: RustWing) -> Result<()> {
            Err(RustWingError::BackendUnavailable("kafka".into()))
        }

        // Return a clear unsupported error on Windows builds 当前 Windows 构建返回明确的不可用错误
        pub async fn run_for_node(&self, _node_id: NodeId, _wing: RustWing) -> Result<()> {
            Err(RustWingError::BackendUnavailable("kafka".into()))
        }

        // Return a clear unsupported error on Windows builds 当前 Windows 构建返回明确的不可用错误
        pub fn spawn_current_node(&self, _wing: RustWing) -> Result<KafkaNodeSubscriberHandle> {
            Err(RustWingError::BackendUnavailable("kafka".into()))
        }

        // Return a clear unsupported error on Windows builds 当前 Windows 构建返回明确的不可用错误
        pub fn spawn_for_node(
            &self,
            _node_id: NodeId,
            _wing: RustWing,
        ) -> Result<KafkaNodeSubscriberHandle> {
            Err(RustWingError::BackendUnavailable("kafka".into()))
        }
    }

    // Return a clear unsupported error on Windows builds 当前 Windows 构建返回明确的不可用错误
    pub fn spawn_kafka_external_message_consumer(
        _wing: RustWing,
        _config: KafkaExternalMessageConsumerConfig,
    ) -> Result<KafkaExternalMessageConsumerHandle> {
        Err(RustWingError::BackendUnavailable("kafka".into()))
    }

    // Build an inert handle for documentation tests 为文档测试保留惰性句柄构造
    #[allow(dead_code)]
    fn inert_handle() -> KafkaExternalMessageConsumerHandle {
        KafkaExternalMessageConsumerHandle {
            stats: ExternalMessageConsumerStats::default(),
            task: None,
        }
    }
}

pub use platform::{
    KafkaNodePublisherAdapter, KafkaNodeSubscriberAdapter, spawn_kafka_external_message_consumer,
};

#[async_trait]
impl NodeSubscriberAdapter for KafkaNodeSubscriberAdapter {
    async fn start_for_node(
        &self,
        node_id: NodeId,
        wing: RustWing,
    ) -> Result<Box<dyn ManagedNodeSubscriber>> {
        self.spawn_for_node(node_id, wing)
            .map(|subscriber| Box::new(subscriber) as Box<dyn ManagedNodeSubscriber>)
    }
}

#[async_trait]
impl ManagedNodeSubscriber for KafkaNodeSubscriberHandle {
    fn status(&self) -> NodeSubscriberStatus {
        KafkaNodeSubscriberHandle::status(self)
    }

    fn stats(&self) -> NodeSubscriberStatsSnapshot {
        KafkaNodeSubscriberHandle::stats(self)
    }

    async fn shutdown(self: Box<Self>) -> Result<()> {
        KafkaNodeSubscriberHandle::shutdown(*self).await
    }
}

// Build the exact Kafka topic for one node 构建单个节点的精确 Kafka 主题
#[cfg(any(not(windows), test))]
fn kafka_node_topic(topic_prefix: &str, node_id: &NodeId) -> Result<String> {
    kafka_scoped_name(topic_prefix, node_id, "kafka node topic")
}

// Build the exact Kafka consumer group for one node 构建单个节点的精确 Kafka 消费组
#[cfg(any(not(windows), test))]
fn kafka_node_consumer_group(group_prefix: &str, node_id: &NodeId) -> Result<String> {
    kafka_scoped_name(group_prefix, node_id, "kafka node consumer group")
}

// Build and validate one node-scoped Kafka resource name 构建并校验节点级 Kafka 资源名称
#[cfg(any(not(windows), test))]
fn kafka_scoped_name(prefix: &str, node_id: &NodeId, name: &str) -> Result<String> {
    validate_kafka_name(prefix, name)?;
    validate_kafka_name(node_id.as_str(), "kafka node_id")?;
    let value = format!("{prefix}.{}", node_id.as_str());
    if value.len() > 249 {
        return Err(RustWingError::InvalidConfig(format!(
            "{name} cannot exceed 249 bytes"
        )));
    }
    Ok(value)
}

// Validate one Kafka topic-compatible name 校验 Kafka 主题兼容名称
fn validate_kafka_name(value: &str, name: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 249
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err(RustWingError::InvalidConfig(format!(
            "{name} must use only ASCII letters, digits, '.', '_' or '-'"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{KafkaPublisherConfig, kafka_node_consumer_group, kafka_node_topic};
    use rust_wing_core::NodeId;

    // Kafka node resources are scoped by node id Kafka 节点资源会按节点标识隔离
    #[test]
    fn node_topic_and_group_include_node_id() {
        let node_id = NodeId::from("node-a");
        assert_eq!(
            kafka_node_topic("rust-wing-node", &node_id).unwrap(),
            "rust-wing-node.node-a"
        );
        assert_eq!(
            kafka_node_consumer_group("rust-wing-node", &node_id).unwrap(),
            "rust-wing-node.node-a"
        );
    }

    // Kafka node transport rejects invalid topic prefixes Kafka 节点传输拒绝非法主题前缀
    #[test]
    fn publisher_config_rejects_invalid_topic_prefix() {
        let config = KafkaPublisherConfig::new("127.0.0.1:9092").with_topic_prefix("rust wing");
        assert!(config.validate().is_err());
    }
}

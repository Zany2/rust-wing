use tokio::task::JoinHandle;

use crate::messaging::ExternalMessageConsumerStats;

// Kafka external message consumer configuration Kafka 外部消息消费者配置
#[derive(Debug, Clone)]
pub struct KafkaExternalMessageConsumerConfig {
    // Kafka bootstrap servers Kafka 启动服务器地址
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
    use futures_util::StreamExt;
    use rdkafka::ClientConfig;
    use rdkafka::Message;
    use rdkafka::consumer::{Consumer, StreamConsumer};
    use rust_wing_core::{Result, RustWing, RustWingError};

    use super::{KafkaExternalMessageConsumerConfig, KafkaExternalMessageConsumerHandle};
    use crate::messaging::{ExternalMessageConsumerStats, process_external_message_payload};

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
}

#[cfg(windows)]
mod platform {
    use rust_wing_core::{Result, RustWing, RustWingError};

    use super::{KafkaExternalMessageConsumerConfig, KafkaExternalMessageConsumerHandle};
    use crate::messaging::ExternalMessageConsumerStats;

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

pub use platform::spawn_kafka_external_message_consumer;

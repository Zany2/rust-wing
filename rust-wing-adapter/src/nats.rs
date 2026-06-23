use futures_util::StreamExt;
use rust_wing_core::{Result, RustWing, RustWingError};
use tokio::task::JoinHandle;

use crate::messaging::{ExternalMessageConsumerStats, process_external_message_payload};

// NATS external message consumer configuration NATS 外部消息消费者配置
#[derive(Debug, Clone)]
pub struct NatsExternalMessageConsumerConfig {
    // NATS server URL NATS 服务地址
    pub url: String,
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

impl NatsExternalMessageConsumerConfig {
    // Create a NATS external message consumer configuration 创建 NATS 外部消息消费者配置
    pub fn new(url: impl Into<String>, subject: impl Into<String>) -> Self {
        Self {
            url: url.into(),
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
    let client = async_nats::connect(config.url)
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

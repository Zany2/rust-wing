use async_trait::async_trait;
use rust_wing_core::{
    ClusterEnvelope, NodeId, Result, RuntimeHealth, RuntimeStatus, RustWing, RustWingConfig,
};
use serde::Serialize;

#[cfg(feature = "kafka")]
use crate::{KafkaNodePublisherAdapter, KafkaNodeSubscriberAdapter, KafkaPublisherConfig};
use crate::{
    ManagedNodeSubscriber, NodePublisherAdapter, NodeSubscriberAdapter,
    NodeSubscriberStatsSnapshot, NodeSubscriberStatus, RedisNodePublisherAdapter,
    RedisNodeSubscriberAdapter, RedisPresenceAdapter, RedisPresenceConfig, RedisPublisherConfig,
    rust_wing_from_adapters,
};
#[cfg(feature = "nats")]
use crate::{NatsNodePublisherAdapter, NatsNodeSubscriberAdapter, NatsPublisherConfig};

// Node-to-node transport selected by a distributed runtime 分布式运行时选择的节点间消息传输
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeTransportConfig {
    // Redis Pub/Sub transport Redis Pub/Sub 消息传输
    Redis(RedisPublisherConfig),
    // NATS subject transport NATS Subject 消息传输
    #[cfg(feature = "nats")]
    Nats(NatsPublisherConfig),
    // Kafka topic transport Kafka Topic 消息传输
    #[cfg(feature = "kafka")]
    Kafka(KafkaPublisherConfig),
}

// Redis presence plus one selected node transport Redis 在线路由与一种节点消息传输的组合配置
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DistributedRuntimeConfig {
    // Redis-backed presence and node lease configuration Redis 在线路由与节点租约配置
    pub presence: RedisPresenceConfig,
    // Node-to-node message transport configuration 节点间消息传输配置
    pub transport: NodeTransportConfig,
}

// Managed distributed RustWing runtime 托管的分布式 RustWing 运行时
pub struct DistributedRustWing {
    // Core connection manager 核心连接管理器
    wing: RustWing,
    // Background subscriber matching the selected transport 与所选传输匹配的后台订阅任务
    subscriber: Box<dyn ManagedNodeSubscriber>,
}

// Aggregated core and node transport health 聚合的核心与节点传输健康状态
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DistributedRuntimeHealth {
    // Aggregated distributed runtime status 聚合后的分布式运行时状态
    pub status: RuntimeStatus,
    // Core manager health 核心管理器健康状态
    pub core: RuntimeHealth,
    // Selected node subscriber status 所选节点订阅器状态
    pub subscriber: NodeSubscriberStatus,
}

impl NodeTransportConfig {
    // Validate the selected transport configuration 校验所选消息传输配置
    pub fn validate(&self) -> Result<()> {
        match self {
            Self::Redis(config) => config.validate(),
            #[cfg(feature = "nats")]
            Self::Nats(config) => config.validate(),
            #[cfg(feature = "kafka")]
            Self::Kafka(config) => config.validate(),
        }
    }

    // Return the stable transport name 返回稳定的消息传输名称
    pub fn name(&self) -> &'static str {
        match self {
            Self::Redis(_) => "redis",
            #[cfg(feature = "nats")]
            Self::Nats(_) => "nats",
            #[cfg(feature = "kafka")]
            Self::Kafka(_) => "kafka",
        }
    }
}

impl From<RedisPublisherConfig> for NodeTransportConfig {
    fn from(config: RedisPublisherConfig) -> Self {
        Self::Redis(config)
    }
}

#[cfg(feature = "nats")]
impl From<NatsPublisherConfig> for NodeTransportConfig {
    fn from(config: NatsPublisherConfig) -> Self {
        Self::Nats(config)
    }
}

#[cfg(feature = "kafka")]
impl From<KafkaPublisherConfig> for NodeTransportConfig {
    fn from(config: KafkaPublisherConfig) -> Self {
        Self::Kafka(config)
    }
}

impl DistributedRuntimeConfig {
    // Create a distributed runtime configuration 创建分布式运行时配置
    pub fn new(presence: RedisPresenceConfig, transport: impl Into<NodeTransportConfig>) -> Self {
        Self {
            presence,
            transport: transport.into(),
        }
    }

    // Validate presence and transport before opening connections 建立连接前校验在线路由与消息传输配置
    pub fn validate(&self) -> Result<()> {
        self.presence.validate()?;
        self.transport.validate()
    }
}

impl DistributedRustWing {
    // Connect Redis presence, start the selected transport, and acquire the node lease 连接 Redis 在线路由、启动所选消息传输并获取节点租约
    pub async fn connect(
        config: RustWingConfig,
        runtime_config: DistributedRuntimeConfig,
    ) -> Result<Self> {
        runtime_config.validate()?;
        let presence = RedisPresenceAdapter::connect(runtime_config.presence).await?;
        let transport = DistributedTransportParts::connect(runtime_config.transport).await?;
        let wing = rust_wing_from_adapters(config, presence, transport.publisher).await?;

        let subscriber = match transport.subscriber.start_current_node(wing.clone()).await {
            Ok(subscriber) => subscriber,
            Err(error) => {
                // Release the lease acquired before subscriber startup failed 订阅任务启动失败时释放此前获取的节点租约
                let _ = wing.shutdown().await;
                return Err(error);
            }
        };

        Ok(Self { wing, subscriber })
    }

    // Borrow the managed core manager 借用托管的核心管理器
    pub fn wing(&self) -> &RustWing {
        &self.wing
    }

    // Clone the core manager handle 克隆核心管理器句柄
    pub fn wing_clone(&self) -> RustWing {
        self.wing.clone()
    }

    // Return the latest node subscriber lifecycle state 返回最新的节点订阅器生命周期状态
    pub fn subscriber_status(&self) -> NodeSubscriberStatus {
        self.subscriber.status()
    }

    // Return aggregated core and subscriber health 返回聚合后的核心与订阅器健康状态
    pub fn health(&self) -> DistributedRuntimeHealth {
        let core = self.wing.health();
        let subscriber = self.subscriber.status();
        let status = aggregate_runtime_status(core.status, &subscriber);
        DistributedRuntimeHealth {
            status,
            core,
            subscriber,
        }
    }

    // Check whether both core routing and the node subscriber are ready 检查核心路由与节点订阅器是否均已就绪
    pub fn is_ready(&self) -> bool {
        self.health().status == RuntimeStatus::Running
    }

    // Return a point-in-time node subscriber counter snapshot 返回节点订阅器计数器的时间点快照
    pub fn subscriber_stats(&self) -> NodeSubscriberStatsSnapshot {
        self.subscriber.stats()
    }

    // Drain local routes before stopping the node subscriber 清理本地路由后再停止节点订阅
    pub async fn shutdown(self) -> Result<usize> {
        shutdown_managed_runtime(self.wing, self.subscriber).await
    }
}

// Keep node delivery available until core sessions, routes, and leases are drained 保持节点投递直到核心会话、路由和租约完成清理
pub(crate) async fn shutdown_managed_runtime(
    wing: RustWing,
    subscriber: Box<dyn ManagedNodeSubscriber>,
) -> Result<usize> {
    let core_result = wing.shutdown().await;
    let subscriber_result = subscriber.shutdown().await;
    combine_shutdown_results(core_result, subscriber_result)
}

// Preserve single failures and retain both errors when both shutdown stages fail 保留单侧错误并在双侧失败时汇总两项错误
fn combine_shutdown_results(
    core_result: Result<usize>,
    subscriber_result: Result<()>,
) -> Result<usize> {
    match (core_result, subscriber_result) {
        (Ok(sessions), Ok(())) => Ok(sessions),
        (Err(error), Ok(())) | (Ok(_), Err(error)) => Err(error),
        (Err(core_error), Err(subscriber_error)) => {
            Err(rust_wing_core::RustWingError::Cluster(format!(
                "core shutdown failed: {core_error}; subscriber shutdown failed: {subscriber_error}"
            )))
        }
    }
}

fn aggregate_runtime_status(
    core_status: RuntimeStatus,
    subscriber_status: &NodeSubscriberStatus,
) -> RuntimeStatus {
    match core_status {
        RuntimeStatus::Starting | RuntimeStatus::Stopping | RuntimeStatus::Stopped => core_status,
        RuntimeStatus::Degraded => RuntimeStatus::Degraded,
        RuntimeStatus::Running => match subscriber_status {
            NodeSubscriberStatus::Running => RuntimeStatus::Running,
            NodeSubscriberStatus::Starting => RuntimeStatus::Starting,
            NodeSubscriberStatus::Reconnecting
            | NodeSubscriberStatus::Failed(_)
            | NodeSubscriberStatus::Stopped => RuntimeStatus::Degraded,
        },
    }
}

struct DistributedTransportParts {
    publisher: DistributedNodePublisher,
    subscriber: Box<dyn NodeSubscriberAdapter>,
}

impl DistributedTransportParts {
    async fn connect(config: NodeTransportConfig) -> Result<Self> {
        match config {
            NodeTransportConfig::Redis(config) => {
                let publisher = RedisNodePublisherAdapter::connect(config.clone()).await?;
                let subscriber = RedisNodeSubscriberAdapter::connect(config).await?;
                Ok(Self {
                    publisher: DistributedNodePublisher::Redis(publisher),
                    subscriber: Box::new(subscriber),
                })
            }
            #[cfg(feature = "nats")]
            NodeTransportConfig::Nats(config) => {
                let publisher = NatsNodePublisherAdapter::connect(config.clone()).await?;
                let subscriber = NatsNodeSubscriberAdapter::connect(config).await?;
                Ok(Self {
                    publisher: DistributedNodePublisher::Nats(publisher),
                    subscriber: Box::new(subscriber),
                })
            }
            #[cfg(feature = "kafka")]
            NodeTransportConfig::Kafka(config) => {
                let publisher = KafkaNodePublisherAdapter::connect(config.clone())?;
                let subscriber = KafkaNodeSubscriberAdapter::connect(config)?;
                Ok(Self {
                    publisher: DistributedNodePublisher::Kafka(publisher),
                    subscriber: Box::new(subscriber),
                })
            }
        }
    }
}

#[derive(Clone)]
enum DistributedNodePublisher {
    Redis(RedisNodePublisherAdapter),
    #[cfg(feature = "nats")]
    Nats(NatsNodePublisherAdapter),
    #[cfg(feature = "kafka")]
    Kafka(KafkaNodePublisherAdapter),
}

#[async_trait]
impl NodePublisherAdapter for DistributedNodePublisher {
    async fn publish(&self, node_id: &NodeId, envelope: ClusterEnvelope) -> Result<()> {
        match self {
            Self::Redis(publisher) => publisher.publish(node_id, envelope).await,
            #[cfg(feature = "nats")]
            Self::Nats(publisher) => publisher.publish(node_id, envelope).await,
            #[cfg(feature = "kafka")]
            Self::Kafka(publisher) => publisher.publish(node_id, envelope).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use async_trait::async_trait;
    use rust_wing_core::{Result, RuntimeStatus, RustWing, RustWingConfig, RustWingError};

    use super::{
        DistributedRuntimeConfig, DistributedRustWing, NodeSubscriberStatsSnapshot,
        NodeSubscriberStatus, NodeTransportConfig,
    };
    use crate::{ManagedNodeSubscriber, RedisPresenceConfig, RedisPublisherConfig};

    struct TestSubscriber {
        stopped: Arc<AtomicBool>,
        core_drained: Arc<AtomicBool>,
        wing: RustWing,
        fail: bool,
        status: NodeSubscriberStatus,
    }

    #[async_trait]
    impl ManagedNodeSubscriber for TestSubscriber {
        fn status(&self) -> NodeSubscriberStatus {
            self.status.clone()
        }

        fn stats(&self) -> NodeSubscriberStatsSnapshot {
            NodeSubscriberStatsSnapshot::default()
        }

        async fn shutdown(self: Box<Self>) -> Result<()> {
            let core_drained = self.wing.runtime_status() == RuntimeStatus::Stopped
                && self.wing.connection_count().unwrap_or(usize::MAX) == 0;
            self.core_drained.store(core_drained, Ordering::SeqCst);
            self.stopped.store(true, Ordering::SeqCst);
            if self.fail {
                Err(RustWingError::Cluster(
                    "test subscriber shutdown failed".into(),
                ))
            } else {
                Ok(())
            }
        }
    }

    // Redis is available as the baseline distributed transport Redis 可作为基础分布式消息传输
    #[test]
    fn runtime_config_accepts_redis_transport() {
        let config = DistributedRuntimeConfig::new(
            RedisPresenceConfig::new("redis://127.0.0.1:6379"),
            RedisPublisherConfig::new("redis://127.0.0.1:6379"),
        );

        assert!(config.validate().is_ok());
        assert_eq!(config.transport.name(), "redis");
        assert!(matches!(config.transport, NodeTransportConfig::Redis(_)));
    }

    // NATS can be selected independently from Redis presence NATS 可独立于 Redis 在线路由进行选择
    #[cfg(feature = "nats")]
    #[test]
    fn runtime_config_accepts_nats_transport() {
        let config = DistributedRuntimeConfig::new(
            RedisPresenceConfig::new("redis://127.0.0.1:6379"),
            crate::NatsPublisherConfig::new("nats://127.0.0.1:4222"),
        );

        assert!(config.validate().is_ok());
        assert_eq!(config.transport.name(), "nats");
    }

    // Kafka can be selected independently from Redis presence Kafka 可独立于 Redis 在线路由进行选择
    #[cfg(feature = "kafka")]
    #[test]
    fn runtime_config_accepts_kafka_transport() {
        let config = DistributedRuntimeConfig::new(
            RedisPresenceConfig::new("redis://127.0.0.1:6379"),
            crate::KafkaPublisherConfig::new("127.0.0.1:9092"),
        );

        assert!(config.validate().is_ok());
        assert_eq!(config.transport.name(), "kafka");
    }

    // Shutdown always stops the subscriber and clears local sessions 关闭运行时始终停止订阅并清理本地会话
    #[tokio::test]
    async fn runtime_shutdown_stops_subscriber_and_core() {
        let wing = rust_wing_core::RustWing::new(RustWingConfig::default());
        let accepted = wing.accept_user("alice").await.unwrap();
        let stopped = Arc::new(AtomicBool::new(false));
        let core_drained = Arc::new(AtomicBool::new(false));
        let runtime = DistributedRustWing {
            wing: wing.clone(),
            subscriber: Box::new(TestSubscriber {
                stopped: stopped.clone(),
                core_drained: core_drained.clone(),
                wing: wing.clone(),
                fail: false,
                status: NodeSubscriberStatus::Running,
            }),
        };

        assert!(runtime.is_ready());
        assert_eq!(runtime.subscriber_status(), NodeSubscriberStatus::Running);
        assert_eq!(runtime.shutdown().await.unwrap(), 1);
        assert!(stopped.load(Ordering::SeqCst));
        assert!(core_drained.load(Ordering::SeqCst));
        assert!(accepted.session.is_closed());
        assert_eq!(wing.connection_count().unwrap(), 0);
    }

    // Core cleanup still runs when subscriber shutdown reports an error 订阅关闭报错时仍会执行核心状态清理
    #[tokio::test]
    async fn runtime_shutdown_cleans_core_after_subscriber_error() {
        let wing = rust_wing_core::RustWing::new(RustWingConfig::default());
        let accepted = wing.accept_user("alice").await.unwrap();
        let stopped = Arc::new(AtomicBool::new(false));
        let core_drained = Arc::new(AtomicBool::new(false));
        let runtime = DistributedRustWing {
            wing: wing.clone(),
            subscriber: Box::new(TestSubscriber {
                stopped: stopped.clone(),
                core_drained: core_drained.clone(),
                wing: wing.clone(),
                fail: true,
                status: NodeSubscriberStatus::Running,
            }),
        };

        assert!(runtime.shutdown().await.is_err());
        assert!(stopped.load(Ordering::SeqCst));
        assert!(core_drained.load(Ordering::SeqCst));
        assert!(accepted.session.is_closed());
        assert_eq!(wing.connection_count().unwrap(), 0);
    }

    // Distributed readiness combines core and subscriber health 分布式就绪状态会聚合核心与订阅器健康状态
    #[test]
    fn runtime_health_degrades_when_subscriber_fails() {
        let wing = rust_wing_core::RustWing::new(RustWingConfig::default());
        let subscriber_wing = wing.clone();
        let runtime = DistributedRustWing {
            wing,
            subscriber: Box::new(TestSubscriber {
                stopped: Arc::new(AtomicBool::new(false)),
                core_drained: Arc::new(AtomicBool::new(false)),
                wing: subscriber_wing,
                fail: false,
                status: NodeSubscriberStatus::Failed("subscriber failed".into()),
            }),
        };

        let health = runtime.health();
        assert_eq!(health.status, RuntimeStatus::Degraded);
        assert_eq!(health.core.status, RuntimeStatus::Running);
        assert!(matches!(
            health.subscriber,
            NodeSubscriberStatus::Failed(message) if message == "subscriber failed"
        ));
        assert!(!runtime.is_ready());
    }

    // Dual shutdown failures retain both causes 双侧关闭失败会保留两项错误原因
    #[test]
    fn shutdown_result_combines_core_and_subscriber_failures() {
        let result = super::combine_shutdown_results(
            Err(RustWingError::Cluster("core failed".into())),
            Err(RustWingError::Cluster("subscriber failed".into())),
        );

        let error = result.unwrap_err().to_string();
        assert!(error.contains("core failed"));
        assert!(error.contains("subscriber failed"));
    }
}

// Built-in memory adapters 内置内存适配器
pub mod memory;
// Kafka external message adapters Kafka 外部消息适配器
#[cfg(feature = "kafka")]
pub mod kafka;
// External broker message helpers 外部消息组件辅助工具
pub mod messaging;
// NATS external message adapters NATS 外部消息适配器
#[cfg(feature = "nats")]
pub mod nats;
// Presence adapter contracts 在线路由适配器契约
pub mod presence;
// Node publisher adapter contracts 节点发布适配器契约
pub mod publisher;
// Redis adapters Redis 适配器
#[cfg(feature = "redis")]
pub mod redis;
// Managed distributed runtime 托管的分布式运行时
#[cfg(feature = "redis")]
pub mod runtime;
// Managed node subscriber contracts 托管节点订阅器契约
pub mod subscriber;

#[cfg(feature = "kafka")]
pub use kafka::{
    KafkaExternalMessageConsumerConfig, KafkaExternalMessageConsumerHandle,
    KafkaNodePublisherAdapter, KafkaNodeSubscriberAdapter, KafkaNodeSubscriberHandle,
    KafkaPublisherConfig, spawn_kafka_external_message_consumer,
};
pub use memory::MemoryPresenceAdapter;
pub use messaging::{
    ExternalMessage, ExternalMessageConsumerStats, ExternalMessageConsumerStatsSnapshot,
    ExternalMessagePayload, ExternalMessageTarget, deliver_external_message,
    external_message_from_json, process_external_message_payload,
};
#[cfg(feature = "nats")]
pub use nats::{
    NatsExternalMessageConsumerConfig, NatsExternalMessageConsumerHandle, NatsNodePublisherAdapter,
    NatsNodeSubscriberAdapter, NatsNodeSubscriberHandle, NatsPublisherConfig,
    spawn_nats_external_message_consumer,
};
pub use presence::{PresenceStoreAdapter, PresenceStoreBridge};
pub use publisher::{NodePublisherAdapter, NodePublisherBridge};
#[cfg(feature = "redis")]
pub use redis::{
    RedisClusterParts, RedisDeployment, RedisNodePublisherAdapter, RedisNodeSubscriberAdapter,
    RedisNodeSubscriberHandle, RedisPresenceAdapter, RedisPresenceConfig, RedisPublisherConfig,
    RedisRustWing, RedisSentinelConfig, redis_cluster_from_config, redis_cluster_parts_from_config,
    redis_rust_wing_from_config, redis_rust_wing_from_parts,
};
#[cfg(feature = "redis")]
pub use runtime::{
    DistributedRuntimeConfig, DistributedRuntimeHealth, DistributedRustWing, NodeTransportConfig,
};
pub use subscriber::{
    ManagedNodeSubscriber, NodeSubscriberAdapter, NodeSubscriberStats, NodeSubscriberStatsSnapshot,
    NodeSubscriberStatus,
};

// Build a RustWing cluster from adapter implementations 从适配器实现构建集群依赖
pub fn cluster_from_adapters<P, N>(presence: P, publisher: N) -> rust_wing_core::Cluster
where
    P: PresenceStoreAdapter + 'static,
    N: NodePublisherAdapter + 'static,
{
    rust_wing_core::Cluster::new(
        PresenceStoreBridge::new(presence),
        NodePublisherBridge::new(publisher),
    )
}

// Build a RustWing manager from freely composed cluster adapters 从自由组合的集群适配器构建 RustWing 管理器
pub async fn rust_wing_from_adapters<P, N>(
    mut config: rust_wing_core::RustWingConfig,
    presence: P,
    publisher: N,
) -> rust_wing_core::Result<rust_wing_core::RustWing>
where
    P: PresenceStoreAdapter + 'static,
    N: NodePublisherAdapter + 'static,
{
    config.cluster.enabled = true;
    rust_wing_core::RustWing::with_cluster_checked(
        config,
        Some(cluster_from_adapters(presence, publisher)),
    )
    .await
}

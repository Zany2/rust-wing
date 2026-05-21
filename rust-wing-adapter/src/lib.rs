// Built-in memory adapters 内置内存适配器
pub mod memory;
// Presence adapter contracts 在线路由适配器契约
pub mod presence;
// Node publisher adapter contracts 节点发布适配器契约
pub mod publisher;
// Redis adapters Redis 适配器
#[cfg(feature = "redis")]
pub mod redis;

pub use memory::MemoryPresenceAdapter;
pub use presence::{PresenceStoreAdapter, PresenceStoreBridge};
pub use publisher::{NodePublisherAdapter, NodePublisherBridge};
#[cfg(feature = "redis")]
pub use redis::{
    RedisNodePublisherAdapter, RedisPresenceAdapter, RedisPresenceConfig, RedisPublisherConfig,
    redis_cluster_from_config,
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

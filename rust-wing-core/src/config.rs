use std::time::Duration;

use crate::identity::NodeId;

// Default local node identifier 默认本地节点标识
const DEFAULT_NODE_ID: &str = "local";
// Default heartbeat send interval 默认心跳发送间隔
const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);
// Default heartbeat timeout window 默认心跳超时时间
const DEFAULT_HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(45);
// Default outbound queue size 默认写队列容量
const DEFAULT_WRITE_QUEUE_CAPACITY: usize = 64;
// Default cluster route lifetime 默认集群路由有效期
const DEFAULT_ROUTE_TTL: Duration = Duration::from_secs(90);

// Connection replacement strategy 连接替换策略
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionPolicy {
    // Keep only one session per user 每个用户仅保留一个会话
    Single,
    // Allow multiple sessions per user 允许每个用户保留多个会话
    Multi,
}

// Cluster backend selection 集群后端选择
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClusterBackendConfig {
    // Use the built-in in-memory backend 使用内置内存后端
    Memory,
    // Use Redis through the configured connection URL 使用配置的连接地址访问 Redis
    Redis {
        // Redis connection URL Redis 连接地址
        url: String,
    },
}

// Top-level runtime configuration 顶层运行配置
#[derive(Debug, Clone)]
pub struct RustWingConfig {
    // Current node identifier 当前节点标识
    pub node_id: NodeId,
    // Interval between heartbeat checks 心跳检查间隔
    pub heartbeat_interval: Duration,
    // Maximum tolerated heartbeat silence 心跳最大静默时间
    pub heartbeat_timeout: Duration,
    // Per-session outbound queue capacity 每个会话的写队列容量
    pub write_queue_capacity: usize,
    // Session coexistence policy 会话共存策略
    pub connection_policy: ConnectionPolicy,
    // Cluster-related configuration 集群相关配置
    pub cluster: ClusterConfig,
}

// Cluster runtime configuration 集群运行配置
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    // Whether cluster routing is enabled 是否启用集群路由
    pub enabled: bool,
    // Cluster backend implementation 集群后端实现
    pub backend: ClusterBackendConfig,
    // Route expiration duration 路由过期时长
    pub route_ttl: Duration,
}

impl Default for RustWingConfig {
    // Build default runtime settings 构建默认运行配置
    fn default() -> Self {
        Self {
            node_id: NodeId::from(DEFAULT_NODE_ID),
            heartbeat_interval: DEFAULT_HEARTBEAT_INTERVAL,
            heartbeat_timeout: DEFAULT_HEARTBEAT_TIMEOUT,
            write_queue_capacity: DEFAULT_WRITE_QUEUE_CAPACITY,
            connection_policy: ConnectionPolicy::Single,
            cluster: ClusterConfig::default(),
        }
    }
}

impl Default for ClusterConfig {
    // Build default cluster settings 构建默认集群配置
    fn default() -> Self {
        Self {
            enabled: false,
            backend: ClusterBackendConfig::default(),
            route_ttl: DEFAULT_ROUTE_TTL,
        }
    }
}

impl Default for ClusterBackendConfig {
    // Use the in-memory backend by default 默认使用内存后端
    fn default() -> Self {
        Self::Memory
    }
}

impl RustWingConfig {
    // Replace invalid zero or empty values with defaults 使用默认值替换无效配置
    pub fn normalized(mut self) -> Self {
        // Ensure node routing always has an identifier 确保节点路由始终具备标识
        if self.node_id.as_str().is_empty() {
            self.node_id = NodeId::from(DEFAULT_NODE_ID);
        }
        // Restore the heartbeat interval when disabled accidentally 当心跳间隔被意外置零时恢复默认值
        if self.heartbeat_interval.is_zero() {
            self.heartbeat_interval = DEFAULT_HEARTBEAT_INTERVAL;
        }
        // Restore the heartbeat timeout when disabled accidentally 当心跳超时被意外置零时恢复默认值
        if self.heartbeat_timeout.is_zero() {
            self.heartbeat_timeout = DEFAULT_HEARTBEAT_TIMEOUT;
        }
        // Keep every session queue usable 保持每个会话队列可用
        if self.write_queue_capacity == 0 {
            self.write_queue_capacity = DEFAULT_WRITE_QUEUE_CAPACITY;
        }
        // Keep cluster routes from expiring immediately 避免集群路由立即过期
        if self.cluster.route_ttl.is_zero() {
            self.cluster.route_ttl = DEFAULT_ROUTE_TTL;
        }
        // Return the normalized configuration 返回归一化后的配置
        self
    }
}

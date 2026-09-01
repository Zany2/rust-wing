use std::time::Duration;

use std::collections::HashMap;

use crate::error::{Result, RustWingError};
use crate::identity::{ConnectionType, NodeId};

// Environment variable used to override the node id 覆盖节点标识使用的环境变量
pub const RUST_WING_NODE_ID_ENV: &str = "RUST_WING_NODE_ID";

// Default heartbeat send interval 默认心跳发送间隔
const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);
// Default heartbeat timeout window 默认心跳超时时间
const DEFAULT_HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(90);
// Default outbound queue size 默认写队列容量
const DEFAULT_WRITE_QUEUE_CAPACITY: usize = 64;
// Default session lifecycle event channel capacity 默认会话生命周期事件通道容量
const DEFAULT_SESSION_EVENT_CAPACITY: usize = 256;
// Default cluster route lifetime 默认集群路由有效期
const DEFAULT_ROUTE_TTL: Duration = Duration::from_secs(90);
// Default cluster node lease lifetime 默认集群节点租约有效期
const DEFAULT_NODE_LEASE_TTL: Duration = Duration::from_secs(30);
// Default maintenance scan interval 默认后台维护扫描间隔
const DEFAULT_MAINTENANCE_INTERVAL: Duration = Duration::from_secs(15);
// Default timeout after a liveness probe 默认存活探测超时时间
const DEFAULT_PROBE_TIMEOUT: Duration = Duration::from_secs(10);
// Default maximum removals per maintenance tick 默认每轮维护最多清理数量
const DEFAULT_MAX_CLEANUP_PER_TICK: usize = 1024;
// Default maximum liveness probes per maintenance tick 默认每轮维护最多探测数量
const DEFAULT_MAX_PROBE_PER_TICK: usize = 4096;

// Connection replacement strategy 连接替换策略
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionPolicy {
    // Keep only one session per user in a connection system 每个连接体系内每个用户仅保留一个会话
    UniqueUser,
    // Keep only one session per user-client pair in a connection system 每个连接体系内每个用户客户端组合仅保留一个会话
    UniqueClient,
    // Allow repeated sessions for the same user and client 允许同一用户与客户端保留多个会话
    MultiSession,
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
    // Session lifecycle event channel capacity 会话生命周期事件通道容量
    pub session_event_capacity: usize,
    // Default session coexistence policy 默认会话共存策略
    pub default_connection_policy: ConnectionPolicy,
    // Per-connection-system policy overrides 连接体系级策略覆盖
    pub connection_policies: HashMap<ConnectionType, ConnectionPolicy>,
    // Background maintenance configuration 后台维护配置
    pub maintenance: MaintenanceConfig,
    // Cluster-related configuration 集群相关配置
    pub cluster: ClusterConfig,
}

// Background maintenance configuration 后台维护配置
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaintenanceConfig {
    // Whether the managed maintenance task is enabled 是否启用托管维护任务
    pub enabled: bool,
    // Interval between maintenance scans 维护扫描间隔
    pub interval: Duration,
    // Time to wait after sending a liveness probe 发送存活探测后的等待时间
    pub probe_timeout: Duration,
    // Maximum inactive sessions removed during one maintenance tick 单轮维护最多清理的失活会话数
    pub max_cleanup_per_tick: usize,
    // Maximum liveness probes sent during one maintenance tick 单轮维护最多发送的存活探测数
    pub max_probe_per_tick: usize,
}

// Cluster runtime configuration 集群运行配置
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    // Whether cluster routing is enabled 是否启用集群路由
    pub enabled: bool,
    // Route expiration duration 路由过期时长
    pub route_ttl: Duration,
    // Node lease expiration duration 节点租约过期时长
    pub node_lease_ttl: Duration,
}

impl Default for RustWingConfig {
    // Build default runtime settings 构建默认运行配置
    fn default() -> Self {
        Self {
            node_id: NodeId::generate(),
            heartbeat_interval: DEFAULT_HEARTBEAT_INTERVAL,
            heartbeat_timeout: DEFAULT_HEARTBEAT_TIMEOUT,
            write_queue_capacity: DEFAULT_WRITE_QUEUE_CAPACITY,
            session_event_capacity: DEFAULT_SESSION_EVENT_CAPACITY,
            default_connection_policy: ConnectionPolicy::UniqueClient,
            connection_policies: HashMap::new(),
            maintenance: MaintenanceConfig::default(),
            cluster: ClusterConfig::default(),
        }
    }
}

impl Default for MaintenanceConfig {
    // Build default maintenance settings 构建默认维护配置
    fn default() -> Self {
        Self {
            enabled: true,
            interval: DEFAULT_MAINTENANCE_INTERVAL,
            probe_timeout: DEFAULT_PROBE_TIMEOUT,
            max_cleanup_per_tick: DEFAULT_MAX_CLEANUP_PER_TICK,
            max_probe_per_tick: DEFAULT_MAX_PROBE_PER_TICK,
        }
    }
}

impl Default for ClusterConfig {
    // Build default cluster settings 构建默认集群配置
    fn default() -> Self {
        Self {
            enabled: false,
            route_ttl: DEFAULT_ROUTE_TTL,
            node_lease_ttl: DEFAULT_NODE_LEASE_TTL,
        }
    }
}

impl ClusterConfig {
    // Enable or disable cluster routing 启用或关闭集群路由
    pub fn with_enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    // Set the distributed route lifetime 设置分布式路由生命周期
    pub fn with_route_ttl(mut self, route_ttl: Duration) -> Self {
        self.route_ttl = route_ttl;
        self
    }

    // Set the distributed node lease lifetime 设置分布式节点租约生命周期
    pub fn with_node_lease_ttl(mut self, node_lease_ttl: Duration) -> Self {
        self.node_lease_ttl = node_lease_ttl;
        self
    }
}

impl MaintenanceConfig {
    // Enable or disable background maintenance 启用或关闭后台维护
    pub fn with_enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    // Set the background maintenance scan interval 设置后台维护扫描间隔
    pub fn with_interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }

    // Set the liveness probe timeout 设置存活探测超时时间
    pub fn with_probe_timeout(mut self, probe_timeout: Duration) -> Self {
        self.probe_timeout = probe_timeout;
        self
    }

    // Set the maximum removals per maintenance tick 设置单轮维护最多清理数量
    pub fn with_max_cleanup_per_tick(mut self, max_cleanup_per_tick: usize) -> Self {
        self.max_cleanup_per_tick = max_cleanup_per_tick;
        self
    }

    // Set the maximum liveness probes per maintenance tick 设置单轮维护最多探测数量
    pub fn with_max_probe_per_tick(mut self, max_probe_per_tick: usize) -> Self {
        self.max_probe_per_tick = max_probe_per_tick;
        self
    }
}

impl RustWingConfig {
    // Build default settings and apply supported environment overrides 构建默认配置并应用支持的环境覆盖
    pub fn from_env() -> Self {
        Self::default().with_node_id_from_env()
    }

    // Set the current node identifier 设置当前节点标识
    pub fn with_node_id(mut self, node_id: impl Into<NodeId>) -> Self {
        self.node_id = node_id.into();
        self
    }

    // Set the node identifier from the configured environment variable 从配置的环境变量设置节点标识
    pub fn with_node_id_from_env(mut self) -> Self {
        if let Some(node_id) = node_id_from_env_value(std::env::var(RUST_WING_NODE_ID_ENV).ok()) {
            self.node_id = node_id;
        }
        self
    }

    // Set the default session policy 设置默认会话策略
    pub fn with_default_connection_policy(mut self, policy: ConnectionPolicy) -> Self {
        self.default_connection_policy = policy;
        self
    }

    // Set the heartbeat interval reported to clients 设置返回给客户端的心跳间隔
    pub fn with_heartbeat_interval(mut self, heartbeat_interval: Duration) -> Self {
        self.heartbeat_interval = heartbeat_interval;
        self
    }

    // Set the inactivity timeout used to reap stale sessions 设置用于回收失活会话的不活跃超时
    pub fn with_heartbeat_timeout(mut self, heartbeat_timeout: Duration) -> Self {
        self.heartbeat_timeout = heartbeat_timeout;
        self
    }

    // Set the outbound queue capacity for each session 设置每个会话的出站队列容量
    pub fn with_write_queue_capacity(mut self, write_queue_capacity: usize) -> Self {
        self.write_queue_capacity = write_queue_capacity;
        self
    }

    // Set the bounded session lifecycle event channel capacity 设置有界会话生命周期事件通道容量
    pub fn with_session_event_capacity(mut self, session_event_capacity: usize) -> Self {
        self.session_event_capacity = session_event_capacity;
        self
    }

    // Replace the full maintenance configuration 替换完整维护配置
    pub fn with_maintenance(mut self, maintenance: MaintenanceConfig) -> Self {
        self.maintenance = maintenance;
        self
    }

    // Enable or disable managed maintenance 启用或关闭托管维护
    pub fn with_maintenance_enabled(mut self, enabled: bool) -> Self {
        self.maintenance.enabled = enabled;
        self
    }

    // Set the managed maintenance scan interval 设置托管维护扫描间隔
    pub fn with_maintenance_interval(mut self, interval: Duration) -> Self {
        self.maintenance.interval = interval;
        self
    }

    // Set the managed maintenance probe timeout 设置托管维护探测超时时间
    pub fn with_maintenance_probe_timeout(mut self, probe_timeout: Duration) -> Self {
        self.maintenance.probe_timeout = probe_timeout;
        self
    }

    // Set the managed maintenance cleanup limit 设置托管维护单轮清理上限
    pub fn with_maintenance_max_cleanup_per_tick(mut self, max_cleanup_per_tick: usize) -> Self {
        self.maintenance.max_cleanup_per_tick = max_cleanup_per_tick;
        self
    }

    // Set the managed maintenance probe limit 设置托管维护单轮探测上限
    pub fn with_maintenance_max_probe_per_tick(mut self, max_probe_per_tick: usize) -> Self {
        self.maintenance.max_probe_per_tick = max_probe_per_tick;
        self
    }

    // Replace the full cluster configuration 替换完整集群配置
    pub fn with_cluster(mut self, cluster: ClusterConfig) -> Self {
        self.cluster = cluster;
        self
    }

    // Enable or disable cluster routing 启用或关闭集群路由
    pub fn with_cluster_enabled(mut self, enabled: bool) -> Self {
        self.cluster.enabled = enabled;
        self
    }

    // Set the distributed route lifetime 设置分布式路由生命周期
    pub fn with_cluster_route_ttl(mut self, route_ttl: Duration) -> Self {
        self.cluster.route_ttl = route_ttl;
        self
    }

    // Set the distributed node lease lifetime 设置分布式节点租约生命周期
    pub fn with_cluster_node_lease_ttl(mut self, node_lease_ttl: Duration) -> Self {
        self.cluster.node_lease_ttl = node_lease_ttl;
        self
    }

    // Set the session policy for one connection system 设置某个连接体系的会话策略
    pub fn with_connection_policy(
        mut self,
        connection_type: impl Into<ConnectionType>,
        policy: ConnectionPolicy,
    ) -> Self {
        self.connection_policies
            .insert(connection_type.into(), policy);
        self
    }

    // Resolve the session policy for one connection system 解析某个连接体系的会话策略
    pub fn policy_for(&self, connection_type: &ConnectionType) -> ConnectionPolicy {
        self.connection_policies
            .get(connection_type)
            .copied()
            .unwrap_or(self.default_connection_policy)
    }

    // Validate configuration relationships that normalization cannot infer 校验归一化无法推断的配置关系
    pub fn validate(&self) -> Result<()> {
        if self.node_id.as_str().trim().is_empty() {
            return Err(RustWingError::InvalidConfig(
                "node_id cannot be empty".into(),
            ));
        }
        if self.heartbeat_interval.is_zero() {
            return Err(RustWingError::InvalidConfig(
                "heartbeat_interval cannot be zero".into(),
            ));
        }
        if self.heartbeat_timeout.is_zero() {
            return Err(RustWingError::InvalidConfig(
                "heartbeat_timeout cannot be zero".into(),
            ));
        }
        if self.heartbeat_timeout <= self.heartbeat_interval {
            return Err(RustWingError::InvalidConfig(
                "heartbeat_timeout must be greater than heartbeat_interval".into(),
            ));
        }
        if self.write_queue_capacity == 0 {
            return Err(RustWingError::InvalidConfig(
                "write_queue_capacity cannot be zero".into(),
            ));
        }
        if self.session_event_capacity == 0 {
            return Err(RustWingError::InvalidConfig(
                "session_event_capacity cannot be zero".into(),
            ));
        }
        if self.maintenance.enabled && self.maintenance.interval.is_zero() {
            return Err(RustWingError::InvalidConfig(
                "maintenance interval cannot be zero".into(),
            ));
        }
        if self.maintenance.enabled && self.maintenance.probe_timeout.is_zero() {
            return Err(RustWingError::InvalidConfig(
                "maintenance probe_timeout cannot be zero".into(),
            ));
        }
        if self.maintenance.enabled && self.maintenance.max_cleanup_per_tick == 0 {
            return Err(RustWingError::InvalidConfig(
                "maintenance max_cleanup_per_tick cannot be zero".into(),
            ));
        }
        if self.maintenance.enabled && self.maintenance.max_probe_per_tick == 0 {
            return Err(RustWingError::InvalidConfig(
                "maintenance max_probe_per_tick cannot be zero".into(),
            ));
        }
        if self.cluster.route_ttl.is_zero() {
            return Err(RustWingError::InvalidConfig(
                "cluster route_ttl cannot be zero".into(),
            ));
        }
        if self.cluster.node_lease_ttl.is_zero() {
            return Err(RustWingError::InvalidConfig(
                "cluster node_lease_ttl cannot be zero".into(),
            ));
        }
        if self.cluster.enabled && self.cluster.route_ttl <= self.heartbeat_interval {
            return Err(RustWingError::InvalidConfig(
                "cluster route_ttl must be greater than heartbeat_interval".into(),
            ));
        }
        Ok(())
    }

    // Replace invalid zero or empty values with defaults 使用默认值替换无效配置
    pub fn normalized(mut self) -> Self {
        // Generate a node identifier when the configured value is empty 配置值为空时生成节点标识
        if self.node_id.as_str().is_empty() {
            self.node_id = NodeId::generate();
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
        // Keep lifecycle notifications bounded but usable 保持生命周期通知有界且可用
        if self.session_event_capacity == 0 {
            self.session_event_capacity = DEFAULT_SESSION_EVENT_CAPACITY;
        }
        // Keep enabled maintenance from busy-looping 避免启用的维护任务忙循环
        if self.maintenance.interval.is_zero() {
            self.maintenance.interval = DEFAULT_MAINTENANCE_INTERVAL;
        }
        // Keep enabled liveness probes from expiring immediately 避免启用的存活探测立即过期
        if self.maintenance.probe_timeout.is_zero() {
            self.maintenance.probe_timeout = DEFAULT_PROBE_TIMEOUT;
        }
        // Keep maintenance cleanup bounded but usable 保持维护清理有界且可用
        if self.maintenance.max_cleanup_per_tick == 0 {
            self.maintenance.max_cleanup_per_tick = DEFAULT_MAX_CLEANUP_PER_TICK;
        }
        // Keep maintenance probing bounded but usable 保持维护探测有界且可用
        if self.maintenance.max_probe_per_tick == 0 {
            self.maintenance.max_probe_per_tick = DEFAULT_MAX_PROBE_PER_TICK;
        }
        // Keep cluster routes from expiring immediately 避免集群路由立即过期
        if self.cluster.route_ttl.is_zero() {
            self.cluster.route_ttl = DEFAULT_ROUTE_TTL;
        }
        // Keep node leases from expiring immediately 避免节点租约立即过期
        if self.cluster.node_lease_ttl.is_zero() {
            self.cluster.node_lease_ttl = DEFAULT_NODE_LEASE_TTL;
        }
        // Return the normalized configuration 返回归一化后的配置
        self
    }
}

// Parse a node id from an optional environment value 从可选环境变量值解析节点标识
fn node_id_from_env_value(value: Option<String>) -> Option<NodeId> {
    value
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .map(NodeId::from)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Default configurations receive distinct generated node ids 默认配置会获得不同的自动生成节点标识
    #[test]
    fn default_generates_distinct_node_ids() {
        let first = RustWingConfig::default();
        let second = RustWingConfig::default();

        assert!(first.node_id.as_str().starts_with("node-"));
        assert_eq!(first.node_id.as_str().len(), "node-".len() + 32);
        assert_ne!(first.node_id, second.node_id);
    }

    // An explicit node id replaces the generated default 显式节点标识会覆盖自动生成的默认值
    #[test]
    fn node_id_builder_overrides_generated_default() {
        let config = RustWingConfig::default().with_node_id("node-a");

        assert_eq!(config.node_id, NodeId::from("node-a"));
    }

    // Normalization replaces an empty node id with a generated value 归一化会用自动生成值替换空节点标识
    #[test]
    fn normalized_generates_empty_node_id() {
        let config = RustWingConfig::default().with_node_id("").normalized();

        assert!(config.node_id.as_str().starts_with("node-"));
        assert_eq!(config.node_id.as_str().len(), "node-".len() + 32);
    }

    // Default heartbeat settings favor general application workloads 默认心跳配置偏向通用应用负载
    #[test]
    fn default_heartbeat_settings_are_general_purpose() {
        let config = RustWingConfig::default();

        assert_eq!(config.heartbeat_interval, Duration::from_secs(30));
        assert_eq!(config.heartbeat_timeout, Duration::from_secs(90));
        assert!(config.maintenance.enabled);
        assert_eq!(config.maintenance.interval, Duration::from_secs(15));
        assert_eq!(config.maintenance.probe_timeout, Duration::from_secs(10));
        assert_eq!(config.maintenance.max_cleanup_per_tick, 1024);
        assert_eq!(config.maintenance.max_probe_per_tick, 4096);
    }

    // Heartbeat builders override the default timings 心跳构建方法会覆盖默认时间
    #[test]
    fn heartbeat_builders_override_defaults() {
        let config = RustWingConfig::default()
            .with_heartbeat_interval(Duration::from_secs(10))
            .with_heartbeat_timeout(Duration::from_secs(25));

        assert_eq!(config.heartbeat_interval, Duration::from_secs(10));
        assert_eq!(config.heartbeat_timeout, Duration::from_secs(25));
    }

    // Normalization restores zero heartbeat durations 归一化会恢复零值心跳时长
    #[test]
    fn normalized_restores_zero_heartbeat_settings() {
        let config = RustWingConfig::default()
            .with_heartbeat_interval(Duration::ZERO)
            .with_heartbeat_timeout(Duration::ZERO)
            .normalized();

        assert_eq!(config.heartbeat_interval, Duration::from_secs(30));
        assert_eq!(config.heartbeat_timeout, Duration::from_secs(90));
    }

    // Runtime builders update queue and cluster settings 运行配置构建方法会更新队列与集群设置
    #[test]
    fn runtime_builders_update_queue_and_cluster_settings() {
        let config = RustWingConfig::default()
            .with_write_queue_capacity(128)
            .with_maintenance_interval(Duration::from_secs(5))
            .with_maintenance_probe_timeout(Duration::from_secs(2))
            .with_maintenance_max_cleanup_per_tick(10)
            .with_maintenance_max_probe_per_tick(20)
            .with_cluster_enabled(true)
            .with_cluster_route_ttl(Duration::from_secs(120))
            .with_cluster_node_lease_ttl(Duration::from_secs(40));

        assert_eq!(config.write_queue_capacity, 128);
        assert_eq!(config.maintenance.interval, Duration::from_secs(5));
        assert_eq!(config.maintenance.probe_timeout, Duration::from_secs(2));
        assert_eq!(config.maintenance.max_cleanup_per_tick, 10);
        assert_eq!(config.maintenance.max_probe_per_tick, 20);
        assert!(config.cluster.enabled);
        assert_eq!(config.cluster.route_ttl, Duration::from_secs(120));
        assert_eq!(config.cluster.node_lease_ttl, Duration::from_secs(40));
    }

    // Cluster config builders can be composed independently 集群配置构建方法可以独立组合
    #[test]
    fn cluster_config_builders_compose_independently() {
        let cluster = ClusterConfig::default()
            .with_enabled(true)
            .with_route_ttl(Duration::from_secs(120))
            .with_node_lease_ttl(Duration::from_secs(40));

        assert!(cluster.enabled);
        assert_eq!(cluster.route_ttl, Duration::from_secs(120));
        assert_eq!(cluster.node_lease_ttl, Duration::from_secs(40));
    }

    // Validation rejects impossible heartbeat timing 校验会拒绝不可能的心跳时间关系
    #[test]
    fn validate_rejects_timeout_not_greater_than_interval() {
        let config = RustWingConfig::default()
            .with_heartbeat_interval(Duration::from_secs(30))
            .with_heartbeat_timeout(Duration::from_secs(30));

        assert!(matches!(
            config.validate(),
            Err(RustWingError::InvalidConfig(message))
                if message.contains("heartbeat_timeout")
        ));
    }

    // Validation rejects cluster route TTLs shorter than heartbeat intervals 校验会拒绝短于心跳间隔的集群路由 TTL
    #[test]
    fn validate_rejects_cluster_route_ttl_not_greater_than_heartbeat_interval() {
        let config = RustWingConfig::default()
            .with_cluster_enabled(true)
            .with_cluster_route_ttl(Duration::from_secs(30));

        assert!(matches!(
            config.validate(),
            Err(RustWingError::InvalidConfig(message))
                if message.contains("route_ttl")
        ));
    }

    // Validation rejects zero enabled maintenance intervals 校验会拒绝启用维护时的零扫描间隔
    #[test]
    fn validate_rejects_zero_enabled_maintenance_interval() {
        let config = RustWingConfig::default().with_maintenance_interval(Duration::ZERO);

        assert!(matches!(
            config.validate(),
            Err(RustWingError::InvalidConfig(message))
                if message.contains("maintenance interval")
        ));
    }

    // Validation rejects zero enabled maintenance probe timeouts 校验会拒绝启用维护时的零探测超时
    #[test]
    fn validate_rejects_zero_enabled_probe_timeout() {
        let config = RustWingConfig::default().with_maintenance_probe_timeout(Duration::ZERO);

        assert!(matches!(
            config.validate(),
            Err(RustWingError::InvalidConfig(message))
                if message.contains("probe_timeout")
        ));
    }

    // Validation rejects zero enabled maintenance cleanup limits 校验会拒绝启用维护时的零清理上限
    #[test]
    fn validate_rejects_zero_enabled_cleanup_limit() {
        let config = RustWingConfig::default().with_maintenance_max_cleanup_per_tick(0);

        assert!(matches!(
            config.validate(),
            Err(RustWingError::InvalidConfig(message))
                if message.contains("max_cleanup_per_tick")
        ));
    }

    // Validation rejects zero enabled maintenance probe limits 校验会拒绝启用维护时的零探测上限
    #[test]
    fn validate_rejects_zero_enabled_probe_limit() {
        let config = RustWingConfig::default().with_maintenance_max_probe_per_tick(0);

        assert!(matches!(
            config.validate(),
            Err(RustWingError::InvalidConfig(message))
                if message.contains("max_probe_per_tick")
        ));
    }

    // Node id environment parsing ignores missing values 节点标识环境解析会忽略缺失值
    #[test]
    fn node_id_env_ignores_missing_value() {
        assert_eq!(node_id_from_env_value(None), None);
    }

    // Node id environment parsing ignores blank values 节点标识环境解析会忽略空白值
    #[test]
    fn node_id_env_ignores_blank_value() {
        assert_eq!(node_id_from_env_value(Some("  ".into())), None);
    }

    // Node id environment parsing trims usable values 节点标识环境解析会裁剪可用值
    #[test]
    fn node_id_env_trims_usable_value() {
        assert_eq!(
            node_id_from_env_value(Some("  ws-1  ".into())),
            Some(NodeId::from("ws-1"))
        );
    }
}

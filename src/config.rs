use std::time::Duration;

use crate::identity::NodeId;

const DEFAULT_NODE_ID: &str = "local";
const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);
const DEFAULT_HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(45);
const DEFAULT_WRITE_QUEUE_CAPACITY: usize = 64;
const DEFAULT_ROUTE_TTL: Duration = Duration::from_secs(90);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionPolicy {
    Single,
    Multi,
}

#[derive(Debug, Clone)]
pub struct RustWingConfig {
    pub node_id: NodeId,
    pub heartbeat_interval: Duration,
    pub heartbeat_timeout: Duration,
    pub write_queue_capacity: usize,
    pub connection_policy: ConnectionPolicy,
    pub cluster: ClusterConfig,
}

#[derive(Debug, Clone)]
pub struct ClusterConfig {
    pub enabled: bool,
    pub route_ttl: Duration,
}

impl Default for RustWingConfig {
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
    fn default() -> Self {
        Self {
            enabled: false,
            route_ttl: DEFAULT_ROUTE_TTL,
        }
    }
}

impl RustWingConfig {
    pub fn normalized(mut self) -> Self {
        if self.node_id.as_str().is_empty() {
            self.node_id = NodeId::from(DEFAULT_NODE_ID);
        }
        if self.heartbeat_interval.is_zero() {
            self.heartbeat_interval = DEFAULT_HEARTBEAT_INTERVAL;
        }
        if self.heartbeat_timeout.is_zero() {
            self.heartbeat_timeout = DEFAULT_HEARTBEAT_TIMEOUT;
        }
        if self.write_queue_capacity == 0 {
            self.write_queue_capacity = DEFAULT_WRITE_QUEUE_CAPACITY;
        }
        if self.cluster.route_ttl.is_zero() {
            self.cluster.route_ttl = DEFAULT_ROUTE_TTL;
        }
        self
    }
}

use std::time::Duration;

use async_trait::async_trait;
use futures_util::StreamExt;
use redis::AsyncCommands;
use redis::aio::ConnectionManager;
use rust_wing_core::{
    Cluster, ClusterEnvelope, NodeId, Result, Route, RustWing, RustWingError, SessionId, UserId,
};

use crate::{NodePublisherAdapter, PresenceStoreAdapter, cluster_from_adapters};

// Redis presence adapter configuration Redis 在线路由适配器配置
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisPresenceConfig {
    // Redis connection URL Redis 连接地址
    pub url: String,
    // Key prefix shared by all presence keys 所有在线路由 key 的统一前缀
    pub key_prefix: String,
}

// Redis node publisher adapter configuration Redis 节点发布适配器配置
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisPublisherConfig {
    // Redis connection URL Redis 连接地址
    pub url: String,
    // Channel prefix shared by all node channels 所有节点频道的统一前缀
    pub channel_prefix: String,
}

impl RedisPresenceConfig {
    // Create Redis presence configuration 创建 Redis 在线路由配置
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            key_prefix: "rust-wing".into(),
        }
    }

    // Override the Redis key prefix 覆盖 Redis key 前缀
    pub fn with_key_prefix(mut self, key_prefix: impl Into<String>) -> Self {
        self.key_prefix = key_prefix.into();
        self
    }

    // Validate required Redis configuration 校验必填 Redis 配置
    pub fn validate(&self) -> Result<()> {
        // Require an explicit URL so startup failures are clear 要求显式地址以便启动失败清晰
        if self.url.trim().is_empty() {
            return Err(RustWingError::InvalidConfig(
                "redis presence url cannot be empty".into(),
            ));
        }
        // Keep generated Redis keys namespaced 保持生成的 Redis key 有命名空间
        if self.key_prefix.trim().is_empty() {
            return Err(RustWingError::InvalidConfig(
                "redis presence key_prefix cannot be empty".into(),
            ));
        }
        Ok(())
    }
}

impl RedisPublisherConfig {
    // Create Redis publisher configuration 创建 Redis 发布器配置
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            channel_prefix: "rust-wing".into(),
        }
    }

    // Override the Redis channel prefix 覆盖 Redis 频道前缀
    pub fn with_channel_prefix(mut self, channel_prefix: impl Into<String>) -> Self {
        self.channel_prefix = channel_prefix.into();
        self
    }

    // Validate required Redis publisher configuration 校验必填 Redis 发布配置
    pub fn validate(&self) -> Result<()> {
        // Require an explicit URL so startup failures are clear 要求显式地址以便启动失败清晰
        if self.url.trim().is_empty() {
            return Err(RustWingError::InvalidConfig(
                "redis publisher url cannot be empty".into(),
            ));
        }
        // Keep generated Redis channels namespaced 保持生成的 Redis 频道有命名空间
        if self.channel_prefix.trim().is_empty() {
            return Err(RustWingError::InvalidConfig(
                "redis publisher channel_prefix cannot be empty".into(),
            ));
        }
        Ok(())
    }
}

// Redis-backed presence adapter Redis 在线路由存储适配器
#[derive(Clone)]
pub struct RedisPresenceAdapter {
    // Redis connection manager Redis 连接管理器
    connection: ConnectionManager,
    // Runtime adapter configuration 运行期适配器配置
    config: RedisPresenceConfig,
}

// Redis-backed node publisher adapter Redis 节点消息发布适配器
#[derive(Clone)]
pub struct RedisNodePublisherAdapter {
    // Redis connection manager Redis 连接管理器
    connection: ConnectionManager,
    // Runtime adapter configuration 运行期适配器配置
    config: RedisPublisherConfig,
}

// Redis-backed node subscriber adapter Redis 节点消息订阅适配器
#[derive(Clone)]
pub struct RedisNodeSubscriberAdapter {
    // Redis client used to create a dedicated Pub/Sub connection Redis 客户端用于创建专用 Pub/Sub 连接
    client: redis::Client,
    // Runtime subscriber configuration 运行期订阅配置
    config: RedisPublisherConfig,
}

impl RedisPresenceAdapter {
    // Connect to Redis using user-provided configuration 使用用户配置连接 Redis
    pub async fn connect(config: RedisPresenceConfig) -> Result<Self> {
        // Validate before opening network connections 建立网络连接前先校验配置
        config.validate()?;
        // Create the Redis client from the configured URL 使用配置地址创建 Redis 客户端
        let client = redis::Client::open(config.url.as_str())
            .map_err(|error| redis_error("create redis client", error))?;
        // Use a connection manager so transient disconnects can reconnect 使用连接管理器支持临时断线重连
        let connection = client
            .get_connection_manager()
            .await
            .map_err(|error| redis_error("connect redis presence", error))?;
        Ok(Self { connection, config })
    }

    // Borrow the effective Redis configuration 借用当前 Redis 配置
    pub fn config(&self) -> &RedisPresenceConfig {
        &self.config
    }

    // Build the Redis hash key for one user 构建单个用户的 Redis hash key
    fn key_for_user(&self, user_id: &UserId) -> String {
        format!("{}:presence:{}", self.config.key_prefix, user_id.as_str())
    }
}

impl RedisNodePublisherAdapter {
    // Connect to Redis using user-provided configuration 使用用户配置连接 Redis
    pub async fn connect(config: RedisPublisherConfig) -> Result<Self> {
        // Validate before opening network connections 建立网络连接前先校验配置
        config.validate()?;
        // Create the Redis client from the configured URL 使用配置地址创建 Redis 客户端
        let client = redis::Client::open(config.url.as_str())
            .map_err(|error| redis_error("create redis client", error))?;
        // Use a connection manager so publish calls can reconnect 使用连接管理器支持发布调用重连
        let connection = client
            .get_connection_manager()
            .await
            .map_err(|error| redis_error("connect redis publisher", error))?;
        Ok(Self { connection, config })
    }

    // Borrow the effective Redis publisher configuration 借用当前 Redis 发布配置
    pub fn config(&self) -> &RedisPublisherConfig {
        &self.config
    }

    // Build the Redis channel for one node 构建单个节点的 Redis 频道
    fn channel_for_node(&self, node_id: &NodeId) -> String {
        format!("{}:node:{}", self.config.channel_prefix, node_id.as_str())
    }
}

// Build a core cluster from Redis adapter configuration 从 Redis 适配器配置构建核心集群
pub async fn redis_cluster_from_config(
    presence: RedisPresenceConfig,
    publisher: RedisPublisherConfig,
) -> Result<Cluster> {
    // Connect both Redis-backed adapters before exposing the cluster 暴露集群前先连接两个 Redis 适配器
    let presence = RedisPresenceAdapter::connect(presence).await?;
    let publisher = RedisNodePublisherAdapter::connect(publisher).await?;
    Ok(cluster_from_adapters(presence, publisher))
}

#[async_trait]
impl PresenceStoreAdapter for RedisPresenceAdapter {
    // Register or replace one route in Redis 在 Redis 中注册或替换路由
    async fn register(&self, route: Route, ttl: Duration) -> Result<()> {
        // Clone the manager because Redis commands need a mutable connection 克隆连接管理器以满足命令的可变访问
        let mut connection = self.connection.clone();
        // Store each session as one field under the user's route hash 每个会话作为用户路由 hash 的一个字段
        let key = self.key_for_user(&route.user_id);
        let field = route.session_id.as_str().to_owned();
        let payload = serde_json::to_vec(&route)?;
        // Update the route and its user-level TTL together 同时更新路由和用户级过期时间
        redis::pipe()
            .atomic()
            .hset(&key, field, payload)
            .expire(&key, ttl_seconds(ttl))
            .query_async::<()>(&mut connection)
            .await
            .map_err(|error| redis_error("register redis presence route", error))
    }

    // Remove one exact route from Redis 从 Redis 删除一条精确路由
    async fn remove(&self, user_id: &UserId, session_id: &SessionId) -> Result<()> {
        // Clone the manager because Redis commands need a mutable connection 克隆连接管理器以满足命令的可变访问
        let mut connection = self.connection.clone();
        let key = self.key_for_user(user_id);
        connection
            .hdel::<_, _, ()>(key, session_id.as_str())
            .await
            .map_err(|error| redis_error("remove redis presence route", error))
    }

    // Refresh the route lifetime in Redis 刷新 Redis 中的路由生命周期
    async fn touch(&self, user_id: &UserId, _session_id: &SessionId, ttl: Duration) -> Result<()> {
        // Clone the manager because Redis commands need a mutable connection 克隆连接管理器以满足命令的可变访问
        let mut connection = self.connection.clone();
        let key = self.key_for_user(user_id);
        connection
            .expire::<_, ()>(key, ttl_seconds(ttl))
            .await
            .map_err(|error| redis_error("touch redis presence route", error))
    }

    // Locate every current route for one user 查询用户当前全部路由
    async fn locate(&self, user_id: &UserId) -> Result<Vec<Route>> {
        // Clone the manager because Redis commands need a mutable connection 克隆连接管理器以满足命令的可变访问
        let mut connection = self.connection.clone();
        let key = self.key_for_user(user_id);
        // Load all stored route payloads from the user's hash 读取用户 hash 中的全部路由负载
        let payloads = connection
            .hvals::<_, Vec<Vec<u8>>>(key)
            .await
            .map_err(|error| redis_error("locate redis presence routes", error))?;
        // Decode every stored route through serde 保持路由格式由 serde 统一解码
        payloads
            .into_iter()
            .map(|payload| serde_json::from_slice(&payload).map_err(RustWingError::from))
            .collect()
    }
}

#[async_trait]
impl NodePublisherAdapter for RedisNodePublisherAdapter {
    // Publish one cluster envelope to the target node channel 发布集群信封到目标节点频道
    async fn publish(&self, node_id: &NodeId, envelope: ClusterEnvelope) -> Result<()> {
        // Clone the manager because Redis commands need a mutable connection 克隆连接管理器以满足命令的可变访问
        let mut connection = self.connection.clone();
        let channel = self.channel_for_node(node_id);
        let payload = serde_json::to_vec(&envelope)?;
        // Ignore subscriber count because delivery fanout is Redis-owned 忽略订阅者数量，因为投递扇出由 Redis 负责
        let _: usize = connection
            .publish(channel, payload)
            .await
            .map_err(|error| redis_error("publish redis cluster envelope", error))?;
        Ok(())
    }
}

impl RedisNodeSubscriberAdapter {
    // Connect to Redis using publisher-compatible channel configuration 使用发布器兼容的频道配置连接 Redis
    pub async fn connect(config: RedisPublisherConfig) -> Result<Self> {
        // Validate before creating the reusable client 创建可复用客户端前先校验配置
        config.validate()?;
        let client = redis::Client::open(config.url.as_str())
            .map_err(|error| redis_error("create redis subscriber client", error))?;
        Ok(Self { client, config })
    }

    // Borrow the effective Redis subscriber configuration 借用当前 Redis 订阅配置
    pub fn config(&self) -> &RedisPublisherConfig {
        &self.config
    }

    // Consume messages for the manager's configured node 消费管理器当前节点的消息
    pub async fn run_current_node(&self, wing: RustWing) -> Result<()> {
        self.run_for_node(wing.config().node_id.clone(), wing).await
    }

    // Consume messages for one node until the Pub/Sub stream ends 为指定节点持续消费消息直到订阅流结束
    pub async fn run_for_node(&self, node_id: NodeId, wing: RustWing) -> Result<()> {
        let channel = self.channel_for_node(&node_id);
        let mut pubsub = self
            .client
            .get_async_pubsub()
            .await
            .map_err(|error| redis_error("connect redis subscriber", error))?;
        // Subscribe before reading so setup messages are handled by the client 先订阅再读取，让客户端处理订阅确认消息
        pubsub
            .subscribe(&channel)
            .await
            .map_err(|error| redis_error("subscribe redis node channel", error))?;

        let mut messages = pubsub.on_message();
        while let Some(message) = messages.next().await {
            let envelope = serde_json::from_slice::<ClusterEnvelope>(message.get_payload_bytes())?;
            // Deliver the cross-node envelope into local sessions 将跨节点信封投递到本地会话
            wing.handle_cluster_envelope(envelope)?;
        }
        Ok(())
    }

    // Build the Redis channel for one node 构建单个节点的 Redis 频道
    fn channel_for_node(&self, node_id: &NodeId) -> String {
        format!("{}:node:{}", self.config.channel_prefix, node_id.as_str())
    }
}

// Convert Redis errors into the core error type 将 Redis 错误转换为核心错误类型
fn redis_error(action: &str, error: redis::RedisError) -> RustWingError {
    RustWingError::Cluster(format!("{action}: {error}"))
}

// Convert a Duration to Redis EXPIRE seconds 将 Duration 转换为 Redis EXPIRE 秒数
fn ttl_seconds(ttl: Duration) -> i64 {
    // Round up sub-second TTLs so a positive TTL never expires immediately 向上取整避免正 TTL 立即过期
    let seconds = ttl.as_secs() + u64::from(ttl.subsec_nanos() > 0);
    seconds.max(1) as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    // Redis config rejects empty URLs Redis 配置会拒绝空地址
    #[test]
    fn config_rejects_empty_url() {
        let config = RedisPresenceConfig::new(" ");

        let result = config.validate();

        assert!(matches!(result, Err(RustWingError::InvalidConfig(_))));
    }

    // Redis config keeps user-provided key prefix Redis 配置会保留用户提供的 key 前缀
    #[test]
    fn config_accepts_custom_key_prefix() {
        let config =
            RedisPresenceConfig::new("redis://127.0.0.1:6379").with_key_prefix("my-app:realtime");

        assert!(config.validate().is_ok());
        assert_eq!(config.key_prefix, "my-app:realtime");
    }

    // Redis publisher config rejects empty channel prefixes Redis 发布配置会拒绝空频道前缀
    #[test]
    fn publisher_config_rejects_empty_channel_prefix() {
        let config = RedisPublisherConfig::new("redis://127.0.0.1:6379").with_channel_prefix(" ");

        let result = config.validate();

        assert!(matches!(result, Err(RustWingError::InvalidConfig(_))));
    }

    // TTL conversion rounds up positive sub-second values TTL 转换会向上取整正的亚秒值
    #[test]
    fn ttl_seconds_rounds_up() {
        assert_eq!(ttl_seconds(Duration::from_millis(1)), 1);
        assert_eq!(ttl_seconds(Duration::from_secs(2)), 2);
    }
}

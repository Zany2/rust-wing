use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures_util::StreamExt;
use redis::AsyncCommands;
use redis::aio::{ConnectionLike, ConnectionManager, MultiplexedConnection};
use redis::cluster::ClusterClient;
use redis::cluster_async::ClusterConnection;
use redis::sentinel::{SentinelClient, SentinelNodeConnectionInfo, SentinelServerType};
use rust_wing_core::{
    Cluster, ClusterEnvelope, ConnectionType, NodeId, NodeLease, Result, Route, RustWing,
    RustWingConfig, RustWingError, SessionId, UserId,
};
use tokio::sync::{Mutex, RwLock, watch};
use tokio::task::JoinHandle;

use crate::{NodePublisherAdapter, PresenceStoreAdapter, cluster_from_adapters};

// Initial Redis subscriber reconnect delay in milliseconds Redis 订阅重连初始退避毫秒数
const REDIS_SUBSCRIBER_RECONNECT_BASE_MS: u64 = 100;
// Maximum Redis subscriber reconnect delay in milliseconds Redis 订阅重连最大退避毫秒数
const REDIS_SUBSCRIBER_RECONNECT_MAX_MS: u64 = 5_000;
// Refresh a lease only while the expected instance still owns it 仅在预期实例仍持有时刷新租约
const REDIS_COMPARE_AND_EXPIRE_SCRIPT: &str = "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('EXPIRE', KEYS[1], ARGV[2]) else return 0 end";
// Delete a lease only while the expected instance still owns it 仅在预期实例仍持有时删除租约
const REDIS_COMPARE_AND_DELETE_SCRIPT: &str = "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) else return 0 end";

// Redis deployment mode shared by presence and message adapters Redis 在线路由与消息适配器共用的部署模式
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RedisDeployment {
    // One standalone Redis endpoint 单个 Redis 服务地址
    Standalone {
        // Redis URL, for example redis://default:password@127.0.0.1:6379/0 Redis 连接地址
        url: String,
    },
    // Redis Cluster seed endpoints Redis Cluster 种子地址
    Cluster {
        // Redis Cluster seed URLs Redis Cluster 种子地址列表
        urls: Vec<String>,
    },
    // Redis Sentinel deployment Redis Sentinel 部署
    Sentinel(RedisSentinelConfig),
}

// Redis Sentinel discovery and master connection configuration Redis Sentinel 发现与主节点连接配置
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisSentinelConfig {
    // Sentinel seed URLs Sentinel 种子地址列表
    pub urls: Vec<String>,
    // Sentinel service or master name Sentinel 服务或主节点名称
    pub service_name: String,
    // Optional Redis master ACL username 可选 Redis 主节点 ACL 用户名
    pub redis_username: Option<String>,
    // Optional Redis master password 可选 Redis 主节点密码
    pub redis_password: Option<String>,
    // Redis master database number Redis 主节点数据库编号
    pub redis_database: i64,
}

// Redis presence adapter configuration Redis 在线路由适配器配置
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisPresenceConfig {
    // Redis deployment used for route storage 在线路由存储使用的 Redis 部署
    pub deployment: RedisDeployment,
    // Key prefix shared by all presence keys 所有在线路由 key 的统一前缀
    pub key_prefix: String,
}

// Redis node publisher adapter configuration Redis 节点发布适配器配置
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisPublisherConfig {
    // Redis deployment used for node messages 节点消息使用的 Redis 部署
    pub deployment: RedisDeployment,
    // Channel prefix shared by all node channels 所有节点频道的统一前缀
    pub channel_prefix: String,
}

impl RedisDeployment {
    // Create a standalone Redis deployment 创建单节点 Redis 部署
    pub fn standalone(url: impl Into<String>) -> Self {
        Self::Standalone { url: url.into() }
    }

    // Create a Redis Cluster deployment from seed URLs 通过种子地址创建 Redis Cluster 部署
    pub fn cluster(urls: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self::Cluster {
            urls: urls.into_iter().map(Into::into).collect(),
        }
    }

    // Create a Redis Sentinel deployment 创建 Redis Sentinel 部署
    pub fn sentinel(config: RedisSentinelConfig) -> Self {
        Self::Sentinel(config)
    }

    // Validate deployment-specific connection settings 校验部署模式相关连接配置
    pub fn validate(&self) -> Result<()> {
        match self {
            Self::Standalone { url } => validate_redis_urls(std::slice::from_ref(url), "url"),
            Self::Cluster { urls } => validate_redis_urls(urls, "cluster urls"),
            Self::Sentinel(config) => config.validate(),
        }
    }
}

impl RedisSentinelConfig {
    // Create Redis Sentinel configuration 创建 Redis Sentinel 配置
    pub fn new(
        urls: impl IntoIterator<Item = impl Into<String>>,
        service_name: impl Into<String>,
    ) -> Self {
        Self {
            urls: urls.into_iter().map(Into::into).collect(),
            service_name: service_name.into(),
            redis_username: None,
            redis_password: None,
            redis_database: 0,
        }
    }

    // Set Redis master ACL credentials 设置 Redis 主节点 ACL 凭据
    pub fn with_redis_credentials(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.redis_username = Some(username.into());
        self.redis_password = Some(password.into());
        self
    }

    // Set a password without an ACL username 设置不带 ACL 用户名的密码
    pub fn with_redis_password(mut self, password: impl Into<String>) -> Self {
        self.redis_password = Some(password.into());
        self
    }

    // Set the Redis master database number 设置 Redis 主节点数据库编号
    pub fn with_redis_database(mut self, database: i64) -> Self {
        self.redis_database = database;
        self
    }

    // Validate Sentinel discovery and master settings 校验 Sentinel 发现与主节点配置
    pub fn validate(&self) -> Result<()> {
        validate_redis_urls(&self.urls, "sentinel urls")?;
        if self.service_name.trim().is_empty() {
            return Err(RustWingError::InvalidConfig(
                "redis sentinel service_name cannot be empty".into(),
            ));
        }
        if self.redis_database < 0 {
            return Err(RustWingError::InvalidConfig(
                "redis sentinel database cannot be negative".into(),
            ));
        }
        Ok(())
    }
}

impl RedisPresenceConfig {
    // Create Redis presence configuration 创建 Redis 在线路由配置
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            deployment: RedisDeployment::standalone(url),
            key_prefix: "rust-wing".into(),
        }
    }

    // Create Redis presence configuration for a deployment 为指定 Redis 部署创建在线路由配置
    pub fn from_deployment(deployment: RedisDeployment) -> Self {
        Self {
            deployment,
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
        self.deployment.validate()?;
        // Keep generated Redis keys namespaced 保持生成的 Redis key 有命名空间
        if self.key_prefix.trim().is_empty()
            || self.key_prefix.contains('{')
            || self.key_prefix.contains('}')
        {
            return Err(RustWingError::InvalidConfig(
                "redis presence key_prefix cannot be empty or contain braces".into(),
            ));
        }
        Ok(())
    }
}

impl RedisPublisherConfig {
    // Create Redis publisher configuration 创建 Redis 发布器配置
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            deployment: RedisDeployment::standalone(url),
            channel_prefix: "rust-wing".into(),
        }
    }

    // Create Redis publisher configuration for a deployment 为指定 Redis 部署创建发布配置
    pub fn from_deployment(deployment: RedisDeployment) -> Self {
        Self {
            deployment,
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
        self.deployment.validate()?;
        // Keep generated Redis channels namespaced 保持生成的 Redis 频道有命名空间
        if self.channel_prefix.trim().is_empty() {
            return Err(RustWingError::InvalidConfig(
                "redis publisher channel_prefix cannot be empty".into(),
            ));
        }
        Ok(())
    }
}

// Async command connection selected from the configured Redis deployment 根据 Redis 部署选择的异步命令连接
#[derive(Clone)]
enum RedisConnection {
    // Reconnecting standalone connection 可重连的单节点连接
    Standalone(ConnectionManager),
    // Redis Cluster-aware connection Redis Cluster 感知连接
    Cluster(ClusterConnection),
    // Sentinel-managed master connection Sentinel 管理的主节点连接
    Sentinel(SentinelRedisConnection),
}

// Cached Sentinel master connection plus its resolver Sentinel 主节点缓存连接及其解析器
#[derive(Clone)]
struct SentinelRedisConnection {
    // Shared Sentinel connection state 共享 Sentinel 连接状态
    inner: Arc<SentinelRedisConnectionInner>,
}

struct SentinelRedisConnectionInner {
    // Resolver used to discover the current master 用于发现当前主节点的解析器
    resolver: Mutex<SentinelClient>,
    // Cached current-master connection 缓存的当前主节点连接
    connection: RwLock<MultiplexedConnection>,
    // Selected Redis database number 选定的 Redis 数据库编号
    database: i64,
}

// Pub/Sub connection source selected from the configured deployment 根据部署选择的 Pub/Sub 连接源
#[derive(Clone)]
enum RedisSubscriberSource {
    // Standalone client 单节点客户端
    Standalone(redis::Client),
    // Redis Cluster seeds rotated during reconnect Redis Cluster 重连时轮换的种子地址
    Cluster(Arc<Vec<String>>),
    // Sentinel resolver used to locate the current master 用于定位当前主节点的 Sentinel 解析器
    Sentinel(Arc<Mutex<SentinelClient>>),
}

impl RedisConnection {
    // Connect using one supported Redis deployment mode 使用受支持的 Redis 部署模式连接
    async fn connect(deployment: &RedisDeployment, action: &str) -> Result<Self> {
        deployment.validate()?;
        match deployment {
            RedisDeployment::Standalone { url } => {
                let client = redis::Client::open(url.as_str())
                    .map_err(|error| redis_error("create redis client", error))?;
                let connection = client
                    .get_connection_manager()
                    .await
                    .map_err(|error| redis_error(action, error))?;
                Ok(Self::Standalone(connection))
            }
            RedisDeployment::Cluster { urls } => {
                let client = ClusterClient::new(urls.clone())
                    .map_err(|error| redis_error("create redis cluster client", error))?;
                let connection = client
                    .get_async_connection()
                    .await
                    .map_err(|error| redis_error(action, error))?;
                Ok(Self::Cluster(connection))
            }
            RedisDeployment::Sentinel(config) => Ok(Self::Sentinel(
                SentinelRedisConnection::connect(config, action).await?,
            )),
        }
    }
}

impl ConnectionLike for RedisConnection {
    fn req_packed_command<'a>(
        &'a mut self,
        cmd: &'a redis::Cmd,
    ) -> redis::RedisFuture<'a, redis::Value> {
        match self {
            Self::Standalone(connection) => connection.req_packed_command(cmd),
            Self::Cluster(connection) => connection.req_packed_command(cmd),
            Self::Sentinel(connection) => connection.req_packed_command(cmd),
        }
    }

    fn req_packed_commands<'a>(
        &'a mut self,
        pipeline: &'a redis::Pipeline,
        offset: usize,
        count: usize,
    ) -> redis::RedisFuture<'a, Vec<redis::Value>> {
        match self {
            Self::Standalone(connection) => connection.req_packed_commands(pipeline, offset, count),
            Self::Cluster(connection) => connection.req_packed_commands(pipeline, offset, count),
            Self::Sentinel(connection) => connection.req_packed_commands(pipeline, offset, count),
        }
    }

    fn get_db(&self) -> i64 {
        match self {
            Self::Standalone(connection) => connection.get_db(),
            Self::Cluster(connection) => connection.get_db(),
            Self::Sentinel(connection) => connection.get_db(),
        }
    }
}

impl SentinelRedisConnection {
    // Discover and connect to the current Sentinel master 发现并连接当前 Sentinel 主节点
    async fn connect(config: &RedisSentinelConfig, action: &str) -> Result<Self> {
        let mut resolver = build_sentinel_client(config)?;
        let connection = resolver
            .get_async_connection()
            .await
            .map_err(|error| redis_error(action, error))?;
        Ok(Self {
            inner: Arc::new(SentinelRedisConnectionInner {
                resolver: Mutex::new(resolver),
                connection: RwLock::new(connection),
                database: config.redis_database,
            }),
        })
    }

    // Resolve the latest master and replace the cached connection 解析最新主节点并替换缓存连接
    async fn refresh(&self) -> redis::RedisResult<MultiplexedConnection> {
        let mut resolver = self.inner.resolver.lock().await;
        let connection = resolver.get_async_connection().await?;
        *self.inner.connection.write().await = connection.clone();
        Ok(connection)
    }
}

impl ConnectionLike for SentinelRedisConnection {
    fn req_packed_command<'a>(
        &'a mut self,
        cmd: &'a redis::Cmd,
    ) -> redis::RedisFuture<'a, redis::Value> {
        Box::pin(async move {
            let mut connection = self.inner.connection.read().await.clone();
            match connection.req_packed_command(cmd).await {
                Ok(value) => Ok(value),
                Err(error) if redis_error_requires_master_refresh(&error) => {
                    let retry = redis_error_is_readonly(&error);
                    let mut refreshed = self.refresh().await?;
                    if retry {
                        refreshed.req_packed_command(cmd).await
                    } else {
                        Err(error)
                    }
                }
                Err(error) => Err(error),
            }
        })
    }

    fn req_packed_commands<'a>(
        &'a mut self,
        pipeline: &'a redis::Pipeline,
        offset: usize,
        count: usize,
    ) -> redis::RedisFuture<'a, Vec<redis::Value>> {
        Box::pin(async move {
            let mut connection = self.inner.connection.read().await.clone();
            match connection
                .req_packed_commands(pipeline, offset, count)
                .await
            {
                Ok(values) => Ok(values),
                Err(error) if redis_error_requires_master_refresh(&error) => {
                    let retry = redis_error_is_readonly(&error);
                    let mut refreshed = self.refresh().await?;
                    if retry {
                        refreshed.req_packed_commands(pipeline, offset, count).await
                    } else {
                        Err(error)
                    }
                }
                Err(error) => Err(error),
            }
        })
    }

    fn get_db(&self) -> i64 {
        self.inner.database
    }
}

impl RedisSubscriberSource {
    // Build a Pub/Sub connection source for one deployment 构建指定部署的 Pub/Sub 连接源
    fn from_deployment(deployment: &RedisDeployment) -> Result<Self> {
        deployment.validate()?;
        match deployment {
            RedisDeployment::Standalone { url } => redis::Client::open(url.as_str())
                .map(Self::Standalone)
                .map_err(|error| redis_error("create redis subscriber client", error)),
            RedisDeployment::Cluster { urls } => Ok(Self::Cluster(Arc::new(urls.clone()))),
            RedisDeployment::Sentinel(config) => Ok(Self::Sentinel(Arc::new(Mutex::new(
                build_sentinel_client(config)?,
            )))),
        }
    }

    // Resolve a client for one subscriber connection attempt 解析一次订阅连接尝试使用的客户端
    async fn client_for_attempt(&self, attempt: u32) -> Result<redis::Client> {
        match self {
            Self::Standalone(client) => Ok(client.clone()),
            Self::Cluster(urls) => {
                let index = attempt.saturating_sub(1) as usize % urls.len();
                redis::Client::open(urls[index].as_str())
                    .map_err(|error| redis_error("create redis cluster subscriber client", error))
            }
            Self::Sentinel(resolver) => resolver
                .lock()
                .await
                .async_get_client()
                .await
                .map_err(|error| redis_error("resolve redis sentinel subscriber master", error)),
        }
    }
}

// Redis-backed presence adapter Redis 在线路由存储适配器
#[derive(Clone)]
pub struct RedisPresenceAdapter {
    // Deployment-aware Redis command connection Redis 部署感知命令连接
    connection: RedisConnection,
    // Runtime adapter configuration 运行期适配器配置
    config: RedisPresenceConfig,
}

// Redis-backed node publisher adapter Redis 节点消息发布适配器
#[derive(Clone)]
pub struct RedisNodePublisherAdapter {
    // Deployment-aware Redis command connection Redis 部署感知命令连接
    connection: RedisConnection,
    // Runtime adapter configuration 运行期适配器配置
    config: RedisPublisherConfig,
}

// Redis-backed node subscriber adapter Redis 节点消息订阅适配器
#[derive(Clone)]
pub struct RedisNodeSubscriberAdapter {
    // Source used to create dedicated Pub/Sub connections 用于创建专用 Pub/Sub 连接的来源
    source: RedisSubscriberSource,
    // Runtime subscriber configuration 运行期订阅配置
    config: RedisPublisherConfig,
}

// Redis cluster dependencies plus its subscriber Redis 集群依赖及其订阅器
pub struct RedisClusterParts {
    // Core cluster dependency 核心集群依赖
    pub cluster: Cluster,
    // Subscriber that consumes envelopes for the local node 消费本地节点信封的订阅器
    pub subscriber: RedisNodeSubscriberAdapter,
}

// Managed Redis-backed RustWing runtime 托管的 Redis 版 RustWing 运行时
pub struct RedisRustWing {
    // Core connection manager 核心连接管理器
    wing: RustWing,
    // Background subscriber for this node 当前节点的后台订阅任务
    subscriber: RedisNodeSubscriberHandle,
}

pub struct RedisNodeSubscriberHandle {
    // Stop signal sent to the subscriber task 发送给订阅任务的停止信号
    stop: watch::Sender<bool>,
    // Background subscriber task 后台订阅任务
    task: JoinHandle<Result<()>>,
}

impl RedisPresenceAdapter {
    // Connect to Redis using user-provided configuration 使用用户配置连接 Redis
    pub async fn connect(config: RedisPresenceConfig) -> Result<Self> {
        // Validate before opening network connections 建立网络连接前先校验配置
        config.validate()?;
        let connection =
            RedisConnection::connect(&config.deployment, "connect redis presence").await?;
        Ok(Self { connection, config })
    }

    // Borrow the effective Redis configuration 借用当前 Redis 配置
    pub fn config(&self) -> &RedisPresenceConfig {
        &self.config
    }

    // Build the Redis hash key for one connection-user pair 构建单个连接体系用户的 Redis hash key
    fn key_for_user(&self, connection_type: &ConnectionType, user_id: &UserId) -> String {
        redis_presence_user_key(&self.config.key_prefix, connection_type, user_id)
    }

    // Build the Redis key for one session route 构建单个会话路由的 Redis key
    fn key_for_session(&self, session_id: &SessionId) -> String {
        redis_presence_session_key(&self.config.key_prefix, session_id)
    }

    // Build the Redis set key for route-owning nodes 构建拥有路由的节点集合 key
    fn key_for_nodes(&self) -> String {
        redis_presence_nodes_key(&self.config.key_prefix)
    }

    // Build the Redis key for one node lease 构建单个节点租约的 Redis key
    fn key_for_node_lease(&self, node_id: &NodeId) -> String {
        redis_presence_node_lease_key(&self.config.key_prefix, node_id)
    }

    // Build the Redis set key for all known sessions 构建全部已知会话的 Redis 集合 key
    fn key_for_sessions(&self) -> String {
        redis_presence_sessions_key(&self.config.key_prefix)
    }

    // Collect live session routes through the session index 通过会话索引收集活跃路由
    async fn collect_session_routes(
        &self,
        connection_type: Option<&ConnectionType>,
    ) -> Result<Vec<Route>> {
        let mut connection = self.connection.clone();
        let sessions_key = self.key_for_sessions();
        let session_ids = connection
            .smembers::<_, Vec<String>>(&sessions_key)
            .await
            .map_err(|error| redis_error("list redis presence session index", error))?;
        let mut routes = Vec::new();
        let mut stale_session_ids = Vec::new();
        for session_id in session_ids {
            let key = self.key_for_session(&SessionId::from(session_id.as_str()));
            let payload = connection
                .get::<_, Option<Vec<u8>>>(&key)
                .await
                .map_err(|error| redis_error("load redis presence session route", error))?;
            let Some(payload) = payload else {
                stale_session_ids.push(session_id);
                continue;
            };
            let route: Route = serde_json::from_slice(&payload)?;
            if connection_type.is_some_and(|expected| &route.connection_type != expected) {
                continue;
            }
            if self.route_node_is_live(&mut connection, &route).await? {
                routes.push(route);
            }
        }
        if !stale_session_ids.is_empty() {
            connection
                .srem::<_, _, ()>(&sessions_key, stale_session_ids)
                .await
                .map_err(|error| redis_error("clean redis presence session index", error))?;
        }
        Ok(routes)
    }

    // Check whether the route owner still has an active node lease 检查路由所属节点是否仍有活跃租约
    async fn route_node_is_live(
        &self,
        connection: &mut RedisConnection,
        route: &Route,
    ) -> Result<bool> {
        let lease_key = self.key_for_node_lease(&route.node_id);
        connection
            .exists::<_, bool>(lease_key)
            .await
            .map_err(|error| redis_error("check redis route node lease", error))
    }
}

impl RedisNodePublisherAdapter {
    // Connect to Redis using user-provided configuration 使用用户配置连接 Redis
    pub async fn connect(config: RedisPublisherConfig) -> Result<Self> {
        // Validate before opening network connections 建立网络连接前先校验配置
        config.validate()?;
        let connection =
            RedisConnection::connect(&config.deployment, "connect redis publisher").await?;
        Ok(Self { connection, config })
    }

    // Borrow the effective Redis publisher configuration 借用当前 Redis 发布配置
    pub fn config(&self) -> &RedisPublisherConfig {
        &self.config
    }

    // Build the Redis channel for one node 构建单个节点的 Redis 频道
    fn channel_for_node(&self, node_id: &NodeId) -> String {
        redis_node_channel(&self.config.channel_prefix, node_id)
    }
}

// Build a core cluster from Redis adapter configuration 从 Redis 适配器配置构建核心集群
pub async fn redis_cluster_from_config(
    presence: RedisPresenceConfig,
    publisher: RedisPublisherConfig,
) -> Result<Cluster> {
    Ok(redis_cluster_parts_from_config(presence, publisher)
        .await?
        .cluster)
}

// Build Redis cluster dependencies and a matching node subscriber 构建 Redis 集群依赖及匹配的节点订阅器
pub async fn redis_cluster_parts_from_config(
    presence: RedisPresenceConfig,
    publisher: RedisPublisherConfig,
) -> Result<RedisClusterParts> {
    // Connect Redis-backed adapters before exposing runtime parts 暴露运行部件前先连接 Redis 适配器
    let presence = RedisPresenceAdapter::connect(presence).await?;
    let publisher_adapter = RedisNodePublisherAdapter::connect(publisher.clone()).await?;
    let subscriber = RedisNodeSubscriberAdapter::connect(publisher).await?;
    Ok(RedisClusterParts {
        cluster: cluster_from_adapters(presence, publisher_adapter),
        subscriber,
    })
}

// Build a Redis-backed RustWing runtime from core configuration and one Redis URL 通过核心配置和单个 Redis 地址创建 Redis 版 RustWing 运行时
pub async fn redis_rust_wing_from_config(
    config: RustWingConfig,
    redis_url: impl Into<String>,
) -> Result<RedisRustWing> {
    let url = redis_url.into();
    redis_rust_wing_from_parts(
        config,
        RedisPresenceConfig::new(url.clone()),
        RedisPublisherConfig::new(url),
    )
    .await
}

// Build a Redis-backed RustWing runtime from explicit Redis parts 通过显式 Redis 部件创建 RustWing 运行时
pub async fn redis_rust_wing_from_parts(
    mut config: RustWingConfig,
    presence: RedisPresenceConfig,
    publisher: RedisPublisherConfig,
) -> Result<RedisRustWing> {
    // Redis runtime always enables external cluster routing because adapters provide the dependencies. Redis 运行时始终启用外部集群路由
    config.cluster.enabled = true;
    let parts = redis_cluster_parts_from_config(presence, publisher).await?;
    let wing = RustWing::with_cluster_checked(config, Some(parts.cluster)).await?;
    let subscriber = parts.subscriber.spawn_current_node(wing.clone());
    Ok(RedisRustWing { wing, subscriber })
}

#[async_trait]
impl PresenceStoreAdapter for RedisPresenceAdapter {
    // Register or replace one route in Redis 在 Redis 中注册或替换路由
    async fn register(&self, route: Route, ttl: Duration) -> Result<()> {
        // Clone the manager because Redis commands need a mutable connection 克隆连接管理器以满足命令的可变访问
        let mut connection = self.connection.clone();
        // Store each session as one field under the user's route hash 每个会话作为用户路由 hash 的一个字段
        let key = self.key_for_user(&route.connection_type, &route.user_id);
        let session_key = self.key_for_session(&route.session_id);
        let sessions_key = self.key_for_sessions();
        let nodes_key = self.key_for_nodes();
        let field = route.session_id.as_str().to_owned();
        let payload = serde_json::to_vec(&route)?;
        // Update the route and its user-level TTL together 同时更新路由和用户级过期时间
        redis::pipe()
            .atomic()
            .hset(&key, field, payload.clone())
            .expire(&key, ttl_seconds(ttl))
            .set(&session_key, payload)
            .expire(&session_key, ttl_seconds(ttl))
            .sadd(&sessions_key, route.session_id.as_str())
            .sadd(&nodes_key, route.node_id.as_str())
            .query_async::<()>(&mut connection)
            .await
            .map_err(|error| redis_error("register redis presence route", error))
    }

    // Remove one exact route from Redis 从 Redis 删除一条精确路由
    async fn remove(
        &self,
        connection_type: &ConnectionType,
        user_id: &UserId,
        session_id: &SessionId,
    ) -> Result<()> {
        // Clone the manager because Redis commands need a mutable connection 克隆连接管理器以满足命令的可变访问
        let mut connection = self.connection.clone();
        let key = self.key_for_user(connection_type, user_id);
        let session_key = self.key_for_session(session_id);
        let sessions_key = self.key_for_sessions();
        redis::pipe()
            .atomic()
            .hdel(&key, session_id.as_str())
            .del(&session_key)
            .srem(&sessions_key, session_id.as_str())
            .query_async::<()>(&mut connection)
            .await
            .map_err(|error| redis_error("remove redis presence route", error))
    }

    // Refresh the route lifetime in Redis 刷新 Redis 中的路由生命周期
    async fn touch(
        &self,
        connection_type: &ConnectionType,
        user_id: &UserId,
        session_id: &SessionId,
        ttl: Duration,
    ) -> Result<()> {
        // Clone the manager because Redis commands need a mutable connection 克隆连接管理器以满足命令的可变访问
        let mut connection = self.connection.clone();
        let key = self.key_for_user(connection_type, user_id);
        let session_key = self.key_for_session(session_id);
        redis::pipe()
            .atomic()
            .expire(&key, ttl_seconds(ttl))
            .expire(&session_key, ttl_seconds(ttl))
            .query_async::<()>(&mut connection)
            .await
            .map_err(|error| redis_error("touch redis presence route", error))
    }

    // Locate every current route for one user 查询用户当前全部路由
    async fn locate(
        &self,
        connection_type: &ConnectionType,
        user_id: &UserId,
    ) -> Result<Vec<Route>> {
        // Clone the manager because Redis commands need a mutable connection 克隆连接管理器以满足命令的可变访问
        let mut connection = self.connection.clone();
        let key = self.key_for_user(connection_type, user_id);
        // Load user hash fields as an index; session keys remain the source of truth 读取用户 hash 字段作为索引，session key 才是事实来源
        let fields = connection
            .hkeys::<_, Vec<String>>(&key)
            .await
            .map_err(|error| redis_error("locate redis presence route fields", error))?;
        let mut routes = Vec::new();
        let mut stale_fields = Vec::new();
        for field in fields {
            let session_id = SessionId::from(field.as_str());
            let session_key = self.key_for_session(&session_id);
            let payload = connection
                .get::<_, Option<Vec<u8>>>(&session_key)
                .await
                .map_err(|error| redis_error("locate redis presence session route", error))?;
            match payload {
                Some(payload) => {
                    let route: Route = serde_json::from_slice(&payload)?;
                    if self.route_node_is_live(&mut connection, &route).await? {
                        routes.push(route);
                    }
                }
                None => stale_fields.push(field),
            }
        }
        for field in stale_fields {
            // Remove stale hash fields left after individual session TTL expiry 清理单会话 TTL 过期后残留的 hash 字段
            connection
                .hdel::<_, _, ()>(&key, field)
                .await
                .map_err(|error| redis_error("clean stale redis presence route", error))?;
        }
        Ok(routes)
    }

    // Locate one route by session id from Redis 通过会话标识从 Redis 查询一条路由
    async fn locate_session(&self, session_id: &SessionId) -> Result<Option<Route>> {
        // Clone the manager because Redis commands need a mutable connection 克隆连接管理器以满足命令的可变访问
        let mut connection = self.connection.clone();
        let key = self.key_for_session(session_id);
        let sessions_key = self.key_for_sessions();
        let payload = connection
            .get::<_, Option<Vec<u8>>>(&key)
            .await
            .map_err(|error| redis_error("locate redis presence session route", error))?;
        let Some(payload) = payload else {
            connection
                .srem::<_, _, ()>(sessions_key, session_id.as_str())
                .await
                .map_err(|error| redis_error("clean redis presence session index", error))?;
            return Ok(None);
        };
        let route: Route = serde_json::from_slice(&payload)?;
        if self.route_node_is_live(&mut connection, &route).await? {
            Ok(Some(route))
        } else {
            Ok(None)
        }
    }

    // List live routes in one connection system from Redis 从 Redis 列出某个连接体系中的活跃路由
    async fn list_routes(&self, connection_type: &ConnectionType) -> Result<Vec<Route>> {
        self.collect_session_routes(Some(connection_type)).await
    }

    // List all live routes across connection systems from Redis 从 Redis 列出全部连接体系中的活跃路由
    async fn list_all_routes(&self) -> Result<Vec<Route>> {
        self.collect_session_routes(None).await
    }

    // List route-owning nodes from Redis 从 Redis 列出拥有路由的节点
    async fn list_nodes(&self) -> Result<Vec<NodeId>> {
        // Clone the manager because Redis commands need a mutable connection 克隆连接管理器以满足命令的可变访问
        let mut connection = self.connection.clone();
        let key = self.key_for_nodes();
        let nodes = connection
            .smembers::<_, Vec<String>>(key)
            .await
            .map_err(|error| redis_error("list redis presence nodes", error))?;
        let mut live_nodes = Vec::new();
        for node in nodes {
            let node_id = NodeId::from(node);
            let lease_key = self.key_for_node_lease(&node_id);
            let exists = connection
                .exists::<_, bool>(&lease_key)
                .await
                .map_err(|error| redis_error("check redis node lease", error))?;
            if exists {
                live_nodes.push(node_id);
            }
        }
        Ok(live_nodes)
    }

    // Register or refresh one node lease in Redis 在 Redis 中注册或刷新一个节点租约
    async fn register_node(
        &self,
        node_id: &NodeId,
        instance_id: &str,
        ttl: Duration,
    ) -> Result<NodeLease> {
        // Clone the manager because Redis commands need a mutable connection 克隆连接管理器以满足命令的可变访问
        let mut connection = self.connection.clone();
        let key = self.key_for_node_lease(node_id);
        let ttl = ttl_seconds(ttl) as u64;
        let acquired = redis::cmd("SET")
            .arg(&key)
            .arg(instance_id)
            .arg("NX")
            .arg("EX")
            .arg(ttl)
            .query_async::<Option<String>>(&mut connection)
            .await
            .map_err(|error| redis_error("register redis node lease", error))?;
        if acquired.is_some() {
            return Ok(NodeLease::Acquired);
        }
        let refreshed = redis::cmd("EVAL")
            .arg(REDIS_COMPARE_AND_EXPIRE_SCRIPT)
            .arg(1)
            .arg(&key)
            .arg(instance_id)
            .arg(ttl)
            .query_async::<i64>(&mut connection)
            .await
            .map_err(|error| redis_error("refresh redis node lease", error))?;
        if refreshed == 1 {
            Ok(NodeLease::Refreshed)
        } else {
            Ok(NodeLease::Conflict)
        }
    }

    // Remove one Redis node lease if still owned by the instance 当前实例仍持有时删除 Redis 节点租约
    async fn unregister_node(&self, node_id: &NodeId, instance_id: &str) -> Result<()> {
        // Clone the manager because Redis commands need a mutable connection 克隆连接管理器以满足命令的可变访问
        let mut connection = self.connection.clone();
        let key = self.key_for_node_lease(node_id);
        redis::cmd("EVAL")
            .arg(REDIS_COMPARE_AND_DELETE_SCRIPT)
            .arg(1)
            .arg(&key)
            .arg(instance_id)
            .query_async::<i64>(&mut connection)
            .await
            .map_err(|error| redis_error("remove redis node lease", error))?;
        Ok(())
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
        let source = RedisSubscriberSource::from_deployment(&config.deployment)?;
        Ok(Self { source, config })
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
        let (_stop_tx, stop_rx) = watch::channel(false);
        self.run_for_node_until_stop(node_id, wing, stop_rx).await
    }

    // Start a managed subscriber task for the manager's configured node 为管理器当前节点启动托管订阅任务
    pub fn spawn_current_node(&self, wing: RustWing) -> RedisNodeSubscriberHandle {
        self.spawn_for_node(wing.config().node_id.clone(), wing)
    }

    // Start a managed subscriber task for one node 为指定节点启动托管订阅任务
    pub fn spawn_for_node(&self, node_id: NodeId, wing: RustWing) -> RedisNodeSubscriberHandle {
        let subscriber = self.clone();
        let (stop, stop_rx) = watch::channel(false);
        let task = tokio::spawn(async move {
            subscriber
                .run_for_node_until_stop(node_id, wing, stop_rx)
                .await
        });
        RedisNodeSubscriberHandle { stop, task }
    }

    // Consume messages until Redis ends the stream or a stop signal arrives 消费消息直到 Redis 结束流或收到停止信号
    async fn run_for_node_until_stop(
        &self,
        node_id: NodeId,
        wing: RustWing,
        mut stop_rx: watch::Receiver<bool>,
    ) -> Result<()> {
        let channel = self.channel_for_node(&node_id);
        let mut reconnect_attempt = 0_u32;
        loop {
            if *stop_rx.borrow() {
                break;
            }
            let result = self
                .consume_node_channel_once(
                    &channel,
                    &wing,
                    &mut stop_rx,
                    reconnect_attempt.saturating_add(1),
                )
                .await;
            if *stop_rx.borrow() {
                break;
            }
            if result.is_err() {
                reconnect_attempt = reconnect_attempt.saturating_add(1);
            } else {
                reconnect_attempt = 1;
            }
            let delay = redis_subscriber_reconnect_delay(reconnect_attempt);
            if wait_for_subscriber_reconnect_delay(delay, &mut stop_rx).await {
                break;
            }
        }
        Ok(())
    }

    // Consume one Redis Pub/Sub connection until it disconnects 消费单条 Redis Pub/Sub 连接直到断开
    async fn consume_node_channel_once(
        &self,
        channel: &str,
        wing: &RustWing,
        stop_rx: &mut watch::Receiver<bool>,
        reconnect_attempt: u32,
    ) -> Result<()> {
        let client = self.source.client_for_attempt(reconnect_attempt).await?;
        let mut pubsub = client
            .get_async_pubsub()
            .await
            .map_err(|error| redis_error("connect redis subscriber", error))?;
        // Subscribe before reading so setup messages are handled by the client 先订阅再读取，让客户端处理订阅确认消息
        pubsub
            .subscribe(&channel)
            .await
            .map_err(|error| redis_error("subscribe redis node channel", error))?;

        let mut messages = pubsub.on_message();
        loop {
            tokio::select! {
                message = messages.next() => {
                    let Some(message) = message else {
                        break;
                    };
                    let Ok(envelope) = serde_json::from_slice::<ClusterEnvelope>(message.get_payload_bytes()) else {
                        continue;
                    };
                    // Deliver the cross-node envelope into local sessions 将跨节点信封投递到本地会话
                    let _ = wing.handle_cluster_envelope(envelope);
                }
                changed = stop_rx.changed() => {
                    if changed.is_err() || *stop_rx.borrow() {
                        break;
                    }
                }
            }
        }
        Ok(())
    }

    // Build the Redis channel for one node 构建单个节点的 Redis 频道
    fn channel_for_node(&self, node_id: &NodeId) -> String {
        redis_node_channel(&self.config.channel_prefix, node_id)
    }
}

impl RedisNodeSubscriberHandle {
    // Ask the subscriber task to stop and wait for it 请求订阅任务停止并等待结束
    pub async fn shutdown(self) -> Result<()> {
        let _ = self.stop.send(true);
        self.task
            .await
            .map_err(|error| RustWingError::Cluster(format!("redis subscriber task: {error}")))?
    }

    // Wait for the subscriber task to finish without sending a stop signal 等待订阅任务自行结束且不发送停止信号
    pub async fn join(self) -> Result<()> {
        self.task
            .await
            .map_err(|error| RustWingError::Cluster(format!("redis subscriber task: {error}")))?
    }
}

// Convert Redis errors into the core error type 将 Redis 错误转换为核心错误类型
impl RedisRustWing {
    // Borrow the managed core manager 借用托管的核心管理器
    pub fn wing(&self) -> &RustWing {
        &self.wing
    }

    // Clone the core manager handle 克隆核心管理器句柄
    pub fn wing_clone(&self) -> RustWing {
        self.wing.clone()
    }

    // Split the runtime into its core manager and subscriber handle 拆分运行时为核心管理器和订阅句柄
    pub fn into_parts(self) -> (RustWing, RedisNodeSubscriberHandle) {
        (self.wing, self.subscriber)
    }

    // Stop Redis subscription and unregister local runtime state 停止 Redis 订阅并注销本地运行状态
    pub async fn shutdown(self) -> Result<usize> {
        let (wing, subscriber) = self.into_parts();
        let subscriber_result = subscriber.shutdown().await;
        let shutdown_result = wing.shutdown().await;
        subscriber_result?;
        shutdown_result
    }
}

fn redis_error(action: &str, error: redis::RedisError) -> RustWingError {
    RustWingError::Cluster(format!("{action}: {error}"))
}

// Validate a non-empty list of Redis URLs 校验非空 Redis 地址列表
fn validate_redis_urls(urls: &[String], name: &str) -> Result<()> {
    if urls.is_empty() || urls.iter().any(|url| url.trim().is_empty()) {
        return Err(RustWingError::InvalidConfig(format!(
            "redis {name} cannot be empty"
        )));
    }
    for url in urls {
        redis::Client::open(url.as_str()).map_err(|error| {
            RustWingError::InvalidConfig(format!("invalid redis {name} entry: {error}"))
        })?;
    }
    Ok(())
}

// Build a Sentinel resolver with optional Redis master credentials 构建带可选 Redis 主节点凭据的 Sentinel 解析器
fn build_sentinel_client(config: &RedisSentinelConfig) -> Result<SentinelClient> {
    config.validate()?;
    let mut redis_info = redis::RedisConnectionInfo::default().set_db(config.redis_database);
    if let Some(username) = &config.redis_username {
        redis_info = redis_info.set_username(username);
    }
    if let Some(password) = &config.redis_password {
        redis_info = redis_info.set_password(password);
    }
    let node_info = SentinelNodeConnectionInfo::default().set_redis_connection_info(redis_info);
    SentinelClient::build(
        config.urls.clone(),
        config.service_name.clone(),
        Some(node_info),
        SentinelServerType::Master,
    )
    .map_err(|error| redis_error("create redis sentinel client", error))
}

// Check whether Sentinel should resolve a fresh master 检查 Sentinel 是否需要重新解析主节点
fn redis_error_requires_master_refresh(error: &redis::RedisError) -> bool {
    error.is_connection_dropped() || redis_error_is_readonly(error)
}

// Check whether Redis rejected a write because the connection points to a replica 检查 Redis 是否因连接指向副本而拒绝写入
fn redis_error_is_readonly(error: &redis::RedisError) -> bool {
    matches!(
        error.kind(),
        redis::ErrorKind::Server(redis::ServerErrorKind::ReadOnly)
    )
}

// Compute the capped Redis subscriber reconnect delay 计算带上限的 Redis 订阅重连退避
fn redis_subscriber_reconnect_delay(attempt: u32) -> Duration {
    let exponent = attempt.saturating_sub(1).min(6);
    let multiplier = 1_u64 << exponent;
    let millis = REDIS_SUBSCRIBER_RECONNECT_BASE_MS
        .saturating_mul(multiplier)
        .min(REDIS_SUBSCRIBER_RECONNECT_MAX_MS);
    Duration::from_millis(millis)
}

// Wait for reconnect delay or stop signal 等待重连退避或停止信号
async fn wait_for_subscriber_reconnect_delay(
    delay: Duration,
    stop_rx: &mut watch::Receiver<bool>,
) -> bool {
    tokio::select! {
        _ = tokio::time::sleep(delay) => false,
        changed = stop_rx.changed() => changed.is_err() || *stop_rx.borrow(),
    }
}

// Convert a Duration to Redis EXPIRE seconds 将 Duration 转换为 Redis EXPIRE 秒数
fn ttl_seconds(ttl: Duration) -> i64 {
    // Round up sub-second TTLs so a positive TTL never expires immediately 向上取整避免正 TTL 立即过期
    let seconds = ttl.as_secs() + u64::from(ttl.subsec_nanos() > 0);
    seconds.max(1) as i64
}

// Build the Redis hash key for one connection-user pair 构建单个连接体系用户的 Redis hash key
fn redis_presence_user_key(
    key_prefix: &str,
    connection_type: &ConnectionType,
    user_id: &UserId,
) -> String {
    format!(
        "{}:user:{}:{}",
        redis_presence_namespace(key_prefix),
        connection_type.as_str(),
        user_id.as_str()
    )
}

// Build the Redis key for one session route 构建单个会话路由的 Redis key
fn redis_presence_session_key(key_prefix: &str, session_id: &SessionId) -> String {
    format!(
        "{}:session:{}",
        redis_presence_namespace(key_prefix),
        session_id.as_str()
    )
}

// Build the Redis set key for all known sessions 构建全部已知会话的 Redis 集合 key
fn redis_presence_sessions_key(key_prefix: &str) -> String {
    format!("{}:sessions", redis_presence_namespace(key_prefix))
}

// Build the Redis set key for route-owning nodes 构建拥有路由的节点集合 key
fn redis_presence_nodes_key(key_prefix: &str) -> String {
    format!("{}:nodes", redis_presence_namespace(key_prefix))
}

// Build the Redis key for one node lease 构建单个节点租约的 Redis key
fn redis_presence_node_lease_key(key_prefix: &str, node_id: &NodeId) -> String {
    format!(
        "{}:node:{}",
        redis_presence_namespace(key_prefix),
        node_id.as_str()
    )
}

// Build the versioned same-slot namespace for all presence keys 构建全部在线路由 key 共用的版本化同槽命名空间
fn redis_presence_namespace(key_prefix: &str) -> String {
    format!("{{{key_prefix}:presence}}:v2")
}

// Build the Redis channel for one node 构建单个节点的 Redis 频道
fn redis_node_channel(channel_prefix: &str, node_id: &NodeId) -> String {
    format!("{}:node:{}", channel_prefix, node_id.as_str())
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

    // Subscriber reconnect delay grows and stays capped 订阅重连退避会增长并保持上限
    #[test]
    fn subscriber_reconnect_delay_is_capped() {
        assert_eq!(
            redis_subscriber_reconnect_delay(1),
            Duration::from_millis(100)
        );
        assert_eq!(
            redis_subscriber_reconnect_delay(2),
            Duration::from_millis(200)
        );
        assert_eq!(
            redis_subscriber_reconnect_delay(100),
            Duration::from_millis(5_000)
        );
    }

    // Subscriber reconnect sleep can be interrupted by shutdown 订阅重连等待可以被关闭信号打断
    #[tokio::test]
    async fn subscriber_reconnect_delay_stops_on_shutdown() {
        let (stop, mut stop_rx) = watch::channel(false);
        stop.send(true).unwrap();

        let stopped =
            wait_for_subscriber_reconnect_delay(Duration::from_secs(30), &mut stop_rx).await;

        assert!(stopped);
    }

    // Redis presence keys are scoped by connection type Redis 在线路由 key 会按连接体系隔离
    #[test]
    fn presence_key_includes_connection_type() {
        let admin = redis_presence_user_key(
            "rust-wing",
            &ConnectionType::from("admin"),
            &UserId::from("alice"),
        );
        let game = redis_presence_user_key(
            "rust-wing",
            &ConnectionType::from("game"),
            &UserId::from("alice"),
        );

        assert_ne!(admin, game);
        assert_eq!(admin, "{rust-wing:presence}:v2:user:admin:alice");
    }

    // Every presence key uses the same Redis Cluster hash tag 全部在线路由 key 使用相同 Redis Cluster Hash Tag
    #[test]
    fn presence_keys_share_one_cluster_slot() {
        let user = redis_presence_user_key(
            "rust-wing",
            &ConnectionType::from("default"),
            &UserId::from("alice"),
        );
        let session = redis_presence_session_key("rust-wing", &SessionId::from("session-a"));
        let sessions = redis_presence_sessions_key("rust-wing");
        let nodes = redis_presence_nodes_key("rust-wing");
        let lease = redis_presence_node_lease_key("rust-wing", &NodeId::from("node-a"));

        for key in [user, session, sessions, nodes, lease] {
            assert!(key.starts_with("{rust-wing:presence}:v2:"));
        }
    }

    // Redis presence prefixes reject braces that could change the hash slot Redis 在线路由前缀拒绝会改变 Hash Slot 的花括号
    #[test]
    fn presence_config_rejects_hash_tag_braces() {
        let config = RedisPresenceConfig::new("redis://127.0.0.1:6379").with_key_prefix("my-{app}");

        assert!(config.validate().is_err());
    }

    // Redis Cluster configuration accepts multiple seed URLs Redis Cluster 配置接受多个种子地址
    #[test]
    fn cluster_config_accepts_multiple_seed_urls() {
        let deployment = RedisDeployment::cluster(["redis://redis-1:6379", "redis://redis-2:6379"]);
        let config = RedisPresenceConfig::from_deployment(deployment);

        assert!(config.validate().is_ok());
    }

    // Redis Sentinel configuration keeps discovery and master settings Redis Sentinel 配置会保留发现与主节点设置
    #[test]
    fn sentinel_config_accepts_master_settings() {
        let sentinel = RedisSentinelConfig::new(
            ["redis://sentinel-1:26379", "redis://sentinel-2:26379"],
            "mymaster",
        )
        .with_redis_credentials("default", "secret")
        .with_redis_database(1);
        let config = RedisPresenceConfig::from_deployment(RedisDeployment::sentinel(sentinel));

        assert!(config.validate().is_ok());
    }

    // Redis node channels are scoped by node id Redis 节点频道会按节点标识隔离
    #[test]
    fn publisher_channel_includes_node_id() {
        assert_eq!(
            redis_node_channel("rust-wing", &NodeId::from("ws-1")),
            "rust-wing:node:ws-1"
        );
    }

    // Redis runtime rejects empty URLs before connecting Redis 运行时会在连接前拒绝空地址
    #[tokio::test]
    async fn redis_runtime_rejects_empty_backend_url() {
        let result = redis_rust_wing_from_config(RustWingConfig::default(), " ").await;

        assert!(matches!(result, Err(RustWingError::InvalidConfig(_))));
    }
}

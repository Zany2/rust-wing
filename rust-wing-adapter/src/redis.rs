use std::time::Duration;

use async_trait::async_trait;
use futures_util::StreamExt;
use redis::AsyncCommands;
use redis::aio::ConnectionManager;
use rust_wing_core::{
    Cluster, ClusterEnvelope, ConnectionType, NodeId, NodeLease, Result, Route, RustWing,
    RustWingConfig, RustWingError, SessionId, UserId,
};
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::{NodePublisherAdapter, PresenceStoreAdapter, cluster_from_adapters};

// Initial Redis subscriber reconnect delay in milliseconds Redis 订阅重连初始退避毫秒数
const REDIS_SUBSCRIBER_RECONNECT_BASE_MS: u64 = 100;
// Maximum Redis subscriber reconnect delay in milliseconds Redis 订阅重连最大退避毫秒数
const REDIS_SUBSCRIBER_RECONNECT_MAX_MS: u64 = 5_000;

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

// Redis cluster dependencies plus its subscriber Redis 集群依赖及其订阅器
pub struct RedisClusterParts {
    // Core cluster dependency 核心集群依赖
    pub cluster: Cluster,
    // Subscriber that consumes envelopes for the local node 消费本地节点信封的订阅器
    pub subscriber: RedisNodeSubscriberAdapter,
}

// Managed Redis subscriber task handle 托管的 Redis 订阅任务句柄
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

    // Build the Redis pattern for all session route keys 构建全部会话路由 Redis key 的匹配模式
    fn key_for_session_pattern(&self) -> String {
        format!("{}:presence:session:*", self.config.key_prefix)
    }

    // Collect live session routes by scanning session route keys 扫描会话路由 key 以收集活跃路由
    async fn collect_session_routes(
        &self,
        connection_type: Option<&ConnectionType>,
    ) -> Result<Vec<Route>> {
        let mut connection = self.connection.clone();
        let pattern = self.key_for_session_pattern();
        let mut cursor = 0_u64;
        let mut routes = Vec::new();

        loop {
            let (next_cursor, keys) = redis::cmd("SCAN")
                .cursor_arg(cursor)
                .arg("MATCH")
                .arg(&pattern)
                .arg("COUNT")
                .arg(100)
                .query_async::<(u64, Vec<String>)>(&mut connection)
                .await
                .map_err(|error| redis_error("scan redis presence session routes", error))?;

            for key in keys {
                let payload = connection
                    .get::<_, Option<Vec<u8>>>(&key)
                    .await
                    .map_err(|error| redis_error("load redis presence session route", error))?;
                let Some(payload) = payload else {
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

            if next_cursor == 0 {
                break;
            }
            cursor = next_cursor;
        }

        Ok(routes)
    }

    // Check whether the route owner still has an active node lease 检查路由所属节点是否仍有活跃租约
    async fn route_node_is_live(
        &self,
        connection: &mut ConnectionManager,
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

// Build a Redis-backed RustWing runtime from core configuration 通过核心配置创建 Redis 版 RustWing 运行时
// Build a Redis-backed RustWing runtime from one Redis URL 通过单个 Redis 地址创建 Redis 版 RustWing 运行时
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
        redis::pipe()
            .atomic()
            .hdel(&key, session_id.as_str())
            .del(&session_key)
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
                Some(payload) => routes.push(serde_json::from_slice(&payload)?),
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
        let payload = connection
            .get::<_, Option<Vec<u8>>>(key)
            .await
            .map_err(|error| redis_error("locate redis presence session route", error))?;
        payload
            .map(|payload| serde_json::from_slice(&payload).map_err(RustWingError::from))
            .transpose()
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

        let owner = connection
            .get::<_, Option<String>>(&key)
            .await
            .map_err(|error| redis_error("read redis node lease", error))?;
        if owner.as_deref() != Some(instance_id) {
            return Ok(NodeLease::Conflict);
        }
        connection
            .expire::<_, ()>(&key, ttl as i64)
            .await
            .map_err(|error| redis_error("refresh redis node lease", error))?;
        Ok(NodeLease::Refreshed)
    }

    // Remove one Redis node lease if still owned by the instance 当前实例仍持有时删除 Redis 节点租约
    async fn unregister_node(&self, node_id: &NodeId, instance_id: &str) -> Result<()> {
        // Clone the manager because Redis commands need a mutable connection 克隆连接管理器以满足命令的可变访问
        let mut connection = self.connection.clone();
        let key = self.key_for_node_lease(node_id);
        let owner = connection
            .get::<_, Option<String>>(&key)
            .await
            .map_err(|error| redis_error("read redis node lease before remove", error))?;
        if owner.as_deref() == Some(instance_id) {
            connection
                .del::<_, ()>(&key)
                .await
                .map_err(|error| redis_error("remove redis node lease", error))?;
        }
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
                .consume_node_channel_once(&channel, &wing, &mut stop_rx)
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
    ) -> Result<()> {
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
        "{}:presence:{}:{}",
        key_prefix,
        connection_type.as_str(),
        user_id.as_str()
    )
}

// Build the Redis key for one session route 构建单个会话路由的 Redis key
fn redis_presence_session_key(key_prefix: &str, session_id: &SessionId) -> String {
    format!("{}:presence:session:{}", key_prefix, session_id.as_str())
}

// Build the Redis set key for route-owning nodes 构建拥有路由的节点集合 key
fn redis_presence_nodes_key(key_prefix: &str) -> String {
    format!("{key_prefix}:presence:nodes")
}

// Build the Redis key for one node lease 构建单个节点租约的 Redis key
fn redis_presence_node_lease_key(key_prefix: &str, node_id: &NodeId) -> String {
    format!("{}:presence:node:{}", key_prefix, node_id.as_str())
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
        assert_eq!(admin, "rust-wing:presence:admin:alice");
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

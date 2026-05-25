use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use rust_wing_core::{
    Cluster, ClusterBackendConfig, ClusterConfig, ClusterEnvelope, ConnectionPolicy, Identity,
    MemoryPresenceStore, NodeId, NodePublisher, OutboundFrame, PresenceStore, Result, Route,
    RustWing, RustWingConfig, RustWingError, UserId,
};

// Replacing a user session keeps only the newest connection 替换用户会话时仅保留最新连接
#[tokio::test]
async fn single_connection_policy_replaces_previous_session() {
    // Build the default single-connection manager 构建默认单连接管理器
    let wing = RustWing::new(RustWingConfig::default());

    // Accept two sessions for the same user 依次接收同一用户的两个会话
    let first = wing.accept(Identity::new("alice")).await.unwrap();
    let first_id = first.session.id().clone();
    let second = wing.accept(Identity::new("alice")).await.unwrap();

    // Confirm the first session was replaced 确认首个会话已被替换
    assert!(first.session.is_closed());
    assert_ne!(first_id, *second.session.id());
    assert_eq!(wing.connection_count().unwrap(), 1);
}

// Cluster configuration defaults to the memory backend 集群配置默认使用内存后端
#[test]
fn cluster_config_defaults_to_memory_backend() {
    // Read the default cluster configuration 读取默认集群配置
    let config = ClusterConfig::default();

    // Confirm memory is the default backend choice 确认默认后端选择为内存
    assert_eq!(config.backend, ClusterBackendConfig::Memory);
}

// Config-driven construction accepts the default memory backend 配置驱动构造接受默认内存后端
#[tokio::test]
async fn from_config_builds_memory_backend() {
    // Enable clustering while keeping the default backend 启用集群并保留默认后端
    let config = RustWingConfig {
        cluster: ClusterConfig {
            enabled: true,
            ..ClusterConfig::default()
        },
        ..RustWingConfig::default()
    };

    // Build a manager from configuration and accept a session 通过配置构建管理器并接收一个会话
    let wing = RustWing::from_config(config).await.unwrap();
    let accepted = wing.accept(Identity::new("alice")).await.unwrap();

    // Confirm the manager remains usable with the memory backend 确认内存后端下管理器仍可正常使用
    assert_eq!(wing.connection_count().unwrap(), 1);
    assert_eq!(accepted.session.user_id(), &UserId::from("alice"));
}

// Redis selection requires an explicit non-empty URL Redis 选择需要显式非空地址
#[tokio::test]
async fn from_config_rejects_empty_redis_url() {
    // Select Redis without providing a usable URL 选择 Redis 但不提供可用地址
    let config = RustWingConfig {
        cluster: ClusterConfig {
            enabled: true,
            backend: ClusterBackendConfig::Redis { url: " ".into() },
            ..ClusterConfig::default()
        },
        ..RustWingConfig::default()
    };

    // Confirm invalid Redis configuration is rejected 确认无效 Redis 配置会被拒绝
    let result = RustWing::from_config(config).await;
    assert!(matches!(result, Err(RustWingError::InvalidConfig(_))));
}

// Redis selection is explicit before the backend exists Redis 后端存在前选择仍需显式暴露
#[tokio::test]
async fn from_config_reports_unavailable_redis_backend() {
    // Select Redis with a usable URL 使用可用地址选择 Redis
    let config = RustWingConfig {
        cluster: ClusterConfig {
            enabled: true,
            backend: ClusterBackendConfig::Redis {
                url: "redis://127.0.0.1:6379".into(),
            },
            ..ClusterConfig::default()
        },
        ..RustWingConfig::default()
    };

    // Confirm the current crate reports the missing implementation 明确当前 crate 还没有对应实现
    let result = RustWing::from_config(config).await;
    assert!(matches!(
        result,
        Err(RustWingError::BackendUnavailable(name)) if name == "redis"
    ));
}

// Multi-connection mode preserves every session 多连接模式会保留全部会话
#[tokio::test]
async fn multi_connection_policy_keeps_all_sessions() {
    // Enable multi-connection behavior 启用多连接行为
    let config = RustWingConfig {
        connection_policy: ConnectionPolicy::Multi,
        ..RustWingConfig::default()
    };
    let wing = RustWing::new(config);

    // Accept two sessions for the same user 接收同一用户的两个会话
    let _first = wing.accept(Identity::new("alice")).await.unwrap();
    let _second = wing
        .accept(Identity::new("alice").with_device("phone"))
        .await
        .unwrap();

    // Verify both sessions remain registered 验证两个会话都仍被注册
    assert_eq!(wing.connection_count().unwrap(), 2);
    assert_eq!(
        wing.list_user_sessions(&UserId::from("alice"))
            .unwrap()
            .len(),
        2
    );
}

// Concurrent accepts keep registry indexes consistent 并发接入会保持注册表索引一致
#[tokio::test]
async fn concurrent_accepts_keep_registry_consistent() {
    // Enable multi-connection behavior so every task owns a distinct session 启用多连接行为以保留每个任务的独立会话
    let wing = RustWing::new(RustWingConfig {
        connection_policy: ConnectionPolicy::Multi,
        ..RustWingConfig::default()
    });

    // Accept many users concurrently to exercise the sharded registry 并发接入多个用户以验证分片注册表
    let mut tasks = Vec::new();
    for index in 0..64 {
        let wing = wing.clone();
        tasks.push(tokio::spawn(async move {
            let user_id = format!("user-{index}");
            wing.accept(Identity::new(user_id)).await.unwrap();
        }));
    }

    // Wait for all accepts before reading registry snapshots 读取注册表快照前等待全部接入完成
    for task in tasks {
        task.await.unwrap();
    }

    // Confirm both primary and reverse indexes are populated 确认主索引和反向索引都已填充
    assert_eq!(wing.connection_count().unwrap(), 64);
    assert_eq!(
        wing.list_user_sessions(&UserId::from("user-7"))
            .unwrap()
            .len(),
        1
    );
}

// Local sessions are preferred before cluster routing 集群路由前优先使用本地会话
#[tokio::test]
async fn send_to_user_prefers_local_session() {
    // Build a manager with one local session 构建包含一个本地会话的管理器
    let wing = RustWing::new(RustWingConfig::default());
    let accepted = wing.accept(Identity::new("alice")).await.unwrap();

    // Send one frame to the local user 向本地用户发送一帧
    let sent = wing
        .send_to_user("alice", OutboundFrame::text("hello"))
        .await
        .unwrap();

    // Confirm the local delivery path was used 确认使用了本地投递路径
    assert_eq!(sent, 1);
    drop(accepted);
}

// Remote routes publish to the owning node 远端路由会发布到归属节点
#[tokio::test]
async fn remote_route_publishes_to_target_node() {
    // Seed a remote route in presence storage 向在线状态存储写入远端路由
    let presence = MemoryPresenceStore::new();
    presence
        .register(
            Route {
                user_id: UserId::from("alice"),
                session_id: "remote-session".into(),
                node_id: NodeId::from("node-b"),
            },
            std::time::Duration::from_secs(60),
        )
        .await
        .unwrap();

    // Capture remote publish attempts 记录远端发布尝试
    let publisher = RecordingPublisher::default();
    let published = publisher.published.clone();
    let cluster = Cluster::new(presence, publisher);
    let config = RustWingConfig {
        node_id: NodeId::from("node-a"),
        cluster: ClusterConfig {
            enabled: true,
            ..ClusterConfig::default()
        },
        ..RustWingConfig::default()
    };
    let wing = RustWing::with_cluster(config, Some(cluster));

    // Send a frame that must leave the current node 发送一帧必须离开当前节点的消息
    let sent = wing
        .send_to_user("alice", OutboundFrame::text("hello"))
        .await
        .unwrap();

    // Verify publication targeted the remote owner 验证发布目标为远端归属节点
    let published = published.lock().unwrap();
    assert_eq!(sent, 1);
    assert_eq!(published.len(), 1);
    assert_eq!(published[0].0, NodeId::from("node-b"));
    assert_eq!(published[0].1.user_id, UserId::from("alice"));
}

// Multi-route presence fans out across remote nodes 多路由在线状态会向多个远端节点扇出
#[tokio::test]
async fn remote_routes_publish_once_per_node() {
    // Seed multiple remote routes, including two sessions on one node 写入多条远端路由，其中同一节点含两个会话
    let presence = MemoryPresenceStore::new();
    for (session_id, node_id) in [
        ("remote-session-a", "node-b"),
        ("remote-session-b", "node-b"),
        ("remote-session-c", "node-c"),
    ] {
        presence
            .register(
                Route {
                    user_id: UserId::from("alice"),
                    session_id: session_id.into(),
                    node_id: NodeId::from(node_id),
                },
                std::time::Duration::from_secs(60),
            )
            .await
            .unwrap();
    }

    // Capture every cross-node publish 记录每次跨节点发布
    let publisher = RecordingPublisher::default();
    let published = publisher.published.clone();
    let cluster = Cluster::new(presence, publisher);
    let config = RustWingConfig {
        node_id: NodeId::from("node-a"),
        cluster: ClusterConfig {
            enabled: true,
            ..ClusterConfig::default()
        },
        ..RustWingConfig::default()
    };
    let wing = RustWing::with_cluster(config, Some(cluster));

    // Send one user message through the cluster 向集群发送一条用户消息
    let sent = wing
        .send_to_user("alice", OutboundFrame::text("hello"))
        .await
        .unwrap();

    // Verify each remote node receives one publish 验证每个远端节点只收到一次发布
    let published = published.lock().unwrap();
    let mut nodes = published
        .iter()
        .map(|entry| entry.0.clone())
        .collect::<Vec<_>>();
    nodes.sort();
    assert_eq!(sent, 2);
    assert_eq!(nodes, vec![NodeId::from("node-b"), NodeId::from("node-c")]);
}

// Heartbeats update timestamps and return negotiated timings 心跳会更新时间戳并返回协商参数
#[tokio::test]
async fn heartbeat_updates_session_state() {
    // Build a manager with custom heartbeat settings 构建带自定义心跳设置的管理器
    let config = RustWingConfig {
        heartbeat_interval: std::time::Duration::from_secs(5),
        heartbeat_timeout: std::time::Duration::from_secs(20),
        ..RustWingConfig::default()
    };
    let wing = RustWing::new(config);
    let accepted = wing.accept(Identity::new("alice")).await.unwrap();

    // Record one heartbeat carrying a client timestamp 记录一次带客户端时间戳的心跳
    let ack = wing
        .handle_heartbeat(&accepted.session, Some(123))
        .await
        .unwrap();
    let snapshot = accepted.session.snapshot();

    // Confirm both state and acknowledgement fields were updated 确认状态和确认字段都已更新
    assert_eq!(snapshot.client_heartbeat_time, 123);
    assert!(snapshot.last_heartbeat_time > 0);
    assert_eq!(ack.client_heartbeat_time, 123);
    assert_eq!(ack.last_heartbeat_time, snapshot.last_heartbeat_time);
    assert_eq!(ack.heartbeat_interval_ms, 5_000);
    assert_eq!(ack.heartbeat_timeout_ms, 20_000);
}

// Inactive sessions can be reaped by the manager 管理器可以回收不活跃会话
#[tokio::test]
async fn inactive_sessions_are_reaped() {
    // Use a short timeout so the test can cross the inactivity boundary 使用较短超时以便测试跨过不活跃边界
    let config = RustWingConfig {
        heartbeat_timeout: std::time::Duration::from_millis(10),
        ..RustWingConfig::default()
    };
    let wing = RustWing::new(config);
    let accepted = wing.accept(Identity::new("alice")).await.unwrap();

    // Wait long enough for the session to become inactive 等待足够时间让会话变为不活跃
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    let removed = wing.reap_inactive_sessions().await.unwrap();

    // Confirm the stale session was removed and closed 确认过期会话已被移除并关闭
    assert_eq!(removed, 1);
    assert!(accepted.session.is_closed());
    assert_eq!(wing.connection_count().unwrap(), 0);
}

// Test publisher that records envelopes 测试用记录型发布器
#[derive(Default)]
struct RecordingPublisher {
    // Published node and envelope pairs 已发布的节点与信封组合
    published: Arc<Mutex<Vec<(NodeId, ClusterEnvelope)>>>,
}

#[async_trait]
impl NodePublisher for RecordingPublisher {
    // Store each publish request for later assertions 保存每次发布请求以供后续断言
    async fn publish(&self, node_id: &NodeId, envelope: ClusterEnvelope) -> Result<()> {
        self.published
            .lock()
            .unwrap()
            .push((node_id.clone(), envelope));
        Ok(())
    }
}

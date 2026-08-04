use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use rust_wing_core::{
    AckStage, Cluster, ClusterConfig, ClusterEnvelope, ClusterTarget, ConnectionPolicy,
    ConnectionType, FrameKind, Identity, MemoryPresenceStore, NodeId, NodeLease, NodePublisher,
    OutboundFrame, PresenceStore, Result, Route, RustWing, RustWingConfig, RustWingError,
    SessionId, UserId,
};

// Default single-client policy replaces only the same client 默认单客户端策略仅替换同一客户端
#[tokio::test]
async fn default_single_client_policy_replaces_previous_session() {
    // Build the default single-client manager 构建默认单客户端管理器
    let wing = RustWing::new(RustWingConfig::default());

    // Accept two sessions for the same default client 依次接收同一默认客户端的两个会话
    let first = wing
        .accept(Identity::new("default", "alice"))
        .await
        .unwrap();
    let first_id = first.session.id().clone();
    let second = wing
        .accept(Identity::new("default", "alice"))
        .await
        .unwrap();

    // Confirm the first session was replaced 确认首个会话已被替换
    assert!(first.session.is_closed());
    assert_ne!(first_id, *second.session.id());
    assert_eq!(wing.connection_count().unwrap(), 1);
}

// Default helpers use the default connection system 默认快捷方法使用默认连接体系
#[tokio::test]
async fn default_connection_helpers_route_through_default_type() {
    let wing = RustWing::new(
        RustWingConfig::default().with_default_connection_policy(ConnectionPolicy::MultiSession),
    );
    let accepted = wing.accept_user("alice").await.unwrap();
    let explicit = wing.accept(Identity::new("admin", "alice")).await.unwrap();

    let report = wing
        .send_to_user("alice", OutboundFrame::text("hello default"))
        .await
        .unwrap();
    let default_sessions = wing.list_user_sessions(&UserId::from("alice")).unwrap();
    let admin_sessions = wing
        .list_user_sessions_in(&ConnectionType::from("admin"), &UserId::from("alice"))
        .unwrap();

    assert_eq!(
        accepted.session.connection_type(),
        &ConnectionType::default()
    );
    assert_eq!(
        explicit.session.connection_type(),
        &ConnectionType::from("admin")
    );
    assert_eq!(report.local_sessions, 1);
    assert_eq!(default_sessions.len(), 1);
    assert_eq!(admin_sessions.len(), 1);
}

// Local session listing can be scoped by connection system 本地会话列表可以按连接体系筛选
#[tokio::test]
async fn list_sessions_can_filter_by_connection_type() {
    let wing = RustWing::new(
        RustWingConfig::default().with_default_connection_policy(ConnectionPolicy::MultiSession),
    );

    let _default_alice = wing.accept_user("alice").await.unwrap();
    let _default_bob = wing.accept_user("bob").await.unwrap();
    let _admin = wing.accept(Identity::new("admin", "root")).await.unwrap();

    let all_sessions = wing.list_sessions().unwrap();
    let default_sessions = wing.list_sessions_in(&ConnectionType::default()).unwrap();
    let admin_sessions = wing
        .list_sessions_in(&ConnectionType::from("admin"))
        .unwrap();

    assert_eq!(all_sessions.len(), 3);
    assert_eq!(default_sessions.len(), 2);
    assert_eq!(admin_sessions.len(), 1);
    assert!(
        admin_sessions
            .iter()
            .all(|session| session.connection_type == ConnectionType::from("admin"))
    );
}

// Single-user policy replaces sessions across different clients 单用户策略会跨不同客户端替换会话
#[tokio::test]
async fn single_user_policy_replaces_different_clients() {
    let wing = RustWing::new(
        RustWingConfig::default().with_default_connection_policy(ConnectionPolicy::UniqueUser),
    );

    let first = wing
        .accept(Identity::new("default", "alice").with_client("phone"))
        .await
        .unwrap();
    let second = wing
        .accept(Identity::new("default", "alice").with_client("browser"))
        .await
        .unwrap();

    assert!(first.session.is_closed());
    assert!(!second.session.is_closed());
    assert_eq!(wing.connection_count().unwrap(), 1);
}

// Single-client policy keeps different clients but replaces the same client 单客户端策略保留不同客户端但替换同一客户端
#[tokio::test]
async fn single_client_policy_replaces_only_matching_client() {
    let wing = RustWing::new(RustWingConfig::default());

    let phone = wing
        .accept(Identity::new("default", "alice").with_client("phone"))
        .await
        .unwrap();
    let browser = wing
        .accept(Identity::new("default", "alice").with_client("browser"))
        .await
        .unwrap();
    let newer_phone = wing
        .accept(Identity::new("default", "alice").with_client("phone"))
        .await
        .unwrap();

    assert!(phone.session.is_closed());
    assert!(!browser.session.is_closed());
    assert!(!newer_phone.session.is_closed());
    assert_eq!(wing.connection_count().unwrap(), 2);
}

// Multi policy keeps repeated sessions for the same client 多连接策略会保留同一客户端的重复会话
#[tokio::test]
async fn multi_policy_keeps_same_client_sessions() {
    let wing = RustWing::new(
        RustWingConfig::default().with_default_connection_policy(ConnectionPolicy::MultiSession),
    );

    let _first = wing
        .accept(Identity::new("default", "alice").with_client("phone"))
        .await
        .unwrap();
    let _second = wing
        .accept(Identity::new("default", "alice").with_client("phone"))
        .await
        .unwrap();

    assert_eq!(wing.connection_count().unwrap(), 2);
}

// Connection systems can use independent coexistence policies 连接体系可以使用独立的共存策略
#[tokio::test]
async fn connection_type_policy_overrides_are_isolated() {
    let wing = RustWing::new(
        RustWingConfig::default().with_connection_policy("game", ConnectionPolicy::MultiSession),
    );

    let game_first = wing
        .accept(Identity::new("game", "alice").with_client("phone"))
        .await
        .unwrap();
    let game_second = wing
        .accept(Identity::new("game", "alice").with_client("phone"))
        .await
        .unwrap();
    let user_first = wing
        .accept(Identity::new("user", "alice").with_client("phone"))
        .await
        .unwrap();
    let user_second = wing
        .accept(Identity::new("user", "alice").with_client("phone"))
        .await
        .unwrap();

    assert!(!game_first.session.is_closed());
    assert!(!game_second.session.is_closed());
    assert!(user_first.session.is_closed());
    assert!(!user_second.session.is_closed());
    assert_eq!(
        wing.list_user_sessions_in(&ConnectionType::from("game"), &UserId::from("alice"))
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        wing.list_user_sessions_in(&ConnectionType::from("user"), &UserId::from("alice"))
            .unwrap()
            .len(),
        1
    );
}

// Cluster configuration defaults to standalone mode 集群配置默认使用单机模式
#[test]
fn cluster_config_defaults_to_standalone_mode() {
    // Read the default cluster configuration 读取默认集群配置
    let config = ClusterConfig::default();

    // Confirm distributed routing is opt-in 确认分布式路由需要显式启用
    assert!(!config.enabled);
    assert_eq!(config.route_ttl, Duration::from_secs(90));
    assert_eq!(config.node_lease_ttl, Duration::from_secs(30));
}

// Node id can be configured with a builder helper 节点标识可以通过构建辅助方法配置
#[tokio::test]
async fn node_id_builder_sets_session_owner() {
    let wing = RustWing::new(RustWingConfig::default().with_node_id("ws-1"));
    let accepted = wing.accept_user("alice").await.unwrap();

    assert_eq!(wing.config().node_id, NodeId::from("ws-1"));
    assert_eq!(accepted.session.node_id(), &NodeId::from("ws-1"));
    assert!(accepted.session.id().as_str().starts_with("ws-1-"));
    assert_eq!(accepted.session.id().as_str().len(), "ws-1-".len() + 32);
}

// Config-driven construction requires adapter-provided cluster dependencies 配置驱动构造要求适配器提供集群依赖
#[tokio::test]
async fn from_config_rejects_enabled_cluster_without_adapters() {
    // Enable clustering without injecting route or message adapters 启用集群但不注入路由或消息适配器
    let config = RustWingConfig {
        cluster: ClusterConfig {
            enabled: true,
            ..ClusterConfig::default()
        },
        ..RustWingConfig::default()
    };

    // Confirm core does not guess infrastructure backends 确认 core 不自行猜测基础设施后端
    let result = RustWing::from_config(config).await;
    assert!(matches!(
        result,
        Err(RustWingError::InvalidConfig(message))
            if message.contains("adapter-provided cluster dependencies")
    ));
}

// Checked cluster construction rejects duplicate live node ids 校验式集群构造会拒绝重复的活跃节点标识
#[tokio::test]
async fn checked_cluster_rejects_duplicate_node_id() {
    let presence = SharedPresenceStore::default();
    let first_cluster = Cluster::new(presence.clone(), RecordingPublisher::default());
    let second_cluster = Cluster::new(presence, RecordingPublisher::default());
    let config = RustWingConfig {
        node_id: NodeId::from("node-a"),
        cluster: ClusterConfig {
            enabled: true,
            ..ClusterConfig::default()
        },
        ..RustWingConfig::default()
    };

    let _first = RustWing::with_cluster_checked(config.clone(), Some(first_cluster))
        .await
        .unwrap();
    let second = RustWing::with_cluster_checked(config, Some(second_cluster)).await;

    assert!(matches!(
        second,
        Err(RustWingError::InvalidConfig(message)) if message.contains("node_id 'node-a' is already active")
    ));
}

// Checked cluster construction keeps node leases refreshed 校验式集群构造会持续刷新节点租约
#[tokio::test]
async fn checked_cluster_refreshes_node_lease() {
    let presence = SharedPresenceStore::default();
    let first_cluster = Cluster::new(presence.clone(), RecordingPublisher::default());
    let second_cluster = Cluster::new(presence, RecordingPublisher::default());
    let config = RustWingConfig {
        node_id: NodeId::from("node-a"),
        cluster: ClusterConfig {
            enabled: true,
            node_lease_ttl: Duration::from_millis(300),
            ..ClusterConfig::default()
        },
        ..RustWingConfig::default()
    };

    let _first = RustWing::with_cluster_checked(config.clone(), Some(first_cluster))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(1200)).await;
    let second = RustWing::with_cluster_checked(config, Some(second_cluster)).await;

    assert!(matches!(
        second,
        Err(RustWingError::InvalidConfig(message)) if message.contains("node_id 'node-a' is already active")
    ));
}

// Shutdown unregisters sessions and releases the node lease 关闭会注销会话并释放节点租约
#[tokio::test]
async fn shutdown_releases_sessions_and_node_lease() {
    let presence = SharedPresenceStore::default();
    let first_cluster = Cluster::new(presence.clone(), RecordingPublisher::default());
    let second_cluster = Cluster::new(presence, RecordingPublisher::default());
    let config = RustWingConfig {
        node_id: NodeId::from("node-a"),
        cluster: ClusterConfig {
            enabled: true,
            ..ClusterConfig::default()
        },
        ..RustWingConfig::default()
    };

    let first = RustWing::with_cluster_checked(config.clone(), Some(first_cluster))
        .await
        .unwrap();
    let accepted = first.accept_user("alice").await.unwrap();

    let closed = first.shutdown().await.unwrap();
    let second = RustWing::with_cluster_checked(config, Some(second_cluster)).await;

    assert_eq!(closed, 1);
    assert!(accepted.session.is_closed());
    assert_eq!(first.connection_count().unwrap(), 0);
    assert!(second.is_ok());
}

// Cluster unique-client policy displaces matching remote sessions 集群单客户端策略会替换匹配的远端会话
#[tokio::test]
async fn cluster_unique_client_accept_closes_matching_remote_session() {
    let presence = SharedPresenceStore::default();
    presence
        .register_node(
            &NodeId::from("node-b"),
            "instance-node-b",
            Duration::from_secs(60),
        )
        .await
        .unwrap();
    presence
        .register(
            Route {
                connection_type: ConnectionType::from("default"),
                user_id: UserId::from("alice"),
                client_id: Some("phone".into()),
                session_id: "remote-phone".into(),
                node_id: NodeId::from("node-b"),
            },
            Duration::from_secs(60),
        )
        .await
        .unwrap();
    presence
        .register(
            Route {
                connection_type: ConnectionType::from("default"),
                user_id: UserId::from("alice"),
                client_id: Some("browser".into()),
                session_id: "remote-browser".into(),
                node_id: NodeId::from("node-b"),
            },
            Duration::from_secs(60),
        )
        .await
        .unwrap();

    let publisher = RecordingPublisher::default();
    let published = publisher.published.clone();
    let cluster = Cluster::new(presence.clone(), publisher);
    let wing = RustWing::with_cluster_unchecked(
        RustWingConfig {
            node_id: NodeId::from("node-a"),
            cluster: ClusterConfig {
                enabled: true,
                ..ClusterConfig::default()
            },
            ..RustWingConfig::default()
        },
        Some(cluster),
    );

    let accepted = wing
        .accept(Identity::new("default", "alice").with_client("phone"))
        .await
        .unwrap();

    let published = published.lock().unwrap();
    assert!(!accepted.session.is_closed());
    assert_eq!(published.len(), 1);
    assert_eq!(published[0].0, NodeId::from("node-b"));
    assert_eq!(published[0].1.frame_kind, FrameKind::Close);
    assert_eq!(
        published[0].1.target,
        ClusterTarget::Session {
            session_id: "remote-phone".into()
        }
    );
    assert!(
        presence
            .locate_session(&SessionId::from("remote-phone"))
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        presence
            .locate_session(&SessionId::from("remote-browser"))
            .await
            .unwrap()
            .is_some()
    );
}

// Cluster unique-user policy displaces all remote user sessions 集群单用户策略会替换用户的全部远端会话
#[tokio::test]
async fn cluster_unique_user_accept_closes_all_remote_user_sessions() {
    let presence = SharedPresenceStore::default();
    presence
        .register_node(
            &NodeId::from("node-b"),
            "instance-node-b",
            Duration::from_secs(60),
        )
        .await
        .unwrap();
    for (client_id, session_id) in [("phone", "remote-phone"), ("browser", "remote-browser")] {
        presence
            .register(
                Route {
                    connection_type: ConnectionType::from("default"),
                    user_id: UserId::from("alice"),
                    client_id: Some(client_id.into()),
                    session_id: session_id.into(),
                    node_id: NodeId::from("node-b"),
                },
                Duration::from_secs(60),
            )
            .await
            .unwrap();
    }

    let publisher = RecordingPublisher::default();
    let published = publisher.published.clone();
    let cluster = Cluster::new(presence.clone(), publisher);
    let wing = RustWing::with_cluster_unchecked(
        RustWingConfig {
            node_id: NodeId::from("node-a"),
            default_connection_policy: ConnectionPolicy::UniqueUser,
            cluster: ClusterConfig {
                enabled: true,
                ..ClusterConfig::default()
            },
            ..RustWingConfig::default()
        },
        Some(cluster),
    );

    let _accepted = wing
        .accept(Identity::new("default", "alice").with_client("tablet"))
        .await
        .unwrap();

    let published = published.lock().unwrap();
    let mut closed_sessions = published
        .iter()
        .filter_map(|entry| match &entry.1.target {
            ClusterTarget::Session { session_id } => Some(session_id.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
    closed_sessions.sort();
    assert_eq!(
        closed_sessions,
        vec![
            SessionId::from("remote-browser"),
            SessionId::from("remote-phone")
        ]
    );
    assert!(
        presence
            .locate(&ConnectionType::from("default"), &UserId::from("alice"))
            .await
            .unwrap()
            .iter()
            .all(|route| route.node_id == NodeId::from("node-a"))
    );
}

// Cluster multi-session policy keeps matching remote sessions 集群多会话策略会保留匹配的远端会话
#[tokio::test]
async fn cluster_multi_session_accept_keeps_remote_session() {
    let presence = SharedPresenceStore::default();
    presence
        .register_node(
            &NodeId::from("node-b"),
            "instance-node-b",
            Duration::from_secs(60),
        )
        .await
        .unwrap();
    presence
        .register(
            Route {
                connection_type: ConnectionType::from("default"),
                user_id: UserId::from("alice"),
                client_id: Some("phone".into()),
                session_id: "remote-phone".into(),
                node_id: NodeId::from("node-b"),
            },
            Duration::from_secs(60),
        )
        .await
        .unwrap();

    let publisher = RecordingPublisher::default();
    let published = publisher.published.clone();
    let cluster = Cluster::new(presence.clone(), publisher);
    let wing = RustWing::with_cluster_unchecked(
        RustWingConfig {
            node_id: NodeId::from("node-a"),
            default_connection_policy: ConnectionPolicy::MultiSession,
            cluster: ClusterConfig {
                enabled: true,
                ..ClusterConfig::default()
            },
            ..RustWingConfig::default()
        },
        Some(cluster),
    );

    let _accepted = wing
        .accept(Identity::new("default", "alice").with_client("phone"))
        .await
        .unwrap();

    assert!(published.lock().unwrap().is_empty());
    assert!(
        presence
            .locate_session(&SessionId::from("remote-phone"))
            .await
            .unwrap()
            .is_some()
    );
}

// Cluster close envelopes mark the target local session closed 集群关闭信封会标记目标本地会话为关闭
#[tokio::test]
async fn cluster_close_envelope_marks_session_closed() {
    let wing = RustWing::new(RustWingConfig::default());
    let mut accepted = wing.accept_user("alice").await.unwrap();
    let envelope = ClusterEnvelope::new_for_session(
        accepted.session.id().clone(),
        OutboundFrame::close("replaced by a newer connection"),
    );

    let delivered = wing.handle_cluster_envelope(envelope).unwrap();
    let close_frame = accepted.outbound.recv().await.unwrap();

    assert_eq!(delivered, 1);
    assert!(accepted.session.is_closed());
    assert_eq!(close_frame.kind, FrameKind::Close);
    assert_eq!(wing.connection_count().unwrap(), 0);
}

// Disconnecting a default-system user removes only that user's sessions 断开默认连接体系用户只移除该用户的会话
#[tokio::test]
async fn disconnect_user_removes_default_user_sessions() {
    let wing = RustWing::new(
        RustWingConfig::default().with_default_connection_policy(ConnectionPolicy::MultiSession),
    );
    let first = wing.accept_user("alice").await.unwrap();
    let second = wing.accept_client("alice", "phone").await.unwrap();
    let other = wing.accept_user("bob").await.unwrap();

    let report = wing.disconnect_user("alice", "signed out").await.unwrap();

    assert_eq!(report.local_sessions, 2);
    assert_eq!(report.remote_nodes, 0);
    assert!(first.session.is_closed());
    assert!(second.session.is_closed());
    assert!(!other.session.is_closed());
    assert_eq!(wing.connection_count().unwrap(), 1);
    assert_eq!(
        wing.stats_snapshot()
            .unwrap()
            .disconnected_local_sessions_total,
        2
    );
}

// Disconnecting a default-system client removes only the matching client slot 断开默认连接体系客户端只移除匹配的客户端槽位
#[tokio::test]
async fn disconnect_client_removes_default_client_sessions() {
    let wing = RustWing::new(
        RustWingConfig::default().with_default_connection_policy(ConnectionPolicy::MultiSession),
    );
    let phone = wing.accept_client("alice", "phone").await.unwrap();
    let browser = wing.accept_client("alice", "browser").await.unwrap();

    let report = wing
        .disconnect_client("alice", Some("phone"), "client replaced")
        .await
        .unwrap();

    assert_eq!(report.local_sessions, 1);
    assert!(phone.session.is_closed());
    assert!(!browser.session.is_closed());
    assert_eq!(wing.connection_count().unwrap(), 1);
}

// Disconnecting one exact local session leaves other sessions online 断开一条精确本地会话会保留其他会话在线
#[tokio::test]
async fn disconnect_session_removes_exact_local_session() {
    let wing = RustWing::new(
        RustWingConfig::default().with_default_connection_policy(ConnectionPolicy::MultiSession),
    );
    let first = wing.accept_user("alice").await.unwrap();
    let second = wing.accept_client("alice", "phone").await.unwrap();

    let report = wing
        .disconnect_session(first.session.id(), "session revoked")
        .await
        .unwrap();

    assert_eq!(report.local_sessions, 1);
    assert_eq!(report.remote_nodes, 0);
    assert!(first.session.is_closed());
    assert!(!second.session.is_closed());
    assert_eq!(wing.connection_count().unwrap(), 1);
}

// Sending a close frame to a session uses disconnect semantics 发送关闭帧到会话会使用断开语义
#[tokio::test]
async fn send_to_session_close_removes_exact_local_session() {
    let wing = RustWing::new(
        RustWingConfig::default().with_default_connection_policy(ConnectionPolicy::MultiSession),
    );
    let first = wing.accept_user("alice").await.unwrap();
    let second = wing.accept_client("alice", "phone").await.unwrap();

    let report = wing
        .send_to_session(first.session.id(), OutboundFrame::close("close frame"))
        .await
        .unwrap();

    assert_eq!(report.local_sessions, 1);
    assert!(first.session.is_closed());
    assert!(!second.session.is_closed());
    assert_eq!(wing.connection_count().unwrap(), 1);
}

// Broadcasting a close frame disconnects the targeted connection system 广播关闭帧会断开目标连接体系
#[tokio::test]
async fn broadcast_close_disconnects_default_connection_system() {
    let wing = RustWing::new(
        RustWingConfig::default().with_default_connection_policy(ConnectionPolicy::MultiSession),
    );
    let default_a = wing.accept_user("alice").await.unwrap();
    let default_b = wing.accept_user("bob").await.unwrap();
    let admin = wing.accept(Identity::new("admin", "root")).await.unwrap();

    let report = wing
        .broadcast(OutboundFrame::close("system shutdown"))
        .await
        .unwrap();

    assert_eq!(report.local_sessions, 2);
    assert!(default_a.session.is_closed());
    assert!(default_b.session.is_closed());
    assert!(!admin.session.is_closed());
    assert_eq!(wing.connection_count().unwrap(), 1);
}

// Broadcasting a global close frame disconnects every local session 广播全局关闭帧会断开全部本地会话
#[tokio::test]
async fn broadcast_all_close_disconnects_all_local_sessions() {
    let wing = RustWing::new(
        RustWingConfig::default().with_default_connection_policy(ConnectionPolicy::MultiSession),
    );
    let first = wing.accept_user("alice").await.unwrap();
    let second = wing.accept(Identity::new("admin", "root")).await.unwrap();

    let report = wing
        .broadcast_all(OutboundFrame::close("node shutdown"))
        .await
        .unwrap();

    assert_eq!(report.local_sessions, 2);
    assert!(first.session.is_closed());
    assert!(second.session.is_closed());
    assert_eq!(wing.connection_count().unwrap(), 0);
}

// Remote session disconnect publishes a close envelope and removes the route 远端会话断开会发布关闭信封并移除路由
#[tokio::test]
async fn disconnect_session_publishes_to_remote_owner() {
    let presence = SharedPresenceStore::default();
    presence
        .register_node(
            &NodeId::from("node-b"),
            "instance-node-b",
            Duration::from_secs(60),
        )
        .await
        .unwrap();
    presence
        .register(
            Route {
                connection_type: ConnectionType::from("default"),
                user_id: UserId::from("alice"),
                client_id: None,
                session_id: "remote-session".into(),
                node_id: NodeId::from("node-b"),
            },
            Duration::from_secs(60),
        )
        .await
        .unwrap();
    let publisher = RecordingPublisher::default();
    let published = publisher.published.clone();
    let wing = RustWing::with_cluster_unchecked(
        RustWingConfig {
            node_id: NodeId::from("node-a"),
            cluster: ClusterConfig {
                enabled: true,
                ..ClusterConfig::default()
            },
            ..RustWingConfig::default()
        },
        Some(Cluster::new(presence.clone(), publisher)),
    );

    let report = wing
        .disconnect_session(&SessionId::from("remote-session"), "kicked")
        .await
        .unwrap();

    let published = published.lock().unwrap();
    assert_eq!(report.local_sessions, 0);
    assert_eq!(report.remote_nodes, 1);
    assert_eq!(
        wing.stats_snapshot()
            .unwrap()
            .disconnected_remote_nodes_total,
        1
    );
    assert_eq!(published.len(), 1);
    assert_eq!(published[0].0, NodeId::from("node-b"));
    assert_eq!(published[0].1.frame_kind, FrameKind::Close);
    assert!(
        presence
            .locate_session(&SessionId::from("remote-session"))
            .await
            .unwrap()
            .is_none()
    );
}

// Multi-connection mode preserves every session 多连接模式会保留全部会话
#[tokio::test]
async fn multi_connection_policy_keeps_all_sessions() {
    // Enable multi-connection behavior 启用多连接行为
    let config =
        RustWingConfig::default().with_default_connection_policy(ConnectionPolicy::MultiSession);
    let wing = RustWing::new(config);

    // Accept two sessions for the same user 接收同一用户的两个会话
    let _first = wing
        .accept(Identity::new("default", "alice"))
        .await
        .unwrap();
    let _second = wing
        .accept(Identity::new("default", "alice").with_client("phone"))
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
    let wing = RustWing::new(
        RustWingConfig::default().with_default_connection_policy(ConnectionPolicy::MultiSession),
    );

    // Accept many users concurrently to exercise the sharded registry 并发接入多个用户以验证分片注册表
    let mut tasks = Vec::new();
    for index in 0..64 {
        let wing = wing.clone();
        tasks.push(tokio::spawn(async move {
            let user_id = format!("user-{index}");
            wing.accept(Identity::new("default", user_id))
                .await
                .unwrap();
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
    let accepted = wing
        .accept(Identity::new("default", "alice"))
        .await
        .unwrap();

    // Send one frame to the local user 向本地用户发送一帧
    let report = wing
        .send_to_user("alice", OutboundFrame::text("hello"))
        .await
        .unwrap();

    // Confirm the local delivery path was used 确认使用了本地投递路径
    assert_eq!(report.local_sessions, 1);
    assert_eq!(report.remote_nodes, 0);
    drop(accepted);
}

// Delivery report separates local sessions from remote nodes 投递报告会区分本地会话和远端节点
#[tokio::test]
async fn send_to_user_counts_local_sessions() {
    // Build a manager with one local session 构建包含一个本地会话的管理器
    let wing = RustWing::new(RustWingConfig::default());
    let accepted = wing
        .accept(Identity::new("default", "alice"))
        .await
        .unwrap();

    // Send one frame and inspect the structured report 发送一帧并检查结构化报告
    let report = wing
        .send_to_user("alice", OutboundFrame::text("hello"))
        .await
        .unwrap();

    // Confirm only the local session counter is incremented 确认只有本地会话计数增加
    assert_eq!(report.local_sessions, 1);
    assert_eq!(report.remote_nodes, 0);
    assert_eq!(report.delivered(), 1);
    drop(accepted);
}

// Runtime stats report local connections and enqueued frames 运行统计会报告本地连接与入队帧
#[tokio::test]
async fn stats_snapshot_reports_local_runtime_counts() {
    let wing = RustWing::new(
        RustWingConfig::default().with_default_connection_policy(ConnectionPolicy::MultiSession),
    );
    let _first = wing.accept_user("alice").await.unwrap();
    let _second = wing.accept_user("alice").await.unwrap();
    let message_id = wing.next_message_id();

    let initial = wing.stats_snapshot().unwrap();
    assert_eq!(initial.node_id, NodeId::from("local"));
    assert_eq!(initial.local_connections, 2);
    assert_eq!(initial.local_users, 1);
    assert_eq!(initial.ack_pending_messages, 0);
    assert_eq!(initial.cluster_nodes, 0);
    assert_eq!(initial.cluster_routes, 0);
    assert_eq!(initial.outbound_frames_enqueued_total, 0);
    assert_eq!(initial.outbound_frames_failed_total, 0);

    wing.send_to_user(
        "alice",
        OutboundFrame::text("tracked").require_ack(message_id),
    )
    .await
    .unwrap();
    let snapshot = wing.stats_snapshot().unwrap();

    assert_eq!(snapshot.local_connections, 2);
    assert_eq!(snapshot.local_users, 1);
    assert_eq!(snapshot.ack_pending_messages, 1);
    assert_eq!(snapshot.outbound_frames_enqueued_total, 2);
    assert_eq!(snapshot.outbound_frames_failed_total, 0);
}

// Client-targeted sends reach only the matching client 客户端定向发送只会命中匹配客户端
#[tokio::test]
async fn cluster_status_lists_empty_without_cluster() {
    let wing = RustWing::new(RustWingConfig::default());

    assert!(wing.list_cluster_nodes().await.unwrap().is_empty());
    assert!(
        wing.list_cluster_routes(&ConnectionType::default())
            .await
            .unwrap()
            .is_empty()
    );
    assert!(wing.list_all_cluster_routes().await.unwrap().is_empty());
}

// Cluster status APIs list memory-backed nodes and routes 集群状态接口会列出内存后端中的节点和路由
#[tokio::test]
async fn cluster_status_lists_memory_nodes_and_routes() {
    let presence = SharedPresenceStore::default();
    let cluster = Cluster::new(presence, RecordingPublisher::default());
    let wing = RustWing::with_cluster_checked(
        RustWingConfig {
            node_id: NodeId::from("node-a"),
            default_connection_policy: ConnectionPolicy::MultiSession,
            cluster: ClusterConfig {
                enabled: true,
                ..ClusterConfig::default()
            },
            ..RustWingConfig::default()
        },
        Some(cluster),
    )
    .await
    .unwrap();

    let accepted = wing
        .accept(Identity::new("default", "alice").with_client("browser"))
        .await
        .unwrap();
    let nodes = wing.list_cluster_nodes().await.unwrap();
    let routes = wing
        .list_cluster_routes(&ConnectionType::default())
        .await
        .unwrap();
    let all_routes = wing.list_all_cluster_routes().await.unwrap();
    let snapshot = wing.stats_snapshot().unwrap();

    assert_eq!(nodes, vec![NodeId::from("node-a")]);
    assert_eq!(routes.len(), 1);
    assert_eq!(routes[0].session_id, *accepted.session.id());
    assert_eq!(routes[0].client_id, Some("browser".into()));
    assert_eq!(all_routes, routes);
    assert_eq!(snapshot.cluster_nodes, 1);
    assert_eq!(snapshot.cluster_routes, 1);
}

// Cluster status skips expired memory routes 集群状态会跳过已过期的内存路由
#[tokio::test]
async fn cluster_status_omits_expired_routes() {
    let presence = SharedPresenceStore::default();
    let cluster = Cluster::new(presence, RecordingPublisher::default());
    let wing = RustWing::with_cluster_checked(
        RustWingConfig {
            node_id: NodeId::from("node-a"),
            heartbeat_interval: Duration::from_millis(5),
            heartbeat_timeout: Duration::from_millis(10),
            cluster: ClusterConfig {
                enabled: true,
                route_ttl: Duration::from_millis(20),
                ..ClusterConfig::default()
            },
            ..RustWingConfig::default()
        },
        Some(cluster),
    )
    .await
    .unwrap();

    let _accepted = wing.accept_user("alice").await.unwrap();
    tokio::time::sleep(Duration::from_millis(80)).await;

    assert!(wing.list_all_cluster_routes().await.unwrap().is_empty());
}

// Client-targeted sends reach only the matching client 客户端定向发送只会命中匹配客户端
#[tokio::test]
async fn send_to_client_counts_matching_local_sessions() {
    let wing = RustWing::new(
        RustWingConfig::default().with_default_connection_policy(ConnectionPolicy::MultiSession),
    );
    let _phone = wing
        .accept(Identity::new("default", "alice").with_client("phone"))
        .await
        .unwrap();
    let _browser = wing
        .accept(Identity::new("default", "alice").with_client("browser"))
        .await
        .unwrap();
    let _phone_tab = wing
        .accept(Identity::new("default", "alice").with_client("phone"))
        .await
        .unwrap();

    let report = wing
        .send_to_client("alice", Some("phone"), OutboundFrame::text("hello"))
        .await
        .unwrap();

    assert_eq!(report.local_sessions, 2);
    assert_eq!(report.remote_nodes, 0);
}

// Default client sends target sessions without a client id 默认客户端发送会命中没有客户端标识的会话
#[tokio::test]
async fn send_to_client_none_counts_default_client_sessions() {
    let wing = RustWing::new(
        RustWingConfig::default().with_default_connection_policy(ConnectionPolicy::MultiSession),
    );
    let _default = wing
        .accept(Identity::new("default", "alice"))
        .await
        .unwrap();
    let _phone = wing
        .accept(Identity::new("default", "alice").with_client("phone"))
        .await
        .unwrap();

    let report = wing
        .send_to_client::<&str>("alice", None, OutboundFrame::text("hello"))
        .await
        .unwrap();

    assert_eq!(report.local_sessions, 1);
    assert_eq!(report.remote_nodes, 0);
}

// Session-targeted sends reach exactly one local session 会话定向发送只会命中一个精确本地会话
#[tokio::test]
async fn send_to_session_counts_exact_local_session() {
    let wing = RustWing::new(
        RustWingConfig::default().with_default_connection_policy(ConnectionPolicy::MultiSession),
    );
    let first = wing
        .accept(Identity::new("default", "alice"))
        .await
        .unwrap();
    let _second = wing
        .accept(Identity::new("default", "alice"))
        .await
        .unwrap();

    let report = wing
        .send_to_session(first.session.id(), OutboundFrame::text("hello"))
        .await
        .unwrap();

    assert_eq!(report.local_sessions, 1);
    assert_eq!(report.remote_nodes, 0);
}

// Session-targeted sends can route to a remote node 会话定向发送可以路由到远端节点
#[tokio::test]
async fn send_to_session_publishes_to_remote_owner() {
    let presence = MemoryPresenceStore::new();
    register_test_node(&presence, "node-b").await;
    presence
        .register(
            Route {
                connection_type: ConnectionType::from("default"),
                user_id: UserId::from("alice"),
                client_id: None,
                session_id: "remote-session".into(),
                node_id: NodeId::from("node-b"),
            },
            std::time::Duration::from_secs(60),
        )
        .await
        .unwrap();

    let publisher = RecordingPublisher::default();
    let published = publisher.published.clone();
    let cluster = Cluster::new(presence, publisher);
    let wing = RustWing::with_cluster_unchecked(
        RustWingConfig {
            node_id: NodeId::from("node-a"),
            cluster: ClusterConfig {
                enabled: true,
                ..ClusterConfig::default()
            },
            ..RustWingConfig::default()
        },
        Some(cluster),
    );

    let report = wing
        .send_to_session(
            &SessionId::from("remote-session"),
            OutboundFrame::text("hello"),
        )
        .await
        .unwrap();

    let published = published.lock().unwrap();
    assert_eq!(report.local_sessions, 0);
    assert_eq!(report.remote_nodes, 1);
    assert_eq!(published.len(), 1);
    assert_eq!(published[0].0, NodeId::from("node-b"));
    assert_eq!(
        published[0].1.target,
        ClusterTarget::Session {
            session_id: SessionId::from("remote-session")
        }
    );
}

// Remote routes publish to the owning node 远端路由会发布到归属节点
#[tokio::test]
async fn remote_route_publishes_to_target_node() {
    // Seed a remote route in presence storage 向在线状态存储写入远端路由
    let presence = MemoryPresenceStore::new();
    register_test_node(&presence, "node-b").await;
    presence
        .register(
            Route {
                connection_type: ConnectionType::from("default"),
                user_id: UserId::from("alice"),
                client_id: None,
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
    let wing = RustWing::with_cluster_unchecked(config, Some(cluster));

    // Send a frame that must leave the current node 发送一帧必须离开当前节点的消息
    let report = wing
        .send_to_user("alice", OutboundFrame::text("hello"))
        .await
        .unwrap();

    // Verify publication targeted the remote owner 验证发布目标为远端归属节点
    let published = published.lock().unwrap();
    assert_eq!(report.local_sessions, 0);
    assert_eq!(report.remote_nodes, 1);
    assert_eq!(published.len(), 1);
    assert_eq!(published[0].0, NodeId::from("node-b"));
    assert_eq!(
        published[0].1.target,
        ClusterTarget::User {
            connection_type: ConnectionType::from("default"),
            user_id: UserId::from("alice")
        }
    );
}

// Client-targeted remote routes publish only matching clients 客户端定向远端路由只发布到匹配客户端
#[tokio::test]
async fn send_to_client_publishes_only_matching_remote_client() {
    // Seed remote client routes for the same user 写入同一用户的远端客户端路由
    let presence = MemoryPresenceStore::new();
    register_test_node(&presence, "node-b").await;
    register_test_node(&presence, "node-c").await;
    for (session_id, client_id, node_id) in [
        ("remote-phone", "phone", "node-b"),
        ("remote-browser", "browser", "node-c"),
    ] {
        presence
            .register(
                Route {
                    connection_type: ConnectionType::from("default"),
                    user_id: UserId::from("alice"),
                    client_id: Some(client_id.into()),
                    session_id: session_id.into(),
                    node_id: NodeId::from(node_id),
                },
                std::time::Duration::from_secs(60),
            )
            .await
            .unwrap();
    }

    // Capture the targeted cluster publish 记录定向集群发布
    let publisher = RecordingPublisher::default();
    let published = publisher.published.clone();
    let cluster = Cluster::new(presence, publisher);
    let wing = RustWing::with_cluster_unchecked(
        RustWingConfig {
            node_id: NodeId::from("node-a"),
            cluster: ClusterConfig {
                enabled: true,
                ..ClusterConfig::default()
            },
            ..RustWingConfig::default()
        },
        Some(cluster),
    );

    let report = wing
        .send_to_client("alice", Some("phone"), OutboundFrame::text("hello"))
        .await
        .unwrap();

    let published = published.lock().unwrap();
    assert_eq!(report.local_sessions, 0);
    assert_eq!(report.remote_nodes, 1);
    assert_eq!(published.len(), 1);
    assert_eq!(published[0].0, NodeId::from("node-b"));
    assert_eq!(
        published[0].1.target,
        ClusterTarget::Client {
            connection_type: ConnectionType::from("default"),
            user_id: UserId::from("alice"),
            client_id: Some("phone".into())
        }
    );
}

// Broadcast publishes only to remote nodes in the connection system 广播只会向连接体系内的远端节点发布
#[tokio::test]
async fn broadcast_publishes_to_remote_nodes() {
    let presence = MemoryPresenceStore::new();
    register_test_node(&presence, "node-b").await;
    register_test_node(&presence, "node-c").await;
    register_test_node(&presence, "node-d").await;
    for (connection_type, user_id, session_id, node_id) in [
        ("default", "alice", "remote-a", "node-b"),
        ("default", "bob", "remote-b", "node-c"),
        ("admin", "root", "remote-c", "node-d"),
    ] {
        presence
            .register(
                Route {
                    connection_type: ConnectionType::from(connection_type),
                    user_id: UserId::from(user_id),
                    client_id: None,
                    session_id: session_id.into(),
                    node_id: NodeId::from(node_id),
                },
                std::time::Duration::from_secs(60),
            )
            .await
            .unwrap();
    }

    let publisher = RecordingPublisher::default();
    let published = publisher.published.clone();
    let cluster = Cluster::new(presence, publisher);
    let wing = RustWing::with_cluster_unchecked(
        RustWingConfig {
            node_id: NodeId::from("node-a"),
            default_connection_policy: ConnectionPolicy::MultiSession,
            cluster: ClusterConfig {
                enabled: true,
                ..ClusterConfig::default()
            },
            ..RustWingConfig::default()
        },
        Some(cluster),
    );
    let _local_default = wing
        .accept(Identity::new("default", "local"))
        .await
        .unwrap();
    let _local_admin = wing.accept(Identity::new("admin", "root")).await.unwrap();

    let report = wing.broadcast(OutboundFrame::text("notice")).await.unwrap();

    let published = published.lock().unwrap();
    assert_eq!(report.local_sessions, 1);
    assert_eq!(report.remote_nodes, 2);
    assert_eq!(published.len(), 2);
    assert!(
        published
            .iter()
            .all(|entry| entry.0 != NodeId::from("node-d"))
    );
    assert!(published.iter().all(|entry| {
        entry.1.target
            == ClusterTarget::Broadcast {
                connection_type: ConnectionType::from("default"),
            }
    }));
}

// Global broadcast publishes to all remote route-owning nodes 全局广播会发布到所有拥有路由的远端节点
#[tokio::test]
async fn broadcast_all_publishes_to_remote_nodes() {
    let presence = MemoryPresenceStore::new();
    register_test_node(&presence, "node-b").await;
    register_test_node(&presence, "node-c").await;
    for (session_id, node_id) in [("remote-a", "node-b"), ("remote-b", "node-c")] {
        presence
            .register(
                Route {
                    connection_type: ConnectionType::from("default"),
                    user_id: UserId::from(session_id),
                    client_id: None,
                    session_id: session_id.into(),
                    node_id: NodeId::from(node_id),
                },
                std::time::Duration::from_secs(60),
            )
            .await
            .unwrap();
    }

    let publisher = RecordingPublisher::default();
    let published = publisher.published.clone();
    let cluster = Cluster::new(presence, publisher);
    let wing = RustWing::with_cluster_unchecked(
        RustWingConfig {
            node_id: NodeId::from("node-a"),
            cluster: ClusterConfig {
                enabled: true,
                ..ClusterConfig::default()
            },
            ..RustWingConfig::default()
        },
        Some(cluster),
    );
    let _default = wing
        .accept(Identity::new("default", "alice"))
        .await
        .unwrap();
    let _admin = wing.accept(Identity::new("admin", "root")).await.unwrap();

    let report = wing
        .broadcast_all(OutboundFrame::text("notice"))
        .await
        .unwrap();

    let published = published.lock().unwrap();
    assert_eq!(report.local_sessions, 2);
    assert_eq!(report.remote_nodes, 2);
    assert_eq!(published.len(), 2);
    assert!(
        published
            .iter()
            .all(|entry| entry.1.target == ClusterTarget::BroadcastAll)
    );
}

// User sends count local sessions and remote nodes 用户发送会同时统计本地会话和远端节点
#[tokio::test]
async fn send_to_user_counts_local_sessions_and_remote_nodes() {
    let presence = MemoryPresenceStore::new();
    register_test_node(&presence, "node-b").await;
    presence
        .register(
            Route {
                connection_type: ConnectionType::from("default"),
                user_id: UserId::from("alice"),
                client_id: Some("phone".into()),
                session_id: "remote-session".into(),
                node_id: NodeId::from("node-b"),
            },
            std::time::Duration::from_secs(60),
        )
        .await
        .unwrap();

    let publisher = RecordingPublisher::default();
    let published = publisher.published.clone();
    let cluster = Cluster::new(presence, publisher);
    let wing = RustWing::with_cluster_unchecked(
        RustWingConfig {
            node_id: NodeId::from("node-a"),
            default_connection_policy: ConnectionPolicy::MultiSession,
            cluster: ClusterConfig {
                enabled: true,
                ..ClusterConfig::default()
            },
            ..RustWingConfig::default()
        },
        Some(cluster),
    );
    let _local = wing
        .accept(Identity::new("default", "alice"))
        .await
        .unwrap();

    let report = wing
        .send_to_user("alice", OutboundFrame::text("hello"))
        .await
        .unwrap();

    assert_eq!(report.local_sessions, 1);
    assert_eq!(report.remote_nodes, 1);
    assert_eq!(report.delivered(), 2);
    assert_eq!(published.lock().unwrap().len(), 1);
}

// Multi-route presence fans out across remote nodes 多路由在线状态会向多个远端节点扇出
#[tokio::test]
async fn remote_routes_publish_once_per_node() {
    // Seed multiple remote routes, including two sessions on one node 写入多条远端路由，其中同一节点含两个会话
    let presence = MemoryPresenceStore::new();
    register_test_node(&presence, "node-b").await;
    register_test_node(&presence, "node-c").await;
    for (session_id, node_id) in [
        ("remote-session-a", "node-b"),
        ("remote-session-b", "node-b"),
        ("remote-session-c", "node-c"),
    ] {
        presence
            .register(
                Route {
                    connection_type: ConnectionType::from("default"),
                    user_id: UserId::from("alice"),
                    client_id: None,
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
    let wing = RustWing::with_cluster_unchecked(config, Some(cluster));

    // Send one user message through the cluster 向集群发送一条用户消息
    let report = wing
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
    assert_eq!(report.local_sessions, 0);
    assert_eq!(report.remote_nodes, 2);
    assert_eq!(nodes, vec![NodeId::from("node-b"), NodeId::from("node-c")]);
}

// Delivery report counts remote publish targets separately 投递报告会单独统计远程发布目标
#[tokio::test]
async fn send_to_user_counts_remote_nodes() {
    // Seed routes on two remote nodes 写入两个远端节点上的路由
    let presence = MemoryPresenceStore::new();
    register_test_node(&presence, "node-b").await;
    register_test_node(&presence, "node-c").await;
    for (session_id, node_id) in [
        ("remote-session-a", "node-b"),
        ("remote-session-b", "node-c"),
    ] {
        presence
            .register(
                Route {
                    connection_type: ConnectionType::from("default"),
                    user_id: UserId::from("alice"),
                    client_id: None,
                    session_id: session_id.into(),
                    node_id: NodeId::from(node_id),
                },
                std::time::Duration::from_secs(60),
            )
            .await
            .unwrap();
    }

    // Build a clustered manager without a local session 构建没有本地会话的集群管理器
    let publisher = RecordingPublisher::default();
    let published = publisher.published.clone();
    let cluster = Cluster::new(presence, publisher);
    let wing = RustWing::with_cluster_unchecked(
        RustWingConfig {
            node_id: NodeId::from("node-a"),
            cluster: ClusterConfig {
                enabled: true,
                ..ClusterConfig::default()
            },
            ..RustWingConfig::default()
        },
        Some(cluster),
    );

    // Send one frame and inspect remote routing counts 发送一帧并检查远程路由计数
    let report = wing
        .send_to_user("alice", OutboundFrame::text("hello"))
        .await
        .unwrap();

    // Confirm the report counts nodes, not remote sessions 确认报告统计的是节点而不是远端会话
    assert_eq!(report.local_sessions, 0);
    assert_eq!(report.remote_nodes, 2);
    assert_eq!(report.delivered(), 2);
    assert_eq!(published.lock().unwrap().len(), 2);
}

// Failed presence registration rolls back the accepted session 在线状态注册失败会回滚已接收的会话
#[tokio::test]
async fn accept_rolls_back_when_presence_registration_fails() {
    // Build a clustered manager whose presence store rejects registration 构建在线状态注册会失败的集群管理器
    let cluster = Cluster::new(FailingPresenceStore, RecordingPublisher::default());
    let wing = RustWing::with_cluster_unchecked(
        RustWingConfig {
            cluster: ClusterConfig {
                enabled: true,
                ..ClusterConfig::default()
            },
            ..RustWingConfig::default()
        },
        Some(cluster),
    );

    // Accepting the session must surface the registration error 接收会话必须返回注册错误
    let result = wing.accept(Identity::new("default", "alice")).await;

    // Confirm the failed accept did not leave a local session behind 确认失败的接收不会留下本地会话
    assert!(matches!(result, Err(RustWingError::Cluster(message)) if message == "register failed"));
    assert_eq!(wing.connection_count().unwrap(), 0);
    assert!(
        wing.list_user_sessions(&UserId::from("alice"))
            .unwrap()
            .is_empty()
    );
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
    let accepted = wing
        .accept(Identity::new("default", "alice"))
        .await
        .unwrap();

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

// Acknowledgement tracking records local targets and updates stages 确认追踪会记录本地目标并更新阶段
#[tokio::test]
async fn ack_tracking_records_and_updates_local_session() {
    let wing = RustWing::new(RustWingConfig::default());
    let accepted = wing.accept_user("alice").await.unwrap();
    let message_id = wing.next_message_id();

    let report = wing
        .send_to_user(
            "alice",
            OutboundFrame::text("needs ack").require_ack(message_id.clone()),
        )
        .await
        .unwrap();

    assert_eq!(report.local_sessions, 1);
    let snapshot = wing.ack_snapshot(&message_id).unwrap().unwrap();
    assert_eq!(snapshot.sessions.len(), 1);
    assert_eq!(snapshot.sessions[0].stage, None);

    let updated = wing
        .acknowledge(
            accepted.session.id(),
            &message_id,
            AckStage::ClientReceived,
            Some(123),
        )
        .await
        .unwrap();

    assert!(updated);
    let snapshot = wing.ack_snapshot(&message_id).unwrap().unwrap();
    assert!(snapshot.reached(AckStage::ClientReceived));
    assert_eq!(snapshot.sessions[0].client_time, Some(123));
}

// Distributed acknowledgement returns to the origin node 分布式确认会回传到发起节点
#[tokio::test]
async fn distributed_acknowledgement_returns_to_origin_node() {
    let presence = SharedPresenceStore::default();
    let origin_publisher = RecordingPublisher::default();
    let origin_published = origin_publisher.published.clone();
    let remote_publisher = RecordingPublisher::default();
    let remote_published = remote_publisher.published.clone();
    let origin = RustWing::with_cluster_unchecked(
        RustWingConfig {
            node_id: NodeId::from("node-a"),
            cluster: ClusterConfig {
                enabled: true,
                ..ClusterConfig::default()
            },
            ..RustWingConfig::default()
        },
        Some(Cluster::new(presence.clone(), origin_publisher)),
    );
    let remote = RustWing::with_cluster_unchecked(
        RustWingConfig {
            node_id: NodeId::from("node-b"),
            cluster: ClusterConfig {
                enabled: true,
                ..ClusterConfig::default()
            },
            ..RustWingConfig::default()
        },
        Some(Cluster::new(presence, remote_publisher)),
    );
    let accepted = remote.accept_user("alice").await.unwrap();
    let message_id = origin.next_message_id();

    let report = origin
        .send_to_user(
            "alice",
            OutboundFrame::text("needs ack").require_ack(message_id.clone()),
        )
        .await
        .unwrap();
    let outbound_envelope = origin_published.lock().unwrap()[0].1.clone();

    assert_eq!(report.remote_nodes, 1);
    assert_eq!(
        origin
            .ack_snapshot(&message_id)
            .unwrap()
            .unwrap()
            .sessions
            .len(),
        1
    );

    remote.handle_cluster_envelope(outbound_envelope).unwrap();
    let updated = remote
        .acknowledge(
            accepted.session.id(),
            &message_id,
            AckStage::ClientReceived,
            Some(456),
        )
        .await
        .unwrap();
    let ack_envelope = remote_published.lock().unwrap()[0].1.clone();

    assert!(updated);
    assert!(matches!(ack_envelope.target, ClusterTarget::Ack { .. }));

    let delivered = origin.handle_cluster_envelope(ack_envelope).unwrap();
    let snapshot = origin.ack_snapshot(&message_id).unwrap().unwrap();

    assert_eq!(delivered, 1);
    assert!(snapshot.reached(AckStage::ClientReceived));
    assert_eq!(snapshot.sessions[0].client_time, Some(456));
}

// Distributed broadcast acknowledgements return to the origin node 分布式广播确认会回传到发起节点
#[tokio::test]
async fn distributed_broadcast_acknowledgement_returns_to_origin_node() {
    let presence = SharedPresenceStore::default();
    presence
        .register_node(
            &NodeId::from("node-b"),
            "instance-node-b",
            Duration::from_secs(60),
        )
        .await
        .unwrap();
    let origin_publisher = RecordingPublisher::default();
    let origin_published = origin_publisher.published.clone();
    let remote_publisher = RecordingPublisher::default();
    let remote_published = remote_publisher.published.clone();
    let origin = RustWing::with_cluster_unchecked(
        RustWingConfig {
            node_id: NodeId::from("node-a"),
            cluster: ClusterConfig {
                enabled: true,
                ..ClusterConfig::default()
            },
            ..RustWingConfig::default()
        },
        Some(Cluster::new(presence.clone(), origin_publisher)),
    );
    let remote = RustWing::with_cluster_unchecked(
        RustWingConfig {
            node_id: NodeId::from("node-b"),
            cluster: ClusterConfig {
                enabled: true,
                ..ClusterConfig::default()
            },
            ..RustWingConfig::default()
        },
        Some(Cluster::new(presence, remote_publisher)),
    );
    let accepted = remote.accept_user("alice").await.unwrap();
    let message_id = origin.next_message_id();

    let report = origin
        .broadcast(OutboundFrame::text("needs ack").require_ack(message_id.clone()))
        .await
        .unwrap();
    let outbound_envelope = origin_published.lock().unwrap()[0].1.clone();

    assert_eq!(report.remote_nodes, 1);
    assert_eq!(
        origin
            .ack_snapshot(&message_id)
            .unwrap()
            .unwrap()
            .sessions
            .len(),
        1
    );

    remote.handle_cluster_envelope(outbound_envelope).unwrap();
    let updated = remote
        .acknowledge(
            accepted.session.id(),
            &message_id,
            AckStage::ClientReceived,
            Some(789),
        )
        .await
        .unwrap();
    let ack_envelope = remote_published.lock().unwrap()[0].1.clone();

    assert!(updated);
    assert!(matches!(ack_envelope.target, ClusterTarget::Ack { .. }));

    let delivered = origin.handle_cluster_envelope(ack_envelope).unwrap();
    let snapshot = origin.ack_snapshot(&message_id).unwrap().unwrap();

    assert_eq!(delivered, 1);
    assert!(snapshot.reached(AckStage::ClientReceived));
    assert_eq!(snapshot.sessions[0].client_time, Some(789));
}

// Acknowledgement wait returns once the target stage is reached 确认等待会在目标阶段达到后返回
#[tokio::test]
async fn wait_for_ack_returns_when_stage_is_reached() {
    let wing = RustWing::new(RustWingConfig::default());
    let accepted = wing.accept_user("alice").await.unwrap();
    let message_id = wing.next_message_id();
    wing.send_to_user(
        "alice",
        OutboundFrame::text("needs ack").require_ack(message_id.clone()),
    )
    .await
    .unwrap();

    let ack_wing = wing.clone();
    let ack_session_id = accepted.session.id().clone();
    let ack_message_id = message_id.clone();
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        ack_wing
            .acknowledge(
                &ack_session_id,
                &ack_message_id,
                AckStage::ClientReceived,
                None,
            )
            .await
            .unwrap();
    });

    let snapshot = wing
        .wait_for_ack(
            &message_id,
            AckStage::ClientReceived,
            std::time::Duration::from_secs(1),
        )
        .await
        .unwrap()
        .unwrap();

    assert!(snapshot.reached(AckStage::ClientReceived));
}

// Expired acknowledgement entries can be reaped 过期确认条目可以被清理
#[tokio::test]
async fn expired_acks_are_reaped() {
    let wing = RustWing::new(
        RustWingConfig::default()
            .with_ack_ttl(std::time::Duration::from_millis(10))
            .with_maintenance_enabled(false),
    );
    let _accepted = wing.accept_user("alice").await.unwrap();
    let message_id = wing.next_message_id();
    wing.send_to_user(
        "alice",
        OutboundFrame::text("needs ack").require_ack(message_id.clone()),
    )
    .await
    .unwrap();

    assert_eq!(wing.ack_pending_count(), 1);
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;

    assert_eq!(wing.reap_expired_acks(), 1);
    assert_eq!(wing.ack_pending_count(), 0);
    assert!(wing.ack_snapshot(&message_id).unwrap().is_none());
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
    let accepted = wing
        .accept(Identity::new("default", "alice"))
        .await
        .unwrap();

    // Wait long enough for the session to become inactive 等待足够时间让会话变为不活跃
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    let removed = wing.reap_inactive_sessions().await.unwrap();

    // Confirm the stale session was removed and closed 确认过期会话已被移除并关闭
    assert_eq!(removed, 1);
    assert!(accepted.session.is_closed());
    assert_eq!(wing.connection_count().unwrap(), 0);
}

// Managed maintenance reaps inactive sessions automatically 托管维护会自动回收失活会话
#[tokio::test]
async fn managed_maintenance_reaps_inactive_sessions() {
    let config = RustWingConfig::default()
        .with_heartbeat_interval(Duration::from_millis(10))
        .with_heartbeat_timeout(Duration::from_millis(20))
        .with_maintenance_probe_timeout(Duration::from_millis(10))
        .with_maintenance_interval(Duration::from_millis(5));
    let wing = RustWing::from_config(config).await.unwrap();
    let accepted = wing.accept_user("alice").await.unwrap();

    tokio::time::sleep(Duration::from_millis(80)).await;

    assert!(accepted.session.is_closed());
    assert_eq!(wing.connection_count().unwrap(), 0);
}

// New managers start managed maintenance when enabled new 创建的管理器会在启用时启动托管维护
#[tokio::test]
async fn new_starts_managed_maintenance_when_enabled() {
    let config = RustWingConfig::default()
        .with_heartbeat_interval(Duration::from_millis(10))
        .with_heartbeat_timeout(Duration::from_millis(20))
        .with_maintenance_probe_timeout(Duration::from_millis(10))
        .with_maintenance_interval(Duration::from_millis(5));
    let wing = RustWing::new(config);
    let accepted = wing.accept_user("alice").await.unwrap();

    tokio::time::sleep(Duration::from_millis(80)).await;

    assert!(accepted.session.is_closed());
    assert_eq!(wing.connection_count().unwrap(), 0);
}

// Managed maintenance sends a probe before removing an inactive session 托管维护会先发送探测再移除失活会话
#[tokio::test]
async fn managed_maintenance_probes_before_reaping_session() {
    let config = RustWingConfig::default()
        .with_heartbeat_interval(Duration::from_millis(10))
        .with_heartbeat_timeout(Duration::from_millis(20))
        .with_maintenance_probe_timeout(Duration::from_millis(30))
        .with_maintenance_interval(Duration::from_millis(5));
    let wing = RustWing::from_config(config).await.unwrap();
    let mut accepted = wing.accept_user("alice").await.unwrap();

    let frame = tokio::time::timeout(Duration::from_millis(80), accepted.outbound.recv())
        .await
        .unwrap()
        .unwrap();

    assert_eq!(frame.kind, FrameKind::Ping);
    assert_eq!(wing.connection_count().unwrap(), 1);
}

// Managed maintenance updates runtime stats 托管维护会更新运行统计
#[tokio::test]
async fn managed_maintenance_updates_runtime_stats() {
    let config = RustWingConfig::default()
        .with_heartbeat_interval(Duration::from_millis(10))
        .with_heartbeat_timeout(Duration::from_millis(20))
        .with_maintenance_probe_timeout(Duration::from_millis(20))
        .with_maintenance_interval(Duration::from_millis(100));
    let wing = RustWing::from_config(config).await.unwrap();
    let mut accepted = wing.accept_user("alice").await.unwrap();

    let frame = tokio::time::timeout(Duration::from_millis(160), accepted.outbound.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(frame.kind, FrameKind::Ping);
    let probed = wing.stats_snapshot().unwrap();
    assert_eq!(probed.maintenance_probes_sent_total, 1);
    assert_eq!(probed.outbound_frames_enqueued_total, 1);

    let count = wait_for_connection_count_at_most(&wing, 0, Duration::from_millis(400)).await;
    assert_eq!(count, 0);
    let reaped = wing.stats_snapshot().unwrap();
    assert_eq!(reaped.maintenance_sessions_reaped_total, 1);
}

// Activity after a probe prevents stale cleanup 探测后的活跃会阻止过期清理
#[tokio::test]
async fn activity_after_probe_keeps_session_alive() {
    let config = RustWingConfig::default()
        .with_heartbeat_interval(Duration::from_millis(10))
        .with_heartbeat_timeout(Duration::from_millis(20))
        .with_maintenance_probe_timeout(Duration::from_millis(20))
        .with_maintenance_interval(Duration::from_millis(5));
    let wing = RustWing::from_config(config).await.unwrap();
    let mut accepted = wing.accept_user("alice").await.unwrap();

    let frame = tokio::time::timeout(Duration::from_millis(80), accepted.outbound.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(frame.kind, FrameKind::Ping);

    wing.touch(&accepted.session).await.unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;

    assert!(!accepted.session.is_closed());
    assert_eq!(wing.connection_count().unwrap(), 1);
}

// Managed maintenance limits liveness probes per tick 托管维护会限制单轮存活探测数量
#[tokio::test]
async fn managed_maintenance_limits_probes_per_tick() {
    let config = RustWingConfig::default()
        .with_default_connection_policy(ConnectionPolicy::MultiSession)
        .with_heartbeat_interval(Duration::from_millis(10))
        .with_heartbeat_timeout(Duration::from_millis(20))
        .with_maintenance_probe_timeout(Duration::from_secs(1))
        .with_maintenance_interval(Duration::from_millis(100))
        .with_maintenance_max_probe_per_tick(1);
    let wing = RustWing::from_config(config).await.unwrap();
    let mut accepted = Vec::new();
    for _ in 0..3 {
        accepted.push(wing.accept_user("alice").await.unwrap());
    }

    tokio::time::sleep(Duration::from_millis(140)).await;

    let mut probed = 0;
    for session in &mut accepted {
        if let Ok(Some(frame)) =
            tokio::time::timeout(Duration::from_millis(5), session.outbound.recv()).await
        {
            assert_eq!(frame.kind, FrameKind::Ping);
            probed += 1;
        }
    }
    assert_eq!(probed, 1);
    assert_eq!(wing.connection_count().unwrap(), 3);
}

// Managed maintenance limits removals per tick 托管维护会限制单轮清理数量
#[tokio::test]
async fn managed_maintenance_limits_cleanup_per_tick() {
    let config = RustWingConfig::default()
        .with_default_connection_policy(ConnectionPolicy::MultiSession)
        .with_heartbeat_interval(Duration::from_millis(10))
        .with_heartbeat_timeout(Duration::from_millis(20))
        .with_maintenance_probe_timeout(Duration::from_millis(20))
        .with_maintenance_interval(Duration::from_millis(150))
        .with_maintenance_max_cleanup_per_tick(1)
        .with_maintenance_max_probe_per_tick(10);
    let wing = RustWing::from_config(config).await.unwrap();
    for _ in 0..3 {
        let _accepted = wing.accept_user("alice").await.unwrap();
    }

    let count = wait_for_connection_count_at_most(&wing, 2, Duration::from_millis(700)).await;
    assert_eq!(count, 2);

    tokio::time::sleep(Duration::from_millis(70)).await;
    assert_eq!(wing.connection_count().unwrap(), 2);

    let count = wait_for_connection_count_at_most(&wing, 1, Duration::from_millis(500)).await;
    assert_eq!(count, 1);
}

// Managed maintenance reaps expired acknowledgement entries automatically 托管维护会自动回收过期确认条目
#[tokio::test]
async fn managed_maintenance_reaps_expired_acks() {
    let config = RustWingConfig::default()
        .with_ack_ttl(Duration::from_millis(10))
        .with_maintenance_interval(Duration::from_millis(5));
    let wing = RustWing::from_config(config).await.unwrap();
    let _accepted = wing.accept_user("alice").await.unwrap();
    let message_id = wing.next_message_id();

    wing.send_to_user(
        "alice",
        OutboundFrame::text("needs ack").require_ack(message_id),
    )
    .await
    .unwrap();

    assert_eq!(wing.ack_pending_count(), 1);
    tokio::time::sleep(Duration::from_millis(60)).await;

    assert_eq!(wing.ack_pending_count(), 0);
}

// Wait until the local connection count reaches an upper bound 等待本地连接数达到指定上限
async fn wait_for_connection_count_at_most(
    wing: &RustWing,
    target: usize,
    timeout: Duration,
) -> usize {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let count = wing.connection_count().unwrap();
        if count <= target || tokio::time::Instant::now() >= deadline {
            return count;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

// Presence store that fails new route registration 新路由注册会失败的在线状态存储
struct FailingPresenceStore;

#[async_trait]
impl PresenceStore for FailingPresenceStore {
    // Reject route registration for rollback tests 为回滚测试拒绝路由注册
    async fn register(&self, _route: Route, _ttl: Duration) -> Result<()> {
        Err(RustWingError::Cluster("register failed".into()))
    }

    // Remove succeeds because no route is stored 没有存储路由所以删除成功
    async fn remove(
        &self,
        _connection_type: &ConnectionType,
        _user_id: &UserId,
        _session_id: &SessionId,
    ) -> Result<()> {
        Ok(())
    }

    // Touch succeeds because no route is stored 没有存储路由所以刷新成功
    async fn touch(
        &self,
        _connection_type: &ConnectionType,
        _user_id: &UserId,
        _session_id: &SessionId,
        _ttl: Duration,
    ) -> Result<()> {
        Ok(())
    }

    // Locate returns no routes because registration always fails 注册总是失败所以查询不到路由
    async fn locate(
        &self,
        _connection_type: &ConnectionType,
        _user_id: &UserId,
    ) -> Result<Vec<Route>> {
        Ok(Vec::new())
    }

    // Session lookup returns no routes because registration always fails 注册总是失败所以查不到会话路由
    async fn locate_session(&self, _session_id: &SessionId) -> Result<Option<Route>> {
        Ok(None)
    }

    // Route listing returns no routes because registration always fails 注册总是失败所以列不出路由
    async fn list_routes(&self, _connection_type: &ConnectionType) -> Result<Vec<Route>> {
        Ok(Vec::new())
    }

    // Global route listing returns no routes because registration always fails 注册总是失败所以列不出全局路由
    async fn list_all_routes(&self) -> Result<Vec<Route>> {
        Ok(Vec::new())
    }

    // Node listing returns no nodes because no routes are stored 没有存储路由所以没有节点
    async fn list_nodes(&self) -> Result<Vec<NodeId>> {
        Ok(Vec::new())
    }

    // Node lease registration is unused by this failing store 此失败存储不使用节点租约注册
    async fn register_node(
        &self,
        _node_id: &NodeId,
        _instance_id: &str,
        _ttl: Duration,
    ) -> Result<NodeLease> {
        Ok(NodeLease::Acquired)
    }

    // Node lease removal is unused by this failing store 此失败存储不使用节点租约删除
    async fn unregister_node(&self, _node_id: &NodeId, _instance_id: &str) -> Result<()> {
        Ok(())
    }
}

// Shared presence store used by duplicate node tests 重复节点测试使用的共享在线状态存储
#[derive(Clone, Default)]
struct SharedPresenceStore {
    // Shared in-memory presence implementation 共享内存在线状态实现
    inner: Arc<MemoryPresenceStore>,
}

#[async_trait]
impl PresenceStore for SharedPresenceStore {
    // Register a route through the shared store 通过共享存储注册路由
    async fn register(&self, route: Route, ttl: Duration) -> Result<()> {
        self.inner.register(route, ttl).await
    }

    // Remove a route through the shared store 通过共享存储删除路由
    async fn remove(
        &self,
        connection_type: &ConnectionType,
        user_id: &UserId,
        session_id: &SessionId,
    ) -> Result<()> {
        self.inner
            .remove(connection_type, user_id, session_id)
            .await
    }

    // Refresh a route through the shared store 通过共享存储刷新路由
    async fn touch(
        &self,
        connection_type: &ConnectionType,
        user_id: &UserId,
        session_id: &SessionId,
        ttl: Duration,
    ) -> Result<()> {
        self.inner
            .touch(connection_type, user_id, session_id, ttl)
            .await
    }

    // Locate routes through the shared store 通过共享存储查询路由
    async fn locate(
        &self,
        connection_type: &ConnectionType,
        user_id: &UserId,
    ) -> Result<Vec<Route>> {
        self.inner.locate(connection_type, user_id).await
    }

    // Locate a session route through the shared store 通过共享存储查询会话路由
    async fn locate_session(&self, session_id: &SessionId) -> Result<Option<Route>> {
        self.inner.locate_session(session_id).await
    }

    // List routes through the shared store 通过共享存储列出路由
    async fn list_routes(&self, connection_type: &ConnectionType) -> Result<Vec<Route>> {
        self.inner.list_routes(connection_type).await
    }

    // List all routes through the shared store 通过共享存储列出全部路由
    async fn list_all_routes(&self) -> Result<Vec<Route>> {
        self.inner.list_all_routes().await
    }

    // List nodes through the shared store 通过共享存储列出节点
    async fn list_nodes(&self) -> Result<Vec<NodeId>> {
        self.inner.list_nodes().await
    }

    // Register a node lease through the shared store 通过共享存储注册节点租约
    async fn register_node(
        &self,
        node_id: &NodeId,
        instance_id: &str,
        ttl: Duration,
    ) -> Result<NodeLease> {
        self.inner.register_node(node_id, instance_id, ttl).await
    }

    // Remove a node lease through the shared store 通过共享存储删除节点租约
    async fn unregister_node(&self, node_id: &NodeId, instance_id: &str) -> Result<()> {
        self.inner.unregister_node(node_id, instance_id).await
    }
}

// Register a live remote node for routing tests 为路由测试注册一个活跃远端节点
async fn register_test_node(presence: &MemoryPresenceStore, node_id: &str) {
    presence
        .register_node(
            &NodeId::from(node_id),
            &format!("instance-{node_id}"),
            Duration::from_secs(60),
        )
        .await
        .unwrap();
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

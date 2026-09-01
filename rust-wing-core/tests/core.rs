use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use rust_wing_core::{
    AcceptedSession, ClientId, Cluster, ClusterConfig, ClusterEnvelope, ClusterTarget,
    ConnectionPolicy, ConnectionType, DisconnectCause, FrameKind, Identity, MemoryPresenceStore,
    NodeId, NodeLease, NodePublisher, OutboundFrame, PresenceStore, Result, Route, RouteClaim,
    RouteRefresh, RuntimeStatus, RustWing, RustWingConfig, RustWingError, SessionEvent, SessionId,
    UserId,
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

// Checked construction requires dependencies for an enabled cluster 校验式构造要求启用的集群具备依赖
#[tokio::test]
async fn checked_cluster_rejects_missing_dependencies() {
    let config = RustWingConfig {
        cluster: ClusterConfig {
            enabled: true,
            ..ClusterConfig::default()
        },
        ..RustWingConfig::default()
    };

    let result = RustWing::with_cluster_checked(config, None).await;

    assert!(matches!(
        result,
        Err(RustWingError::InvalidConfig(message))
            if message.contains("cluster.enabled requires cluster dependencies")
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

// Generated node ids receive the same duplicate lease protection 自动生成的节点标识同样受重复租约保护
#[tokio::test]
async fn checked_cluster_rejects_duplicate_generated_node_id() {
    let presence = SharedPresenceStore::default();
    let first_cluster = Cluster::new(presence.clone(), RecordingPublisher::default());
    let second_cluster = Cluster::new(presence, RecordingPublisher::default());
    let mut config = RustWingConfig::default();
    config.cluster.enabled = true;
    let node_id = config.node_id.clone();

    let _first = RustWing::with_cluster_checked(config.clone(), Some(first_cluster))
        .await
        .unwrap();
    let second = RustWing::with_cluster_checked(config, Some(second_cluster)).await;

    assert!(matches!(
        second,
        Err(RustWingError::InvalidConfig(message))
            if message.contains(&format!("node_id '{}' is already active", node_id.as_str()))
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

// Shutdown is shared by clones and prevents accepting new sessions 关闭状态由克隆句柄共享并阻止接收新会话
#[tokio::test]
async fn shutdown_invalidates_cloned_manager_handles() {
    let wing = RustWing::new(RustWingConfig::default());
    let cloned = wing.clone();
    let accepted = wing.accept_user("alice").await.unwrap();

    assert_eq!(wing.runtime_status(), RuntimeStatus::Running);
    assert!(wing.is_ready());
    assert_eq!(wing.shutdown().await.unwrap(), 1);
    assert_eq!(cloned.shutdown().await.unwrap(), 0);

    assert!(accepted.session.is_closed());
    assert_eq!(cloned.runtime_status(), RuntimeStatus::Stopped);
    assert!(!cloned.is_ready());
    assert!(!cloned.health().maintenance_running);
    assert!(matches!(
        cloned.accept_user("bob").await,
        Err(RustWingError::RuntimeNotReady(status)) if status == "Stopped"
    ));
}

// Lifecycle subscribers receive one connected and one typed disconnected event 生命周期订阅者会收到连接和类型化断开事件
#[tokio::test]
async fn lifecycle_events_report_connected_and_disconnected_once() {
    let wing = RustWing::new(RustWingConfig::default().with_session_event_capacity(8));
    let mut events = wing.subscribe_session_events();
    let accepted = wing.accept_user("alice").await.unwrap();

    let connected = events.recv().await.unwrap();
    assert!(matches!(
        connected,
        SessionEvent::Connected { session }
            if session.id.as_str() == accepted.session.id().as_str() && !session.closed
    ));

    assert!(
        wing.unregister_with_cause(
            &accepted.session,
            DisconnectCause::ServerRequested {
                reason: "logout".into(),
            },
        )
        .await
        .unwrap()
    );
    assert!(
        !wing
            .unregister_with_cause(&accepted.session, DisconnectCause::RuntimeShutdown)
            .await
            .unwrap()
    );

    let disconnected = events.recv().await.unwrap();
    assert!(matches!(
        disconnected,
        SessionEvent::Disconnected { session, cause }
            if session.closed
                && session.id.as_str() == accepted.session.id().as_str()
                && cause
                    == (DisconnectCause::ServerRequested {
                        reason: "logout".into(),
                    })
    ));
    assert!(matches!(
        events.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));
}

// Session replacement emits a replacement cause before the new connection event 会话替换会先发出替换原因再发出新连接事件
#[tokio::test]
async fn lifecycle_events_report_replacement() {
    let wing = RustWing::new(RustWingConfig::default().with_session_event_capacity(8));
    let mut events = wing.subscribe_session_events();
    let first = wing.accept_user("alice").await.unwrap();
    let _ = events.recv().await.unwrap();

    let _second = wing.accept_user("alice").await.unwrap();
    let replaced = events.recv().await.unwrap();
    assert!(matches!(
        replaced,
        SessionEvent::Disconnected { session, cause }
            if session.id.as_str() == first.session.id().as_str()
                && cause == DisconnectCause::Replaced
    ));
    assert!(matches!(
        events.recv().await.unwrap(),
        SessionEvent::Connected { .. }
    ));
}

// Slow lifecycle subscribers observe bounded-channel lag instead of blocking acceptance 慢生命周期订阅者会观察到有界通道丢失而不会阻塞接收
#[tokio::test]
async fn lifecycle_event_channel_reports_lag() {
    let wing = RustWing::new(RustWingConfig::default().with_session_event_capacity(1));
    let mut events = wing.subscribe_session_events();
    let _first = wing.accept_user("alice").await.unwrap();
    let _second = wing.accept_user("bob").await.unwrap();

    assert!(matches!(
        events.recv().await,
        Err(tokio::sync::broadcast::error::RecvError::Lagged(_))
    ));
}

// A full outbound queue terminates the session and removes its distributed route 出站队列满会终止会话并移除分布式路由
#[tokio::test]
async fn outbound_queue_full_cleans_session_and_presence() {
    let presence = SharedPresenceStore::default();
    let wing = RustWing::with_cluster_checked(
        RustWingConfig {
            node_id: NodeId::from("node-a"),
            write_queue_capacity: 1,
            cluster: ClusterConfig {
                enabled: true,
                ..ClusterConfig::default()
            },
            ..RustWingConfig::default()
        },
        Some(Cluster::new(
            presence.clone(),
            RecordingPublisher::default(),
        )),
    )
    .await
    .unwrap();
    let mut events = wing.subscribe_session_events();
    let accepted = wing.accept_user("alice").await.unwrap();
    let session_id = accepted.session.id().clone();
    let close_signal = accepted.session.subscribe_close();
    let _ = events.recv().await.unwrap();

    let first = wing
        .send_to_session(&session_id, OutboundFrame::text("first"))
        .await
        .unwrap();
    let rejected = wing
        .send_to_session(&session_id, OutboundFrame::text("second"))
        .await
        .unwrap();

    assert_eq!(first.local_sessions, 1);
    assert_eq!(rejected.local_sessions, 0);
    assert!(accepted.session.is_closed());
    assert_eq!(wing.connection_count().unwrap(), 0);
    assert!(
        presence
            .locate_session(&session_id)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        close_signal.borrow().as_deref(),
        Some(b"outbound queue full".as_slice())
    );
    assert!(matches!(
        events.recv().await.unwrap(),
        SessionEvent::Disconnected { cause, .. }
            if cause == DisconnectCause::OutboundQueueFull
    ));
}

// Synchronous local close schedules distributed Presence cleanup 同步本地关闭会安排分布式 Presence 清理
#[tokio::test]
async fn synchronous_local_close_cleans_presence() {
    let presence = SharedPresenceStore::default();
    let wing = RustWing::with_cluster_checked(
        RustWingConfig {
            node_id: NodeId::from("node-a"),
            cluster: ClusterConfig {
                enabled: true,
                ..ClusterConfig::default()
            },
            ..RustWingConfig::default()
        },
        Some(Cluster::new(
            presence.clone(),
            RecordingPublisher::default(),
        )),
    )
    .await
    .unwrap();
    let accepted = wing.accept_user("alice").await.unwrap();
    let session_id = accepted.session.id().clone();

    let removed = wing
        .send_local(
            &ConnectionType::default(),
            &UserId::from("alice"),
            OutboundFrame::close("local close"),
        )
        .unwrap();

    assert_eq!(removed, 1);
    assert!(accepted.session.is_closed());
    assert_eq!(wing.connection_count().unwrap(), 0);
    tokio::time::timeout(Duration::from_millis(200), async {
        loop {
            if presence
                .locate_session(&session_id)
                .await
                .unwrap()
                .is_none()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

// A dropped outbound receiver terminates the session exactly once 出站接收端被丢弃时会且只会终止一次会话
#[tokio::test]
async fn closed_outbound_receiver_terminates_session_once() {
    let wing = RustWing::new(RustWingConfig::default());
    let mut events = wing.subscribe_session_events();
    let accepted = wing.accept_user("alice").await.unwrap();
    let session = accepted.session.clone();
    let session_id = session.id().clone();
    let _ = events.recv().await.unwrap();
    drop(accepted.outbound);

    let report = wing
        .send_to_session(&session_id, OutboundFrame::text("message"))
        .await
        .unwrap();

    assert_eq!(report.local_sessions, 0);
    assert!(session.is_closed());
    assert_eq!(wing.connection_count().unwrap(), 0);
    assert!(matches!(
        events.recv().await.unwrap(),
        SessionEvent::Disconnected { cause, .. }
            if cause == DisconnectCause::OutboundReceiverClosed
    ));
    assert!(matches!(
        events.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));
}

// Lease refresh failures degrade the runtime and successful refresh restores it 租约刷新失败会降级运行时且成功刷新会恢复
#[tokio::test]
async fn node_lease_health_tracks_refresh_failures_and_recovery() {
    let presence = SharedPresenceStore::default();
    let cluster = Cluster::new(presence.clone(), RecordingPublisher::default());
    let config = RustWingConfig {
        node_id: NodeId::from("node-a"),
        cluster: ClusterConfig {
            enabled: true,
            node_lease_ttl: Duration::from_millis(300),
            ..ClusterConfig::default()
        },
        ..RustWingConfig::default()
    };
    let wing = RustWing::with_cluster_checked(config, Some(cluster))
        .await
        .unwrap();

    presence
        .fail_node_registration
        .store(true, Ordering::Release);
    assert_eq!(
        wait_for_runtime_status(&wing, RuntimeStatus::Degraded, Duration::from_millis(500)).await,
        RuntimeStatus::Degraded
    );
    let degraded = wing.health();
    assert!(!degraded.node_lease_healthy);
    assert!(degraded.last_error.is_some());

    presence
        .fail_node_registration
        .store(false, Ordering::Release);
    assert_eq!(
        wait_for_runtime_status(&wing, RuntimeStatus::Running, Duration::from_millis(500)).await,
        RuntimeStatus::Running
    );
    let recovered = wing.health();
    assert!(recovered.node_lease_healthy);
    assert!(recovered.last_error.is_none());

    wing.shutdown().await.unwrap();
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
    let wing = RustWing::with_cluster_checked(
        RustWingConfig {
            node_id: NodeId::from("node-a"),
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
    let wing = RustWing::with_cluster_checked(
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
    )
    .await
    .unwrap();

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
    let mut events = wing.subscribe_session_events();
    let mut accepted = wing.accept_user("alice").await.unwrap();
    let _ = events.recv().await.unwrap();
    let envelope = ClusterEnvelope::new_for_session(
        accepted.session.id().clone(),
        OutboundFrame::close("replaced by a newer connection"),
    )
    .with_disconnect_cause(DisconnectCause::Replaced);

    let delivered = wing.handle_cluster_envelope_async(envelope).await.unwrap();
    let close_frame = accepted.outbound.recv().await.unwrap();

    assert_eq!(delivered, 1);
    assert!(accepted.session.is_closed());
    assert_eq!(close_frame.kind, FrameKind::Close);
    assert_eq!(wing.connection_count().unwrap(), 0);
    assert!(matches!(
        events.recv().await.unwrap(),
        SessionEvent::Disconnected { cause, .. } if cause == DisconnectCause::Replaced
    ));
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

// Atomic claims enforce every connection policy and prevent stale refreshes 原子仲裁会执行全部连接策略并阻止旧路由续期
#[tokio::test]
async fn memory_presence_claim_enforces_policies_and_rejects_stale_touch() {
    let presence = MemoryPresenceStore::new();
    for node_id in ["node-a", "node-b", "node-c", "node-d"] {
        register_test_node(&presence, node_id).await;
    }
    let route = |session_id: &str, client_id: Option<&str>, node_id: &str| Route {
        connection_type: ConnectionType::from("default"),
        user_id: UserId::from("alice"),
        client_id: client_id.map(ClientId::from),
        session_id: SessionId::from(session_id),
        node_id: NodeId::from(node_id),
    };
    let first = route("session-a", Some("phone"), "node-a");
    let second = route("session-b", Some("browser"), "node-b");
    let replacement = route("session-c", Some("phone"), "node-c");

    presence
        .claim(
            first.clone(),
            ConnectionPolicy::UniqueClient,
            Duration::from_secs(60),
        )
        .await
        .unwrap();
    presence
        .claim(
            second.clone(),
            ConnectionPolicy::UniqueClient,
            Duration::from_secs(60),
        )
        .await
        .unwrap();
    let claim = presence
        .claim(
            replacement.clone(),
            ConnectionPolicy::UniqueClient,
            Duration::from_secs(60),
        )
        .await
        .unwrap();
    assert_eq!(claim.displaced, vec![first.clone()]);
    assert_eq!(
        presence
            .touch(
                &first.connection_type,
                &first.user_id,
                &first.session_id,
                Duration::from_secs(60),
            )
            .await
            .unwrap(),
        RouteRefresh::Lost
    );
    assert_eq!(
        presence
            .locate(&first.connection_type, &first.user_id)
            .await
            .unwrap()
            .len(),
        2
    );

    let additional = route("session-d", None, "node-d");
    let claim = presence
        .claim(
            additional.clone(),
            ConnectionPolicy::MultiSession,
            Duration::from_secs(60),
        )
        .await
        .unwrap();
    assert!(claim.displaced.is_empty());

    let exclusive = route("session-e", None, "node-a");
    let claim = presence
        .claim(
            exclusive.clone(),
            ConnectionPolicy::UniqueUser,
            Duration::from_secs(60),
        )
        .await
        .unwrap();
    assert_eq!(claim.displaced.len(), 3);
    assert_eq!(
        presence
            .locate(&exclusive.connection_type, &exclusive.user_id)
            .await
            .unwrap(),
        vec![exclusive.clone()]
    );
    assert_eq!(
        presence
            .touch(
                &exclusive.connection_type,
                &exclusive.user_id,
                &exclusive.session_id,
                Duration::from_secs(60),
            )
            .await
            .unwrap(),
        RouteRefresh::Refreshed
    );
}

// Concurrent unique-user claims leave exactly one route owner 并发单用户仲裁最终只保留一个路由所有者
#[tokio::test]
async fn concurrent_memory_presence_unique_user_claims_leave_one_owner() {
    let presence = Arc::new(MemoryPresenceStore::new());
    let barrier = Arc::new(tokio::sync::Barrier::new(16));
    let mut routes = Vec::new();
    let mut tasks = Vec::new();
    for index in 0..16 {
        let node_id = format!("node-{index}");
        register_test_node(&presence, &node_id).await;
        let route = Route {
            connection_type: ConnectionType::from("default"),
            user_id: UserId::from("alice"),
            client_id: Some(ClientId::from(format!("client-{index}"))),
            session_id: SessionId::from(format!("session-{index}")),
            node_id: NodeId::from(node_id),
        };
        routes.push(route.clone());
        let presence = presence.clone();
        let barrier = barrier.clone();
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            presence
                .claim(route, ConnectionPolicy::UniqueUser, Duration::from_secs(60))
                .await
                .unwrap();
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }

    let remaining = presence
        .locate(&ConnectionType::from("default"), &UserId::from("alice"))
        .await
        .unwrap();
    assert_eq!(remaining.len(), 1);
    let mut refreshed = 0;
    for route in routes {
        refreshed += usize::from(
            presence
                .touch(
                    &route.connection_type,
                    &route.user_id,
                    &route.session_id,
                    Duration::from_secs(60),
                )
                .await
                .unwrap()
                == RouteRefresh::Refreshed,
        );
    }
    assert_eq!(refreshed, 1);
}

// A displaced manager session closes when its next refresh observes lost ownership 被替换节点的会话会在续期发现所有权丢失时关闭
#[tokio::test]
async fn distributed_unique_user_loser_closes_on_presence_refresh() {
    let presence = SharedPresenceStore::default();
    let first_wing = RustWing::with_cluster_checked(
        RustWingConfig {
            node_id: NodeId::from("node-a"),
            default_connection_policy: ConnectionPolicy::UniqueUser,
            cluster: ClusterConfig {
                enabled: true,
                ..ClusterConfig::default()
            },
            ..RustWingConfig::default()
        },
        Some(Cluster::new(
            presence.clone(),
            RecordingPublisher::default(),
        )),
    )
    .await
    .unwrap();
    let second_wing = RustWing::with_cluster_checked(
        RustWingConfig {
            node_id: NodeId::from("node-b"),
            default_connection_policy: ConnectionPolicy::UniqueUser,
            cluster: ClusterConfig {
                enabled: true,
                ..ClusterConfig::default()
            },
            ..RustWingConfig::default()
        },
        Some(Cluster::new(
            presence.clone(),
            RecordingPublisher::default(),
        )),
    )
    .await
    .unwrap();

    let (first, second) = tokio::join!(
        first_wing.accept(Identity::new("default", "alice")),
        second_wing.accept(Identity::new("default", "alice")),
    );
    let first = first.unwrap();
    let second = second.unwrap();
    let routes = presence
        .locate(&ConnectionType::from("default"), &UserId::from("alice"))
        .await
        .unwrap();
    assert_eq!(routes.len(), 1);

    let (winner, winner_wing, loser, loser_wing) = if routes[0].session_id == *first.session.id() {
        (&first.session, &first_wing, &second.session, &second_wing)
    } else {
        (&second.session, &second_wing, &first.session, &first_wing)
    };
    winner_wing.touch(winner).await.unwrap();
    loser_wing.touch(loser).await.unwrap();

    assert!(!winner.is_closed());
    assert!(loser.is_closed());
    assert!(loser_wing.get_session(loser.id()).unwrap().is_none());
    assert_eq!(
        presence
            .locate(&ConnectionType::from("default"), &UserId::from("alice"))
            .await
            .unwrap()
            .len(),
        1
    );
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
    let wing = RustWing::with_cluster_checked(
        RustWingConfig {
            node_id: NodeId::from("node-a"),
            cluster: ClusterConfig {
                enabled: true,
                ..ClusterConfig::default()
            },
            ..RustWingConfig::default()
        },
        Some(Cluster::new(presence.clone(), publisher)),
    )
    .await
    .unwrap();

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
    assert_eq!(
        published[0].1.disconnect_cause,
        Some(DisconnectCause::ServerRequested {
            reason: "kicked".into(),
        })
    );
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

// Concurrent unique-user accepts retain exactly one session 同一用户并发唯一用户接入最终仅保留一条会话
#[tokio::test]
async fn concurrent_unique_user_accepts_retain_one_session() {
    let wing = RustWing::new(
        RustWingConfig::default().with_default_connection_policy(ConnectionPolicy::UniqueUser),
    );
    let identities = (0..32)
        .map(|_| Identity::default_connection("alice"))
        .collect();

    let accepted = accept_identities_concurrently(wing.clone(), identities).await;
    let sessions = wing.list_user_sessions(&UserId::from("alice")).unwrap();

    assert_eq!(wing.connection_count().unwrap(), 1);
    assert_eq!(sessions.len(), 1);
    assert_eq!(
        accepted
            .iter()
            .filter(|accepted| !accepted.session.is_closed())
            .count(),
        1
    );
}

// Concurrent unique-client accepts retain one session per client 同一用户并发唯一客户端接入最终每个客户端保留一条会话
#[tokio::test]
async fn concurrent_unique_client_accepts_retain_one_session_per_client() {
    let wing = RustWing::new(
        RustWingConfig::default().with_default_connection_policy(ConnectionPolicy::UniqueClient),
    );
    let identities = (0..32)
        .map(|index| {
            Identity::default_connection("alice").with_client(if index % 2 == 0 {
                "phone"
            } else {
                "browser"
            })
        })
        .collect();

    let accepted = accept_identities_concurrently(wing.clone(), identities).await;
    let sessions = wing.list_user_sessions(&UserId::from("alice")).unwrap();

    assert_eq!(wing.connection_count().unwrap(), 2);
    assert_eq!(sessions.len(), 2);
    assert_eq!(
        sessions
            .iter()
            .filter(|session| session.client_id.as_ref().map(ClientId::as_str) == Some("phone"))
            .count(),
        1
    );
    assert_eq!(
        sessions
            .iter()
            .filter(|session| session.client_id.as_ref().map(ClientId::as_str) == Some("browser"))
            .count(),
        1
    );
    assert_eq!(
        accepted
            .iter()
            .filter(|accepted| !accepted.session.is_closed())
            .count(),
        2
    );
}

// Concurrent multi-session accepts retain every session 同一用户并发多会话接入会保留全部会话
#[tokio::test]
async fn concurrent_multi_session_accepts_retain_every_session() {
    let wing = RustWing::new(
        RustWingConfig::default().with_default_connection_policy(ConnectionPolicy::MultiSession),
    );
    let identities = (0..32)
        .map(|_| Identity::default_connection("alice"))
        .collect();

    let accepted = accept_identities_concurrently(wing.clone(), identities).await;

    assert_eq!(wing.connection_count().unwrap(), 32);
    assert_eq!(
        wing.list_user_sessions(&UserId::from("alice"))
            .unwrap()
            .len(),
        32
    );
    assert!(
        accepted
            .iter()
            .all(|accepted| !accepted.session.is_closed())
    );
}

// Concurrent accepts and unregisters preserve primary and reverse indexes 同一用户并发接入与注销会保持主索引和反向索引一致
#[tokio::test]
async fn concurrent_accepts_and_unregisters_keep_user_indexes_consistent() {
    let wing = RustWing::new(
        RustWingConfig::default().with_default_connection_policy(ConnectionPolicy::MultiSession),
    );
    let initial = accept_identities_concurrently(
        wing.clone(),
        (0..32)
            .map(|_| Identity::default_connection("alice"))
            .collect(),
    )
    .await;
    let barrier = Arc::new(tokio::sync::Barrier::new(33));
    let mut unregister_tasks = Vec::new();
    let mut accept_tasks = Vec::new();

    for accepted in initial.iter().take(16) {
        let wing = wing.clone();
        let session = accepted.session.clone();
        let barrier = barrier.clone();
        unregister_tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            wing.unregister(&session).await.unwrap();
        }));
    }
    for _ in 0..16 {
        let wing = wing.clone();
        let barrier = barrier.clone();
        accept_tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            wing.accept_user("alice").await.unwrap()
        }));
    }
    barrier.wait().await;
    for task in unregister_tasks {
        task.await.unwrap();
    }
    let mut newly_accepted = Vec::new();
    for task in accept_tasks {
        newly_accepted.push(task.await.unwrap());
    }

    let sessions = wing.list_user_sessions(&UserId::from("alice")).unwrap();
    let report = wing
        .send_to_user("alice", OutboundFrame::text("index check"))
        .await
        .unwrap();
    assert_eq!(wing.connection_count().unwrap(), 32);
    assert_eq!(sessions.len(), 32);
    assert_eq!(report.local_sessions, 32);
    assert_eq!(newly_accepted.len(), 16);
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
    let node_id = wing.config().node_id.clone();
    let _first = wing.accept_user("alice").await.unwrap();
    let _second = wing.accept_user("alice").await.unwrap();
    let initial = wing.stats_snapshot().unwrap();
    assert_eq!(initial.node_id, node_id);
    assert_eq!(initial.local_connections, 2);
    assert_eq!(initial.local_users, 1);
    assert_eq!(initial.cluster_nodes, 0);
    assert_eq!(initial.cluster_routes, 0);
    assert_eq!(initial.outbound_frames_enqueued_total, 0);
    assert_eq!(initial.outbound_frames_failed_total, 0);

    wing.send_to_user("alice", OutboundFrame::text("tracked"))
        .await
        .unwrap();
    let snapshot = wing.stats_snapshot().unwrap();

    assert_eq!(snapshot.local_connections, 2);
    assert_eq!(snapshot.local_users, 1);
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
    let wing = RustWing::with_cluster_checked(
        RustWingConfig {
            node_id: NodeId::from("node-a"),
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
    let wing = RustWing::with_cluster_checked(config, Some(cluster))
        .await
        .unwrap();

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
    let wing = RustWing::with_cluster_checked(
        RustWingConfig {
            node_id: NodeId::from("node-a"),
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
    let wing = RustWing::with_cluster_checked(
        RustWingConfig {
            node_id: NodeId::from("node-a"),
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
    let wing = RustWing::with_cluster_checked(config, Some(cluster))
        .await
        .unwrap();

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
    let wing = RustWing::with_cluster_checked(
        RustWingConfig {
            node_id: NodeId::from("node-a"),
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

// Failed presence claim rolls back the accepted session 在线状态仲裁失败会回滚已接收的会话
#[tokio::test]
async fn accept_rolls_back_when_presence_claim_fails() {
    // Build a clustered manager whose presence store rejects claims 构建在线状态仲裁会失败的集群管理器
    let cluster = Cluster::new(FailingPresenceStore, RecordingPublisher::default());
    let wing = RustWing::with_cluster_checked(
        RustWingConfig {
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

    // Accepting the session must surface the claim error 接收会话必须返回仲裁错误
    let result = wing.accept(Identity::new("default", "alice")).await;

    // Confirm the failed accept did not leave a local session behind 确认失败的接收不会留下本地会话
    assert!(matches!(result, Err(RustWingError::Cluster(message)) if message == "claim failed"));
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

// Start identity accepts at one barrier to exercise same-user races 通过同一屏障启动身份接入以制造同用户竞争
async fn accept_identities_concurrently(
    wing: RustWing,
    identities: Vec<Identity>,
) -> Vec<AcceptedSession> {
    let barrier = Arc::new(tokio::sync::Barrier::new(identities.len() + 1));
    let mut tasks = Vec::with_capacity(identities.len());
    for identity in identities {
        let wing = wing.clone();
        let barrier = barrier.clone();
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            wing.accept(identity).await.unwrap()
        }));
    }
    barrier.wait().await;

    let mut accepted = Vec::with_capacity(tasks.len());
    for task in tasks {
        accepted.push(task.await.unwrap());
    }
    accepted
}

// Wait until the runtime reaches the expected lifecycle status 等待运行时到达预期生命周期状态
async fn wait_for_runtime_status(
    wing: &RustWing,
    expected: RuntimeStatus,
    timeout: Duration,
) -> RuntimeStatus {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let status = wing.runtime_status();
        if status == expected || tokio::time::Instant::now() >= deadline {
            return status;
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

    // Reject route claims for rollback tests 为回滚测试拒绝路由仲裁
    async fn claim(
        &self,
        _route: Route,
        _policy: ConnectionPolicy,
        _ttl: Duration,
    ) -> Result<RouteClaim> {
        Err(RustWingError::Cluster("claim failed".into()))
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
    ) -> Result<RouteRefresh> {
        Ok(RouteRefresh::Lost)
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
    // Whether node registration should fail 节点注册是否应失败
    fail_node_registration: Arc<AtomicBool>,
}

#[async_trait]
impl PresenceStore for SharedPresenceStore {
    // Register a route through the shared store 通过共享存储注册路由
    async fn register(&self, route: Route, ttl: Duration) -> Result<()> {
        self.inner.register(route, ttl).await
    }

    // Claim a route through the shared store 通过共享存储原子仲裁路由
    async fn claim(
        &self,
        route: Route,
        policy: ConnectionPolicy,
        ttl: Duration,
    ) -> Result<RouteClaim> {
        self.inner.claim(route, policy, ttl).await
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
    ) -> Result<RouteRefresh> {
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
        if self.fail_node_registration.load(Ordering::Acquire) {
            return Err(RustWingError::Cluster(
                "test node lease refresh failed".into(),
            ));
        }
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

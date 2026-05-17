use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use rust_wing::{
    Cluster, ClusterConfig, ClusterEnvelope, ConnectionPolicy, Identity, MemoryPresenceStore,
    NodeId, NodePublisher, OutboundFrame, PresenceStore, Result, Route, RustWing, RustWingConfig,
    UserId,
};

#[tokio::test]
async fn single_connection_policy_replaces_previous_session() {
    let wing = RustWing::new(RustWingConfig::default());

    let first = wing.accept(Identity::new("alice")).await.unwrap();
    let first_id = first.session.id().clone();
    let second = wing.accept(Identity::new("alice")).await.unwrap();

    assert!(first.session.is_closed());
    assert_ne!(first_id, *second.session.id());
    assert_eq!(wing.connection_count().unwrap(), 1);
}

#[tokio::test]
async fn multi_connection_policy_keeps_all_sessions() {
    let config = RustWingConfig {
        connection_policy: ConnectionPolicy::Multi,
        ..RustWingConfig::default()
    };
    let wing = RustWing::new(config);

    let _first = wing.accept(Identity::new("alice")).await.unwrap();
    let _second = wing
        .accept(Identity::new("alice").with_device("phone"))
        .await
        .unwrap();

    assert_eq!(wing.connection_count().unwrap(), 2);
    assert_eq!(
        wing.list_user_sessions(&UserId::from("alice"))
            .unwrap()
            .len(),
        2
    );
}

#[tokio::test]
async fn send_to_user_prefers_local_session() {
    let wing = RustWing::new(RustWingConfig::default());
    let accepted = wing.accept(Identity::new("alice")).await.unwrap();

    let sent = wing
        .send_to_user("alice", OutboundFrame::text("hello"))
        .await
        .unwrap();

    assert_eq!(sent, 1);
    drop(accepted);
}

#[tokio::test]
async fn remote_route_publishes_to_target_node() {
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

    let sent = wing
        .send_to_user("alice", OutboundFrame::text("hello"))
        .await
        .unwrap();

    let published = published.lock().unwrap();
    assert_eq!(sent, 1);
    assert_eq!(published.len(), 1);
    assert_eq!(published[0].0, NodeId::from("node-b"));
    assert_eq!(published[0].1.user_id, UserId::from("alice"));
}

#[derive(Default)]
struct RecordingPublisher {
    published: Arc<Mutex<Vec<(NodeId, ClusterEnvelope)>>>,
}

#[async_trait]
impl NodePublisher for RecordingPublisher {
    async fn publish(&self, node_id: &NodeId, envelope: ClusterEnvelope) -> Result<()> {
        self.published
            .lock()
            .unwrap()
            .push((node_id.clone(), envelope));
        Ok(())
    }
}

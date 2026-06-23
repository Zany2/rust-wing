use rust_wing_adapter::{
    ExternalMessage, ExternalMessageConsumerStats, ExternalMessageTarget, deliver_external_message,
    external_message_from_json, process_external_message_payload,
};
use rust_wing_core::{ConnectionPolicy, FrameKind, OutboundFrame, RustWing, RustWingConfig};
use serde_json::json;

// External user messages deliver through RustWing 外部用户消息会通过 RustWing 投递
#[tokio::test]
async fn external_user_message_delivers_to_default_user() {
    let wing = RustWing::new(
        RustWingConfig::default().with_default_connection_policy(ConnectionPolicy::MultiSession),
    );
    let mut accepted = wing.accept_user("alice").await.unwrap();
    let message = ExternalMessage::text(ExternalMessageTarget::user("alice"), "hello");

    let report = deliver_external_message(&wing, message).await.unwrap();
    let frame = accepted.outbound.recv().await.unwrap();

    assert_eq!(report.local_sessions, 1);
    assert_eq!(frame.payload, b"hello");
}

// External JSON messages can target exact sessions 外部 JSON 消息可以投递到精确会话
#[tokio::test]
async fn external_json_message_delivers_to_session_with_ack() {
    let wing = RustWing::new(RustWingConfig::default());
    let mut accepted = wing.accept_user("alice").await.unwrap();
    let message_id = wing.next_message_id();
    let payload = json!({
        "target": {
            "type": "session",
            "session_id": accepted.session.id().as_str()
        },
        "payload": {
            "kind": "text",
            "data": "session hello"
        },
        "message_id": message_id.as_str()
    });
    let message = external_message_from_json(payload.to_string()).unwrap();

    let report = deliver_external_message(&wing, message).await.unwrap();
    let frame = accepted.outbound.recv().await.unwrap();

    assert_eq!(report.local_sessions, 1);
    assert_eq!(frame.payload, b"session hello");
    assert_eq!(frame.message_id.as_ref(), Some(&message_id));
    assert_eq!(wing.ack_pending_count(), 1);
}

// External binary broadcast messages reach local sessions 外部二进制广播消息会到达本地会话
#[tokio::test]
async fn external_binary_broadcast_reaches_local_sessions() {
    let wing = RustWing::new(
        RustWingConfig::default().with_default_connection_policy(ConnectionPolicy::MultiSession),
    );
    let mut first = wing.accept_user("alice").await.unwrap();
    let mut second = wing.accept_user("bob").await.unwrap();
    let message = ExternalMessage::binary(ExternalMessageTarget::broadcast(), [1, 2, 3]);

    let report = deliver_external_message(&wing, message).await.unwrap();
    let first_frame = first.outbound.recv().await.unwrap();
    let second_frame = second.outbound.recv().await.unwrap();

    assert_eq!(report.local_sessions, 2);
    assert_eq!(first_frame, OutboundFrame::binary([1, 2, 3]));
    assert_eq!(second_frame, OutboundFrame::binary([1, 2, 3]));
}

// External disconnect messages remove matching sessions 外部断开消息会移除匹配会话
#[tokio::test]
async fn external_disconnect_user_removes_default_sessions() {
    let wing = RustWing::new(
        RustWingConfig::default().with_default_connection_policy(ConnectionPolicy::MultiSession),
    );
    let mut first = wing.accept_user("alice").await.unwrap();
    let mut second = wing.accept_user("alice").await.unwrap();
    let message = ExternalMessage::text(
        ExternalMessageTarget::disconnect_user("alice", "broker kick"),
        "",
    );

    let report = deliver_external_message(&wing, message).await.unwrap();

    assert_eq!(report.local_sessions, 2);
    assert_eq!(first.outbound.recv().await.unwrap().kind, FrameKind::Close);
    assert_eq!(second.outbound.recv().await.unwrap().kind, FrameKind::Close);
    assert_eq!(wing.connection_count().unwrap(), 0);
}

// External disconnect JSON does not require message payload 外部断开 JSON 不需要消息负载
#[tokio::test]
async fn external_disconnect_session_json_can_omit_payload() {
    let wing = RustWing::new(RustWingConfig::default());
    let mut accepted = wing.accept_user("alice").await.unwrap();
    let payload = json!({
        "target": {
            "type": "disconnect_session",
            "session_id": accepted.session.id().as_str(),
            "reason": "json kick"
        }
    });
    let message = external_message_from_json(payload.to_string()).unwrap();

    let report = deliver_external_message(&wing, message).await.unwrap();

    assert_eq!(report.local_sessions, 1);
    assert_eq!(
        accepted.outbound.recv().await.unwrap().kind,
        FrameKind::Close
    );
    assert_eq!(wing.connection_count().unwrap(), 0);
}

// External payload processing updates consumer counters 外部负载处理会更新消费计数器
#[tokio::test]
async fn external_payload_processing_updates_consumer_stats() {
    let wing = RustWing::new(
        RustWingConfig::default().with_default_connection_policy(ConnectionPolicy::MultiSession),
    );
    let mut accepted = wing.accept_user("alice").await.unwrap();
    let stats = ExternalMessageConsumerStats::default();
    let payload = json!({
        "target": {
            "type": "user",
            "user_id": "alice"
        },
        "payload": {
            "kind": "text",
            "data": "hello from broker"
        }
    });

    let delivered = process_external_message_payload(&wing, &stats, payload.to_string()).await;
    let failed = process_external_message_payload(&wing, &stats, b"not json").await;
    let frame = accepted.outbound.recv().await.unwrap();
    let snapshot = stats.snapshot();

    assert_eq!(delivered.unwrap().local_sessions, 1);
    assert!(failed.is_none());
    assert_eq!(frame.payload, b"hello from broker");
    assert_eq!(snapshot.received, 2);
    assert_eq!(snapshot.decoded, 1);
    assert_eq!(snapshot.delivered, 1);
    assert_eq!(snapshot.decode_failed, 1);
    assert_eq!(snapshot.deliver_failed, 0);
}

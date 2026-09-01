use tokio::sync::broadcast;

use crate::lifecycle::SessionEvent;

use super::RustWing;

impl RustWing {
    // Subscribe to non-blocking session lifecycle notifications 订阅非阻塞会话生命周期通知
    pub fn subscribe_session_events(&self) -> broadcast::Receiver<SessionEvent> {
        self.inner.session_events.subscribe()
    }

    // Publish one lifecycle notification without blocking session management 发布一条生命周期通知且不阻塞会话管理
    pub(super) fn emit_session_event(&self, event: SessionEvent) {
        let _ = self.inner.session_events.send(event);
    }
}

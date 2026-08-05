use crate::cluster::Route;
use crate::error::Result;
use crate::identity::{ConnectionType, NodeId, SessionId, UserId};
use crate::session::{Session, SessionSnapshot};

use super::RustWing;

impl RustWing {
    // Look up one session by id 按标识查找一个会话
    pub fn get_session(&self, session_id: &SessionId) -> Result<Option<Session>> {
        Ok(self
            .inner
            .registry
            .by_session
            .get(session_id)
            .map(|session| session.value().clone()))
    }

    // List snapshots for one default-system user's sessions 列出默认连接体系中某个用户的会话快照
    pub fn list_user_sessions(&self, user_id: &UserId) -> Result<Vec<SessionSnapshot>> {
        self.list_user_sessions_in(&ConnectionType::default(), user_id)
    }

    // List snapshots for one user's sessions in one connection system 列出某个连接体系中某个用户的会话快照
    pub fn list_user_sessions_in(
        &self,
        connection_type: &ConnectionType,
        user_id: &UserId,
    ) -> Result<Vec<SessionSnapshot>> {
        Ok(self
            .sessions_for_user(connection_type, user_id)
            .into_iter()
            .map(|session| session.snapshot())
            .collect())
    }

    // List snapshots for all local sessions 列出全部本地会话快照
    pub fn list_sessions(&self) -> Result<Vec<SessionSnapshot>> {
        Ok(self
            .all_sessions()
            .into_iter()
            .map(|session| session.snapshot())
            .collect())
    }

    // List snapshots for all local sessions in one connection system 列出某个连接体系中的全部本地会话快照
    pub fn list_sessions_in(
        &self,
        connection_type: &ConnectionType,
    ) -> Result<Vec<SessionSnapshot>> {
        Ok(self
            .inner
            .registry
            .sessions_for_connection_type(connection_type)
            .into_iter()
            .map(|session| session.snapshot())
            .collect())
    }

    // Count active local sessions 统计活跃本地会话
    pub fn connection_count(&self) -> Result<usize> {
        Ok(self.inner.registry.by_session.len())
    }

    // List live nodes visible through the configured cluster store 列出配置的集群存储中可见的活跃节点
    pub async fn list_cluster_nodes(&self) -> Result<Vec<NodeId>> {
        let Some(cluster) = &self.inner.cluster else {
            return Ok(Vec::new());
        };
        if !self.inner.config.cluster.enabled {
            return Ok(Vec::new());
        }
        cluster.presence.list_nodes().await
    }

    // List live routes in one connection system through the cluster store 列出集群存储中某个连接体系的活跃路由
    pub async fn list_cluster_routes(
        &self,
        connection_type: &ConnectionType,
    ) -> Result<Vec<Route>> {
        let Some(cluster) = &self.inner.cluster else {
            return Ok(Vec::new());
        };
        if !self.inner.config.cluster.enabled {
            return Ok(Vec::new());
        }
        cluster.presence.list_routes(connection_type).await
    }

    // List live routes across all connection systems through the cluster store 列出集群存储中全部连接体系的活跃路由
    pub async fn list_all_cluster_routes(&self) -> Result<Vec<Route>> {
        let Some(cluster) = &self.inner.cluster else {
            return Ok(Vec::new());
        };
        if !self.inner.config.cluster.enabled {
            return Ok(Vec::new());
        }
        cluster.presence.list_all_routes().await
    }

    // Snapshot all local sessions for one user 获取某个用户的全部本地会话快照
    pub(super) fn sessions_for_user(
        &self,
        connection_type: &ConnectionType,
        user_id: &UserId,
    ) -> Vec<Session> {
        self.inner
            .registry
            .sessions_for_user(connection_type, user_id)
    }

    // Snapshot all local sessions 获取全部本地会话快照
    pub(super) fn all_sessions(&self) -> Vec<Session> {
        self.inner.registry.all_sessions()
    }
}

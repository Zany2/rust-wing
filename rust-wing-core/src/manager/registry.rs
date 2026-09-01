use std::collections::HashSet;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::{Mutex, MutexGuard};

use dashmap::DashMap;
use dashmap::mapref::entry::Entry;

use crate::config::{ConnectionPolicy, RustWingConfig};
use crate::identity::{ClientId, ConnectionType, SessionId, UserId};
use crate::session::Session;

// Fixed user-lock shard count keeps coordination bounded 固定用户锁分片数量以保持协调资源有界
const USER_LOCK_SHARDS: usize = 128;

// Local indexes for active sessions 活跃会话的本地索引
pub(super) struct Registry {
    // Direct session lookup by id 按会话标识直接查找
    pub(super) by_session: DashMap<SessionId, Session>,
    // Reverse index from connection-user pairs to session ids 连接体系用户组合到会话标识的反向索引
    by_user: DashMap<UserRouteKey, HashSet<SessionId>>,
    // Reverse index from connection-user-client triples to session ids 连接体系用户客户端组合到会话标识的反向索引
    by_client: DashMap<ClientRouteKey, HashSet<SessionId>>,
    // Short critical sections protecting all indexes for one user 保护单个用户全部索引的短临界区
    mutation_locks: Box<[Mutex<()>]>,
    // Async operations serialized for one user 同一用户串行化的异步操作
    operation_locks: Box<[tokio::sync::Mutex<()>]>,
}

// User routing key scoped by connection system 按连接体系隔离的用户路由键
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct UserRouteKey {
    // Connection system identifier 连接体系标识
    connection_type: ConnectionType,
    // Owning user identifier 所属用户标识
    user_id: UserId,
}

// User and optional client routing key 用户与可选客户端路由键
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct ClientRouteKey {
    // Connection system identifier 连接体系标识
    connection_type: ConnectionType,
    // Owning user identifier 所属用户标识
    user_id: UserId,
    // Optional client identifier 可选客户端标识
    client_id: Option<ClientId>,
}

impl Registry {
    // Return the async operation lock assigned to one connection-user key 返回连接体系用户键对应的异步操作锁
    pub(super) fn operation_lock(
        &self,
        connection_type: &ConnectionType,
        user_id: &UserId,
    ) -> &tokio::sync::Mutex<()> {
        &self.operation_locks[user_lock_index(connection_type, user_id)]
    }

    // Insert a session and optionally replace existing user sessions 插入会话并按需替换用户旧会话
    pub(super) fn insert(&self, session: Session, config: &RustWingConfig) -> Vec<Session> {
        let _mutation_guard = self.mutation_guard(session.connection_type(), session.user_id());
        let user_key = UserRouteKey::from_session(&session);
        let session_id = session.id().clone();
        let client_key = ClientRouteKey::from_session(&session);
        let policy = config.policy_for(session.connection_type());

        // Select displaced ids while the user mutation lock is held 在用户变更锁保护下选择被替换标识
        let replaced_ids = match policy {
            ConnectionPolicy::UniqueUser => self.session_ids_for_user_key(&user_key),
            ConnectionPolicy::UniqueClient => self.session_ids_for_client_key(&client_key),
            ConnectionPolicy::MultiSession => Vec::new(),
        };
        // Remove displaced sessions from every index before adding the new session 新会话加入前从全部索引移除被替换会话
        let replaced = replaced_ids
            .into_iter()
            .filter_map(|id| {
                self.by_session.remove(&id).map(|(_, session)| {
                    self.remove_indexes(&session);
                    session
                })
            })
            .collect::<Vec<_>>();
        // Store the new session in the primary index 将新会话写入主索引
        self.by_session.insert(session_id.clone(), session);
        self.insert_indexes(user_key, client_key, session_id);
        replaced
    }

    // Remove one exact session from both indexes 从两个索引中移除一个精确会话
    pub(super) fn remove(&self, session: &Session) -> Option<Session> {
        let _mutation_guard = self.mutation_guard(session.connection_type(), session.user_id());
        let (_, removed) = self.by_session.remove(session.id())?;
        self.remove_indexes(&removed);
        Some(removed)
    }

    // Snapshot all local sessions for one user 获取某个用户的全部本地会话快照
    pub(super) fn sessions_for_user(
        &self,
        connection_type: &ConnectionType,
        user_id: &UserId,
    ) -> Vec<Session> {
        let user_key = UserRouteKey::new(connection_type.clone(), user_id.clone());
        // Copy session ids before looking up sessions 先复制会话标识再查询会话
        let session_ids = self
            .by_user
            .get(&user_key)
            .map(|ids| ids.iter().cloned().collect::<Vec<_>>())
            .unwrap_or_default();

        session_ids
            .into_iter()
            .filter_map(|id| {
                self.by_session
                    .get(&id)
                    .map(|session| session.value().clone())
            })
            .collect()
    }

    // Snapshot all local sessions for one user-client key 获取某个用户客户端键的全部本地会话快照
    pub(super) fn sessions_for_client_key(&self, client_key: &ClientRouteKey) -> Vec<Session> {
        let session_ids = self.session_ids_for_client_key(client_key);
        session_ids
            .into_iter()
            .filter_map(|id| {
                self.by_session
                    .get(&id)
                    .map(|session| session.value().clone())
            })
            .collect()
    }

    // Snapshot every local session 获取所有本地会话快照
    pub(super) fn all_sessions(&self) -> Vec<Session> {
        self.by_session
            .iter()
            .map(|entry| entry.value().clone())
            .collect()
    }

    // Count unique users across local sessions 统计本地会话中的去重用户数
    pub(super) fn user_count(&self) -> usize {
        self.by_session
            .iter()
            .map(|entry| entry.value().user_id().clone())
            .collect::<HashSet<_>>()
            .len()
    }

    // Snapshot a bounded session window for sharded maintenance 为分片维护获取有界会话窗口
    pub(super) fn session_window(&self, start: usize, limit: usize) -> Vec<Session> {
        if limit == 0 {
            return Vec::new();
        }
        let total = self.by_session.len();
        if total == 0 {
            return Vec::new();
        }

        let start = start % total;
        let target = limit.min(total);
        let mut sessions = Vec::with_capacity(target);

        let mut index = 0;
        for entry in self.by_session.iter() {
            if index >= start {
                sessions.push(entry.value().clone());
                if sessions.len() == target {
                    return sessions;
                }
            }
            index += 1;
        }

        if sessions.len() < target {
            let mut index = 0;
            for entry in self.by_session.iter() {
                if index >= start || sessions.len() == target {
                    break;
                }
                sessions.push(entry.value().clone());
                index += 1;
            }
        }

        sessions
    }

    // List sessions in one connection system 列出某个连接体系中的全部会话
    pub(super) fn sessions_for_connection_type(
        &self,
        connection_type: &ConnectionType,
    ) -> Vec<Session> {
        self.by_session
            .iter()
            .filter(|entry| entry.value().connection_type() == connection_type)
            .map(|entry| entry.value().clone())
            .collect()
    }

    // Snapshot session ids for one user 获取某个用户的会话标识快照
    fn session_ids_for_user_key(&self, user_key: &UserRouteKey) -> Vec<SessionId> {
        self.by_user
            .get(user_key)
            .map(|ids| ids.iter().cloned().collect::<Vec<_>>())
            .unwrap_or_default()
    }

    // Snapshot session ids for one user-client key 获取某个用户客户端键的会话标识快照
    fn session_ids_for_client_key(&self, client_key: &ClientRouteKey) -> Vec<SessionId> {
        self.by_client
            .get(client_key)
            .map(|ids| ids.iter().cloned().collect::<Vec<_>>())
            .unwrap_or_default()
    }

    // Lock the short index-mutation section for one user 锁定单个用户的短索引变更临界区
    fn mutation_guard(
        &self,
        connection_type: &ConnectionType,
        user_id: &UserId,
    ) -> MutexGuard<'_, ()> {
        self.mutation_locks[user_lock_index(connection_type, user_id)]
            .lock()
            .unwrap_or_else(|error| error.into_inner())
    }

    // Add one session id to reverse indexes 将一个会话标识加入反向索引
    fn insert_indexes(
        &self,
        user_key: UserRouteKey,
        client_key: ClientRouteKey,
        session_id: SessionId,
    ) {
        self.by_user
            .entry(user_key)
            .or_default()
            .insert(session_id.clone());
        self.by_client
            .entry(client_key)
            .or_default()
            .insert(session_id);
    }

    // Remove one exact session id from reverse indexes 从反向索引移除一个精确会话标识
    fn remove_indexes(&self, session: &Session) {
        let user_key = UserRouteKey::from_session(session);
        let session_id = session.id().clone();
        let client_key = ClientRouteKey::from_session(session);

        // Remove the session id from the user's reverse index 从用户反向索引中移除会话标识
        let should_prune_user = match self.by_user.entry(user_key.clone()) {
            Entry::Occupied(mut entry) => {
                let ids = entry.get_mut();
                ids.remove(&session_id);
                ids.is_empty()
            }
            Entry::Vacant(_) => false,
        };
        // Drop empty user buckets after the entry guard is gone 在 entry guard 释放后清理空用户桶
        if should_prune_user {
            self.by_user.remove(&user_key);
        }

        // Remove the session id from the client reverse index 从客户端反向索引移除会话标识
        let should_prune_client = match self.by_client.entry(client_key.clone()) {
            Entry::Occupied(mut entry) => {
                let ids = entry.get_mut();
                ids.remove(&session_id);
                ids.is_empty()
            }
            Entry::Vacant(_) => false,
        };
        if should_prune_client {
            self.by_client.remove(&client_key);
        }
    }
}

impl Default for Registry {
    fn default() -> Self {
        Self {
            by_session: DashMap::new(),
            by_user: DashMap::new(),
            by_client: DashMap::new(),
            mutation_locks: (0..USER_LOCK_SHARDS)
                .map(|_| Mutex::new(()))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            operation_locks: (0..USER_LOCK_SHARDS)
                .map(|_| tokio::sync::Mutex::new(()))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        }
    }
}

// Select one stable lock shard for a connection-user key 为连接体系用户键选择稳定锁分片
fn user_lock_index(connection_type: &ConnectionType, user_id: &UserId) -> usize {
    let mut hasher = DefaultHasher::new();
    connection_type.hash(&mut hasher);
    user_id.hash(&mut hasher);
    hasher.finish() as usize % USER_LOCK_SHARDS
}

impl UserRouteKey {
    // Build a key from explicit identity parts 通过显式身份字段构建键
    fn new(connection_type: ConnectionType, user_id: UserId) -> Self {
        Self {
            connection_type,
            user_id,
        }
    }

    // Build a key from a live session 通过活跃会话构建键
    fn from_session(session: &Session) -> Self {
        Self::new(session.connection_type().clone(), session.user_id().clone())
    }
}

impl ClientRouteKey {
    // Build a key from explicit identity parts 通过显式身份字段构建键
    pub(super) fn new(
        connection_type: ConnectionType,
        user_id: UserId,
        client_id: Option<ClientId>,
    ) -> Self {
        Self {
            connection_type,
            user_id,
            client_id,
        }
    }

    // Build a key from a live session 通过活跃会话构建键
    fn from_session(session: &Session) -> Self {
        Self {
            connection_type: session.connection_type().clone(),
            user_id: session.user_id().clone(),
            client_id: session.client_id().cloned(),
        }
    }
}

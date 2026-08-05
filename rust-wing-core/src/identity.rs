use std::fmt::{self, Display};

// Define a strongly typed string identifier 定义强类型字符串标识
macro_rules! string_id {
    ($name:ident) => {
        #[derive(
            Debug,
            Clone,
            PartialEq,
            Eq,
            Hash,
            PartialOrd,
            Ord,
            serde::Serialize,
            serde::Deserialize,
        )]
        pub struct $name(String);

        impl $name {
            // Create an identifier from text 通过文本创建标识
            pub fn new(value: impl Into<String>) -> Self {
                Self(value.into())
            }

            // Borrow the identifier text 借用标识文本
            pub fn as_str(&self) -> &str {
                &self.0
            }

            // Consume the identifier into text 将标识转换为文本
            pub fn into_string(self) -> String {
                self.0
            }
        }

        impl From<&str> for $name {
            // Convert borrowed text into an identifier 将借用文本转换为标识
            fn from(value: &str) -> Self {
                Self::new(value)
            }
        }

        impl From<String> for $name {
            // Convert owned text into an identifier 将拥有的文本转换为标识
            fn from(value: String) -> Self {
                Self::new(value)
            }
        }

        impl Display for $name {
            // Render the raw identifier text 渲染原始标识文本
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }
    };
}

// Node identifier 节点标识
string_id!(NodeId);
// Session identifier 会话标识
string_id!(SessionId);
// Connection type identifier 连接体系标识
string_id!(ConnectionType);
// User identifier 用户标识
string_id!(UserId);
// Client identifier 客户端标识
string_id!(ClientId);

impl Default for ConnectionType {
    // Use the default connection type when callers do not need multiple systems 调用方不需要多体系时使用默认连接体系
    fn default() -> Self {
        Self::new("default")
    }
}

impl NodeId {
    // Generate a practically unique node identifier 生成实际使用中唯一的节点标识
    pub fn generate() -> Self {
        Self(format!("node-{}", uuid_v7_simple()))
    }
}

// Logical client identity 逻辑客户端身份
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Identity {
    // Connection system identifier 连接体系标识
    pub connection_type: ConnectionType,
    // Owning user identifier 所属用户标识
    pub user_id: UserId,
    // Optional client identifier 可选客户端标识
    pub client_id: Option<ClientId>,
}

impl Identity {
    // Create an identity in one connection system 在指定连接体系中创建身份
    pub fn new(connection_type: impl Into<ConnectionType>, user_id: impl Into<UserId>) -> Self {
        Self {
            connection_type: connection_type.into(),
            user_id: user_id.into(),
            client_id: None,
        }
    }

    // Create an identity in the default connection system 在默认连接体系中创建身份
    pub fn default_connection(user_id: impl Into<UserId>) -> Self {
        Self::new(ConnectionType::default(), user_id)
    }

    // Attach a client identifier to the identity 为身份附加客户端标识
    pub fn with_client(mut self, client_id: impl Into<ClientId>) -> Self {
        self.client_id = Some(client_id.into());
        self
    }
}

impl SessionId {
    // Generate a node-scoped time-ordered session identifier 生成带节点前缀且按时间大致有序的会话标识
    pub fn generate(node_id: &NodeId) -> Self {
        Self(format!("{}-{}", node_id.as_str(), uuid_v7_simple()))
    }
}

// Generate a hyphen-free UUID v7 text value 生成不带连字符的 UUID v7 文本值
pub(crate) fn uuid_v7_simple() -> String {
    uuid::Uuid::now_v7().simple().to_string()
}

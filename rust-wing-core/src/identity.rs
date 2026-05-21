use std::fmt::{self, Display};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

// Monotonic session sequence source 单调递增的会话序列源
static SESSION_COUNTER: AtomicU64 = AtomicU64::new(1);

// Define a strongly typed string identifier 定义强类型字符串标识
macro_rules! string_id {
    ($name:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
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
// User identifier 用户标识
string_id!(UserId);
// Device identifier 设备标识
string_id!(DeviceId);

// Logical client identity 逻辑客户端身份
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Identity {
    // Owning user identifier 所属用户标识
    pub user_id: UserId,
    // Optional device identifier 可选设备标识
    pub device_id: Option<DeviceId>,
}

impl Identity {
    // Create an identity for one user 为单个用户创建身份
    pub fn new(user_id: impl Into<UserId>) -> Self {
        Self {
            user_id: user_id.into(),
            device_id: None,
        }
    }

    // Attach a device identifier to the identity 为身份附加设备标识
    pub fn with_device(mut self, device_id: impl Into<DeviceId>) -> Self {
        self.device_id = Some(device_id.into());
        self
    }
}

impl SessionId {
    // Generate a mostly unique session identifier 生成基本唯一的会话标识
    pub fn generate(node_id: &NodeId) -> Self {
        // Capture the current epoch timestamp 获取当前纪元时间戳
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis())
            .unwrap_or_default();
        // Reserve the next local sequence value 预留下一个本地序列值
        let sequence = SESSION_COUNTER.fetch_add(1, Ordering::Relaxed);
        // Combine node, time, and sequence into one identifier 合并节点、时间和序列生成标识
        Self(format!("{}-{}-{}", node_id.as_str(), millis, sequence))
    }
}

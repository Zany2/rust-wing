use std::fmt::{self, Display};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static SESSION_COUNTER: AtomicU64 = AtomicU64::new(1);

macro_rules! string_id {
    ($name:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Self {
                Self(value.into())
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }

            pub fn into_string(self) -> String {
                self.0
            }
        }

        impl From<&str> for $name {
            fn from(value: &str) -> Self {
                Self::new(value)
            }
        }

        impl From<String> for $name {
            fn from(value: String) -> Self {
                Self::new(value)
            }
        }

        impl Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }
    };
}

string_id!(NodeId);
string_id!(SessionId);
string_id!(UserId);
string_id!(DeviceId);

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Identity {
    pub user_id: UserId,
    pub device_id: Option<DeviceId>,
}

impl Identity {
    pub fn new(user_id: impl Into<UserId>) -> Self {
        Self {
            user_id: user_id.into(),
            device_id: None,
        }
    }

    pub fn with_device(mut self, device_id: impl Into<DeviceId>) -> Self {
        self.device_id = Some(device_id.into());
        self
    }
}

impl SessionId {
    pub fn generate(node_id: &NodeId) -> Self {
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis())
            .unwrap_or_default();
        let sequence = SESSION_COUNTER.fetch_add(1, Ordering::Relaxed);
        Self(format!("{}-{}-{}", node_id.as_str(), millis, sequence))
    }
}

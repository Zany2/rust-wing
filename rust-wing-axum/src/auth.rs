use std::sync::Arc;

use async_trait::async_trait;
use axum::http::{HeaderMap, HeaderName, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use rust_wing_core::Identity;

// Context passed to application authenticators 传递给应用认证器的上下文
#[derive(Debug, Clone)]
pub struct AxumAuthContext {
    // Incoming request headers 入站请求头
    pub headers: HeaderMap,
    // Incoming request URI 入站请求 URI
    pub uri: Uri,
}

impl AxumAuthContext {
    // Borrow the raw query string if one exists 借用原始查询字符串
    pub fn query(&self) -> Option<&str> {
        self.uri.query()
    }
}

// Authentication error returned before the WebSocket upgrade 升级 WebSocket 之前返回的认证错误
#[derive(Debug, Clone)]
pub struct AxumAuthError {
    // HTTP status returned to the client 返回给客户端的 HTTP 状态码
    pub status: StatusCode,
    // Human-readable failure reason 可读的失败原因
    pub message: String,
}

impl AxumAuthError {
    // Create an unauthorized error 创建未授权错误
    pub fn unauthorized(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            message: message.into(),
        }
    }

    // Create a forbidden error 创建禁止访问错误
    pub fn forbidden(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::FORBIDDEN,
            message: message.into(),
        }
    }

    // Create a bad request error 创建错误请求错误
    pub fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
        }
    }
}

impl IntoResponse for AxumAuthError {
    // Convert the authentication error into an HTTP response 将认证错误转换为 HTTP 响应
    fn into_response(self) -> Response {
        (self.status, self.message).into_response()
    }
}

// Application authenticator contract 应用认证器契约
#[async_trait]
pub trait AxumAuthenticator: Send + Sync + 'static {
    // Resolve an Identity from the incoming request 从入站请求解析 Identity
    async fn authenticate(
        &self,
        context: AxumAuthContext,
    ) -> std::result::Result<Identity, AxumAuthError>;
}

// Guard contract for the send API 发送接口的保护契约
#[async_trait]
pub trait AxumSendApiGuard: Send + Sync + 'static {
    // Validate whether the request may call the send API 校验请求是否可以调用发送接口
    async fn authorize(&self, headers: &HeaderMap) -> std::result::Result<(), AxumAuthError>;
}

// Guard that intentionally allows every request 有意允许全部请求的保护器
#[derive(Debug, Clone, Copy, Default)]
pub struct AllowAllSendApiGuard;

#[async_trait]
impl AxumSendApiGuard for AllowAllSendApiGuard {
    // Allow the request without additional checks 不做额外检查并允许请求
    async fn authorize(&self, _headers: &HeaderMap) -> std::result::Result<(), AxumAuthError> {
        Ok(())
    }
}

// API-key guard for send and management APIs 发送与管理接口使用的 API Key 保护器
#[derive(Debug, Clone)]
pub struct ApiKeySendApiGuard {
    // Header used to carry the API key 承载 API Key 的请求头
    header_name: HeaderName,
    // Expected API key value 期望的 API Key 值
    api_key: Arc<str>,
}

impl ApiKeySendApiGuard {
    // Build a guard that reads the x-api-key header 构建读取 x-api-key 请求头的保护器
    pub fn new(api_key: impl Into<String>) -> Self {
        Self::with_header_name(api_key, HeaderName::from_static("x-api-key"))
    }

    // Build a guard with a custom header name 构建使用自定义请求头名称的保护器
    pub fn with_header_name(api_key: impl Into<String>, header_name: HeaderName) -> Self {
        Self {
            header_name,
            api_key: Arc::from(api_key.into()),
        }
    }
}

#[async_trait]
impl AxumSendApiGuard for ApiKeySendApiGuard {
    // Require the configured header value to match the expected API key 要求配置的请求头值匹配期望 API Key
    async fn authorize(&self, headers: &HeaderMap) -> std::result::Result<(), AxumAuthError> {
        let Some(value) = headers.get(&self.header_name) else {
            return Err(AxumAuthError::unauthorized("missing api key"));
        };
        if value.as_bytes() == self.api_key.as_bytes() {
            return Ok(());
        }
        Err(AxumAuthError::forbidden("invalid api key"))
    }
}

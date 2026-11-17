use crate::models::{DiagnosedError, ErrorClass};
use serde::Serialize;
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AppError {
    pub class: ErrorClass,
    pub message: String,
    pub detail: Option<String>,
    pub retryable: bool,
    pub code: Option<String>,
}

impl AppError {
    pub fn new(class: ErrorClass, message: impl Into<String>) -> Self {
        let retryable = matches!(
            class,
            ErrorClass::ConnectionFailed | ErrorClass::Timeout | ErrorClass::Internal
        );
        Self {
            class,
            message: message.into(),
            detail: None,
            retryable,
            code: None,
        }
    }

    pub fn connection(message: impl Into<String>) -> Self {
        Self::new(ErrorClass::ConnectionFailed, message)
    }

    pub fn validation(message: impl Into<String>) -> Self {
        Self::new(ErrorClass::Validation, message)
    }

    pub fn unsupported(message: impl Into<String>) -> Self {
        Self::new(ErrorClass::ProtocolUnsupported, message)
    }

    pub fn cancelled() -> Self {
        Self::new(ErrorClass::Cancelled, "The operation was cancelled by the user.")
    }

    pub fn timeout(message: impl Into<String>) -> Self {
        Self::new(ErrorClass::Timeout, message)
    }

    pub fn with_detail(mut self, detail: impl Into<String>) -> Self {
        self.detail = Some(detail.into());
        self
    }

    pub fn with_code(mut self, code: impl Into<String>) -> Self {
        self.code = Some(code.into());
        self
    }

    pub fn diagnosed(&self) -> DiagnosedError {
        DiagnosedError {
            class: self.class.clone(),
            message: self.message.clone(),
            detail: self.detail.clone(),
            retryable: self.retryable,
            code: self.code.clone(),
        }
    }

    pub fn classify_message(message: &str) -> ErrorClass {
        let lower = message.to_ascii_lowercase();
        if lower.contains("permission denied")
            || lower.contains("access denied")
            || lower.contains("not authorized")
            || lower.contains("insufficient privilege")
            || lower.contains("authentication failed")
            || lower.contains("authentication required")
            || lower.contains("wrongpass")
            || lower.contains("noauth")
            || lower.contains("noperm")
        {
            ErrorClass::PermissionDenied
        } else if lower.contains("syntax")
            || lower.contains("parse error")
            || lower.contains("unexpected token")
        {
            ErrorClass::SyntaxError
        } else if lower.contains("timeout") || lower.contains("timed out") {
            ErrorClass::Timeout
        } else if lower.contains("cancelled")
            || lower.contains("canceled")
            || lower.contains("interrupted")
        {
            ErrorClass::Cancelled
        } else if lower.contains("connection refused")
            || lower.contains("connection reset")
            || lower.contains("could not connect")
            || lower.contains("network")
            || lower.contains("tls")
            || lower.contains("lookup address")
            || lower.contains("no route to host")
            || lower.contains("host is down")
            || lower.contains("broken pipe")
            || lower.contains("error communicating with database")
            || lower.contains("unknown database")
            || lower.contains("does not exist")
        {
            ErrorClass::ConnectionFailed
        } else if lower.contains("not supported")
            || lower.contains("unsupported")
            || lower.contains("unknown command")
        {
            ErrorClass::ProtocolUnsupported
        } else if lower.contains("duplicate")
            || lower.contains("conflict")
            || lower.contains("deadlock")
            || lower.contains("unique constraint")
            || lower.contains("foreign key constraint")
            || lower.contains("violates")
        {
            ErrorClass::Conflict
        } else {
            ErrorClass::Internal
        }
    }
}

impl Display for AppError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{:?}: {}", self.class, self.message)
    }
}

impl std::error::Error for AppError {}

impl From<sqlx::Error> for AppError {
    fn from(error: sqlx::Error) -> Self {
        let message = error.to_string();
        Self::new(Self::classify_message(&message), message).with_detail(format!("{error:?}"))
    }
}

impl From<rusqlite::Error> for AppError {
    fn from(error: rusqlite::Error) -> Self {
        let message = error.to_string();
        Self::new(Self::classify_message(&message), message).with_detail(format!("{error:?}"))
    }
}

impl From<r2d2::Error> for AppError {
    fn from(error: r2d2::Error) -> Self {
        let message = error.to_string();
        Self::new(Self::classify_message(&message), message).with_detail(format!("{error:?}"))
    }
}

impl From<redis::RedisError> for AppError {
    fn from(error: redis::RedisError) -> Self {
        let message = error.to_string();
        let class = if error.kind() == redis::ErrorKind::AuthenticationFailed {
            ErrorClass::PermissionDenied
        } else {
            Self::classify_message(&message)
        };
        Self::new(class, message).with_code(error.code().unwrap_or("redis").to_string())
    }
}

impl From<reqwest::Error> for AppError {
    fn from(error: reqwest::Error) -> Self {
        let message = error.to_string();
        let class = if error.is_timeout() {
            ErrorClass::Timeout
        } else if error.is_connect() {
            ErrorClass::ConnectionFailed
        } else {
            Self::classify_message(&message)
        };
        Self::new(class, format!("Elasticsearch HTTP request failed: {message}"))
    }
}

impl From<keyring::Error> for AppError {
    fn from(error: keyring::Error) -> Self {
        Self::new(
            ErrorClass::Internal,
            format!("Unable to read the previous keychain item for the one-time vault import: {error}"),
        )
    }
}

impl From<std::io::Error> for AppError {
    fn from(error: std::io::Error) -> Self {
        let message = error.to_string();
        Self::new(Self::classify_message(&message), message)
    }
}

impl From<base64::DecodeError> for AppError {
    fn from(error: base64::DecodeError) -> Self {
        Self::validation(format!("Invalid base64 data: {error}"))
    }
}

impl From<serde_json::Error> for AppError {
    fn from(error: serde_json::Error) -> Self {
        Self::new(ErrorClass::Internal, format!("Serialization failed: {error}"))
    }
}

impl From<tokio::time::error::Elapsed> for AppError {
    fn from(_: tokio::time::error::Elapsed) -> Self {
        Self::timeout("The operation exceeded its configured timeout.")
    }
}

impl From<tauri::Error> for AppError {
    fn from(error: tauri::Error) -> Self {
        Self::new(ErrorClass::Internal, format!("Window operation failed: {error}"))
    }
}

impl From<ssh2::Error> for AppError {
    fn from(error: ssh2::Error) -> Self {
        let message = format!("SSH tunnel: {error}");
        let class = match error.code() {
            ssh2::ErrorCode::Session(-18) => ErrorClass::PermissionDenied,
            _ => Self::classify_message(&message),
        };
        Self::new(class, message)
    }
}

pub type AppResult<T> = Result<T, AppError>;

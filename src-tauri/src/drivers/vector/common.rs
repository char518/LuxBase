use crate::error::{AppError, AppResult};
use crate::models::{ConnectionConfig, TlsMode};
use parking_lot::Mutex;
use reqwest::{Client, Method};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

/// Shared HTTP plumbing for the vector drivers: TLS-aware client construction, relative-path
/// safety, a cancellable request runner and the per-operation cancellation registry. Milvus and
/// Qdrant differ only in auth headers and response envelope, which each driver handles itself.
pub struct HttpCore {
    client: Option<Client>,
    base_url: String,
    timeout: Duration,
    cancellations: Mutex<HashMap<String, CancellationToken>>,
}

impl HttpCore {
    pub fn new() -> Self {
        Self {
            client: None,
            base_url: String::new(),
            timeout: Duration::from_secs(30),
            cancellations: Mutex::new(HashMap::new()),
        }
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    pub fn client(&self) -> AppResult<&Client> {
        self.client
            .as_ref()
            .ok_or_else(|| AppError::connection("The vector-store connection is not initialized."))
    }

    /// Builds the base URL and a client carrying `headers` on every request. `label` names the
    /// product in error messages. Server TLS with a custom CA is supported; client-cert TLS is not.
    pub fn configure(
        &mut self,
        config: &ConnectionConfig,
        default_port: u16,
        headers: reqwest::header::HeaderMap,
        label: &str,
    ) -> AppResult<()> {
        if config.host.trim().is_empty() {
            return Err(AppError::validation(format!("A {label} host is required.")));
        }
        self.timeout = Duration::from_millis(config.query_timeout_ms.clamp(1_000, 120_000));
        let tls = !matches!(config.tls_mode.unwrap_or(TlsMode::Disabled), TlsMode::Disabled);
        if tls
            && config
                .client_cert_path
                .as_deref()
                .is_some_and(|value| !value.trim().is_empty())
        {
            return Err(AppError::unsupported(format!(
                "{label} client-certificate TLS is not available in this version. Use token/api-key auth over server TLS, or disable TLS."
            )));
        }
        let scheme = if tls { "https" } else { "http" };
        let port = if config.port == 0 { default_port } else { config.port };
        self.base_url = format!("{scheme}://{}:{port}", config.host.trim());
        let mut builder = Client::builder()
            .timeout(self.timeout)
            .connect_timeout(Duration::from_millis(config.pool.connection_timeout_ms.max(1000)))
            .pool_max_idle_per_host(config.pool.max_size.max(1) as usize)
            .default_headers(headers);
        if tls {
            builder = builder.use_rustls_tls();
            if let Some(ca) = config.ca_cert_path.as_deref().filter(|value| !value.trim().is_empty()) {
                let pem = std::fs::read(ca).map_err(|error| {
                    AppError::validation(format!("Cannot read {label} CA certificate '{ca}': {error}"))
                })?;
                let certificate = reqwest::Certificate::from_pem(&pem)
                    .map_err(|error| AppError::validation(format!("Invalid {label} CA certificate: {error}")))?;
                builder = builder.add_root_certificate(certificate);
            }
        }
        self.client = Some(builder.build().map_err(AppError::from)?);
        Ok(())
    }

    pub fn url(&self, path: &str) -> AppResult<String> {
        if !path.starts_with('/') {
            return Err(AppError::validation("The request path must start with '/'."));
        }
        if path.split('/').any(|segment| segment == "..") {
            return Err(AppError::validation("The request path must not contain '..' segments."));
        }
        if path.contains("://") {
            return Err(AppError::validation("The request path must be relative to the connected server."));
        }
        Ok(format!("{}{}", self.base_url, path))
    }

    /// Sends a request and returns the HTTP status plus the parsed JSON body (or a text wrapper).
    /// Non-2xx responses are returned as-is so callers can decide how to surface them.
    pub async fn send(&self, method: Method, path: &str, body: Option<&Value>) -> AppResult<(u16, Value)> {
        let url = self.url(path)?;
        let mut request = self.client()?.request(method, &url);
        if let Some(body) = body {
            request = request.json(body);
        }
        let response = request.send().await?;
        let status = response.status().as_u16();
        let text = response.text().await?;
        let value = if text.trim().is_empty() {
            Value::Null
        } else {
            serde_json::from_str(&text).unwrap_or_else(|_| json!({ "raw": text }))
        };
        Ok((status, value))
    }

    fn token(&self, operation_id: &str) -> CancellationToken {
        let token = CancellationToken::new();
        self.cancellations
            .lock()
            .insert(operation_id.to_string(), token.clone());
        token
    }

    pub async fn run_cancellable<F, T>(&self, operation_id: &str, future: F) -> AppResult<T>
    where
        F: std::future::Future<Output = AppResult<T>>,
    {
        let token = self.token(operation_id);
        let result = tokio::select! {
            biased;
            _ = token.cancelled() => Err(AppError::cancelled()),
            result = tokio::time::timeout(self.timeout, future) => {
                match result {
                    Ok(value) => value,
                    Err(_) => Err(AppError::timeout("The vector-store request exceeded the configured timeout and was cancelled.")),
                }
            }
        };
        self.cancellations.lock().remove(operation_id);
        result
    }

    pub fn cancel(&self, operation_id: &str) -> AppResult<()> {
        let token = self
            .cancellations
            .lock()
            .get(operation_id)
            .cloned()
            .ok_or_else(|| AppError::validation(format!("Vector operation '{operation_id}' is not running.")))?;
        token.cancel();
        Ok(())
    }

    pub fn close(&self) {
        for (_, token) in self.cancellations.lock().drain() {
            token.cancel();
        }
    }
}

/// Percent-encodes a single path segment (collection names, point ids) so names with spaces or
/// unicode round-trip safely.
pub fn encode_path_segment(segment: &str) -> String {
    let mut out = String::new();
    for byte in segment.as_bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => out.push(*byte as char),
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

pub fn validate_collection_name(name: &str) -> AppResult<()> {
    let trimmed = name.trim();
    if trimmed.is_empty() {
        return Err(AppError::validation("A collection name is required."));
    }
    if trimmed.len() > 255 {
        return Err(AppError::validation("A collection name must not exceed 255 bytes."));
    }
    if trimmed.chars().any(|ch| ch.is_whitespace() || matches!(ch, '/' | '\\' | '"' | '\'')) {
        return Err(AppError::validation(
            "A collection name must not contain whitespace or / \\ \" ' characters.",
        ));
    }
    Ok(())
}
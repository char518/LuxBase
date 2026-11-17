use super::r#trait::SearchDriver;
use crate::error::{AppError, AppResult};
use crate::models::{
    ConnectionConfig, EsAlias, EsClusterInfo, EsDocument, EsIndex, EsRestRequest, EsRestResult,
    EsSearchRequest, EsSearchResult, EsHit, ErrorClass, TlsMode,
};
use async_trait::async_trait;
use base64::{engine::general_purpose::STANDARD as B64, Engine};
use parking_lot::Mutex;
use reqwest::{Client, Method, StatusCode};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

pub struct EsDriver {
    connection_id: String,
    client: Option<Client>,
    base_url: String,
    timeout: Duration,
    cancellations: Mutex<HashMap<String, CancellationToken>>,
}

impl EsDriver {
    pub fn new(connection_id: String) -> Self {
        Self {
            connection_id,
            client: None,
            base_url: String::new(),
            timeout: Duration::from_secs(30),
            cancellations: Mutex::new(HashMap::new()),
        }
    }

    fn client(&self) -> AppResult<&Client> {
        self.client
            .as_ref()
            .ok_or_else(|| AppError::connection("The Elasticsearch connection is not initialized."))
    }

    fn token(&self, operation_id: &str) -> CancellationToken {
        let token = CancellationToken::new();
        self.cancellations
            .lock()
            .insert(operation_id.to_string(), token.clone());
        token
    }

    fn finish(&self, operation_id: &str) {
        self.cancellations.lock().remove(operation_id);
    }

    async fn run_cancellable<F, T>(&self, operation_id: &str, future: F) -> AppResult<T>
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
                    Err(_) => Err(AppError::timeout("The Elasticsearch request exceeded the configured timeout and was cancelled.")),
                }
            }
        };
        self.finish(operation_id);
        result
    }

    fn url(&self, path: &str) -> AppResult<String> {
        if !path.starts_with('/') {
            return Err(AppError::validation("Elasticsearch request path must start with '/'."));
        }
        if path.split('/').any(|segment| segment == "..") {
            return Err(AppError::validation("Elasticsearch request path must not contain '..' segments."));
        }
        if path.contains("://") {
            return Err(AppError::validation("Elasticsearch request path must be relative to the connected cluster."));
        }
        Ok(format!("{}{}", self.base_url, path))
    }

    /// Sends a request and returns the HTTP status plus the parsed JSON body (or a text wrapper).
    /// Non-2xx responses are returned as-is so callers can decide how to surface them.
    async fn send(&self, method: Method, path: &str, body: Option<&Value>) -> AppResult<(u16, Value)> {
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

    async fn request_json(&self, method: Method, path: &str, body: Option<&Value>) -> AppResult<Value> {
        let (status, value) = self.send(method, path, body).await?;
        if (200..300).contains(&status) {
            Ok(value)
        } else {
            Err(es_status_error(status, &value))
        }
    }
}

fn es_status_error(status: u16, body: &Value) -> AppError {
    let reason = body
        .get("error")
        .map(|error| {
            if let Some(reason) = error.get("reason").and_then(Value::as_str) {
                let kind = error.get("type").and_then(Value::as_str).unwrap_or("error");
                format!("{kind}: {reason}")
            } else {
                error.to_string()
            }
        })
        .unwrap_or_else(|| body.to_string());
    let class = match status {
        401 | 403 => ErrorClass::PermissionDenied,
        404 => ErrorClass::Validation,
        400 | 405 | 406 => ErrorClass::SyntaxError,
        409 => ErrorClass::Conflict,
        429 => ErrorClass::Timeout,
        503 | 504 => ErrorClass::ConnectionFailed,
        _ => ErrorClass::Internal,
    };
    AppError::new(class, format!("Elasticsearch returned HTTP {status}: {reason}"))
        .with_code(status.to_string())
}

fn auth_header(config: &ConnectionConfig, password: Option<&str>) -> Option<String> {
    let mode = config.es_auth_mode.as_deref().unwrap_or(if config.username.is_empty() { "none" } else { "basic" });
    match mode {
        "apiKey" => password
            .filter(|value| !value.is_empty())
            .map(|key| format!("ApiKey {}", key.trim())),
        "basic" => {
            let password = password.unwrap_or("");
            if config.username.is_empty() && password.is_empty() {
                None
            } else {
                Some(format!("Basic {}", B64.encode(format!("{}:{}", config.username, password))))
            }
        }
        _ => None,
    }
}

fn build_client(config: &ConnectionConfig, timeout: Duration) -> AppResult<Client> {
    let tls = !matches!(config.tls_mode.unwrap_or(TlsMode::Disabled), TlsMode::Disabled);
    if tls
        && config
            .client_cert_path
            .as_deref()
            .is_some_and(|value| !value.trim().is_empty())
    {
        return Err(AppError::unsupported(
            "Elasticsearch client-certificate TLS is not available in this version. Use basic/API-key auth over server TLS, or disable TLS.",
        ));
    }
    let mut builder = Client::builder()
        .timeout(timeout)
        .connect_timeout(Duration::from_millis(config.pool.connection_timeout_ms.max(1000)))
        .pool_max_idle_per_host(config.pool.max_size.max(1) as usize);
    if tls {
        builder = builder.use_rustls_tls();
        if let Some(ca) = config.ca_cert_path.as_deref().filter(|value| !value.trim().is_empty()) {
            let pem = std::fs::read(ca).map_err(|error| {
                AppError::validation(format!("Cannot read Elasticsearch CA certificate '{ca}': {error}"))
            })?;
            let certificate = reqwest::Certificate::from_pem(&pem)
                .map_err(|error| AppError::validation(format!("Invalid Elasticsearch CA certificate: {error}")))?;
            builder = builder.add_root_certificate(certificate);
        }
    }
    builder.build().map_err(AppError::from)
}

pub fn validate_index_name(name: &str) -> AppResult<()> {
    if name.is_empty() {
        return Err(AppError::validation("An Elasticsearch index name must not be empty."));
    }
    if name == "." || name == ".." {
        return Err(AppError::validation("An Elasticsearch index name must not be '.' or '..'."));
    }
    if name.starts_with('-') || name.starts_with('_') || name.starts_with('+') {
        return Err(AppError::validation("An Elasticsearch index name must not start with '-', '_' or '+'."));
    }
    if name.len() > 255 {
        return Err(AppError::validation("An Elasticsearch index name must not exceed 255 bytes."));
    }
    if name.chars().any(|ch| {
        ch.is_whitespace() || matches!(ch, '\\' | '/' | '*' | '?' | '"' | '<' | '>' | '|' | ',' | '#')
    }) {
        return Err(AppError::validation(
            "An Elasticsearch index name must not contain whitespace or \\ / * ? \" < > | , # characters.",
        ));
    }
    Ok(())
}

fn encode_path_segment(segment: &str) -> String {
    let mut out = String::new();
    for byte in segment.as_bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(*byte as char);
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

fn parse_total(hits: &Value) -> (u64, String) {
    match hits.get("total") {
        Some(Value::Object(map)) => (
            map.get("value").and_then(Value::as_u64).unwrap_or(0),
            map.get("relation").and_then(Value::as_str).unwrap_or("eq").to_string(),
        ),
        Some(Value::Number(number)) => (number.as_u64().unwrap_or(0), "eq".to_string()),
        _ => (0, "eq".to_string()),
    }
}

#[async_trait]
impl SearchDriver for EsDriver {
    fn connection_id(&self) -> &str {
        &self.connection_id
    }

    async fn connect(&mut self, config: &ConnectionConfig, password: Option<String>) -> AppResult<()> {
        if config.host.trim().is_empty() {
            return Err(AppError::validation("An Elasticsearch host is required."));
        }
        self.timeout = Duration::from_millis(config.query_timeout_ms.clamp(1_000, 120_000));
        let tls = !matches!(config.tls_mode.unwrap_or(TlsMode::Disabled), TlsMode::Disabled);
        let scheme = if tls { "https" } else { "http" };
        let port = if config.port == 0 { 9200 } else { config.port };
        self.base_url = format!("{scheme}://{}:{port}", config.host.trim());
        let client = build_client(config, self.timeout)?;
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(reqwest::header::ACCEPT, reqwest::header::HeaderValue::from_static("application/json"));
        if let Some(auth) = auth_header(config, password.as_deref()) {
            let value = reqwest::header::HeaderValue::from_str(&auth)
                .map_err(|_| AppError::validation("Elasticsearch credential contains characters invalid in an HTTP header."))?;
            headers.insert(reqwest::header::AUTHORIZATION, value);
        }
        let client = client
            .request(Method::GET, format!("{}/", self.base_url))
            .headers(headers.clone())
            .send()
            .await
            .map_err(|error| AppError::connection(format!("Cannot reach Elasticsearch at {}: {error}", self.base_url)))?;
        // Rebuild a client that carries the auth headers on every request.
        let mut builder = Client::builder()
            .timeout(self.timeout)
            .connect_timeout(Duration::from_millis(config.pool.connection_timeout_ms.max(1000)))
            .default_headers(headers);
        if tls {
            builder = builder.use_rustls_tls();
            if let Some(ca) = config.ca_cert_path.as_deref().filter(|value| !value.trim().is_empty()) {
                let pem = std::fs::read(ca).map_err(|error| AppError::validation(format!("Cannot read CA: {error}")))?;
                let certificate = reqwest::Certificate::from_pem(&pem)
                    .map_err(|error| AppError::validation(format!("Invalid CA certificate: {error}")))?;
                builder = builder.add_root_certificate(certificate);
            }
        }
        let client_with_headers = builder.build().map_err(AppError::from)?;
        let status = client.status();
        let body: Value = client.json().await.unwrap_or(Value::Null);
        if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
            return Err(AppError::new(ErrorClass::PermissionDenied, "Elasticsearch rejected the credentials (HTTP 401/403)."));
        }
        if !status.is_success() {
            return Err(es_status_error(status.as_u16(), &body));
        }
        self.client = Some(client_with_headers);
        Ok(())
    }

    async fn health_check(&self) -> AppResult<()> {
        self.request_json(Method::GET, "/", None).await?;
        Ok(())
    }

    async fn info(&self, operation_id: &str) -> AppResult<EsClusterInfo> {
        self.run_cancellable(operation_id, async {
            let root = self.request_json(Method::GET, "/", None).await?;
            let health = self
                .request_json(Method::GET, "/_cluster/health", None)
                .await
                .unwrap_or(Value::Null);
            Ok(EsClusterInfo {
                cluster_name: root.get("cluster_name").and_then(Value::as_str).map(str::to_string),
                version: root.get("version").and_then(|v| v.get("number")).and_then(Value::as_str).map(str::to_string),
                distribution: root.get("version").and_then(|v| v.get("distribution")).and_then(Value::as_str).map(str::to_string),
                status: health.get("status").and_then(Value::as_str).map(str::to_string),
                number_of_nodes: health.get("number_of_nodes").and_then(Value::as_u64),
                active_shards: health.get("active_shards").and_then(Value::as_u64),
                raw: health,
            })
        })
        .await
    }

    async fn list_indices(&self, operation_id: &str) -> AppResult<Vec<EsIndex>> {
        self.run_cancellable(operation_id, async {
            let cat = self
                .request_json(Method::GET, "/_cat/indices?format=json&bytes=b&h=index,health,status,docs.count,docs.deleted,store.size,pri,rep", None)
                .await?;
            let aliases = self.list_aliases_inner().await.unwrap_or_default();
            let mut indices = Vec::new();
            if let Some(rows) = cat.as_array() {
                for row in rows {
                    let name = row.get("index").and_then(Value::as_str).unwrap_or("").to_string();
                    if name.is_empty() {
                        continue;
                    }
                    let number = |key: &str| row.get(key).and_then(Value::as_str).and_then(|value| value.parse::<u64>().ok());
                    indices.push(EsIndex {
                        aliases: aliases.iter().filter(|alias| alias.index == name).map(|alias| alias.alias.clone()).collect(),
                        name,
                        health: row.get("health").and_then(Value::as_str).map(str::to_string),
                        status: row.get("status").and_then(Value::as_str).map(str::to_string),
                        docs_count: number("docs.count"),
                        docs_deleted: number("docs.deleted"),
                        store_bytes: number("store.size"),
                        primary_shards: row.get("pri").and_then(Value::as_str).and_then(|value| value.parse::<u32>().ok()),
                        replica_shards: row.get("rep").and_then(Value::as_str).and_then(|value| value.parse::<u32>().ok()),
                    });
                }
            }
            indices.sort_by(|a, b| a.name.cmp(&b.name));
            Ok(indices)
        })
        .await
    }

    async fn create_index(&self, name: &str, body: Value, operation_id: &str) -> AppResult<()> {
        validate_index_name(name)?;
        if !body.is_null() && !body.is_object() {
            return Err(AppError::validation("Index settings/mappings must be a JSON object."));
        }
        self.run_cancellable(operation_id, async {
            let payload = if body.is_null() { None } else { Some(&body) };
            self.request_json(Method::PUT, &format!("/{}", encode_path_segment(name)), payload).await?;
            Ok(())
        })
        .await
    }

    async fn delete_index(&self, name: &str, confirmed_dangerous: bool, operation_id: &str) -> AppResult<()> {
        validate_index_name(name)?;
        if !confirmed_dangerous {
            return Err(AppError::validation("Deleting an index requires explicit second-step confirmation."));
        }
        self.run_cancellable(operation_id, async {
            self.request_json(Method::DELETE, &format!("/{}", encode_path_segment(name)), None).await?;
            Ok(())
        })
        .await
    }

    async fn get_mapping(&self, name: &str, operation_id: &str) -> AppResult<Value> {
        validate_index_name(name)?;
        self.run_cancellable(operation_id, async {
            self.request_json(Method::GET, &format!("/{}/_mapping", encode_path_segment(name)), None).await
        })
        .await
    }

    async fn list_aliases(&self, operation_id: &str) -> AppResult<Vec<EsAlias>> {
        self.run_cancellable(operation_id, async { self.list_aliases_inner().await }).await
    }

    async fn modify_aliases(&self, actions: Value, confirmed_dangerous: bool, operation_id: &str) -> AppResult<()> {
        if !confirmed_dangerous {
            return Err(AppError::validation("Alias changes require explicit second-step confirmation."));
        }
        let actions_array = actions.as_array().ok_or_else(|| AppError::validation("Alias actions must be a JSON array."))?;
        if actions_array.is_empty() {
            return Err(AppError::validation("Provide at least one alias action."));
        }
        self.run_cancellable(operation_id, async {
            self.request_json(Method::POST, "/_aliases", Some(&json!({ "actions": actions }))).await?;
            Ok(())
        })
        .await
    }

    async fn search(&self, request: EsSearchRequest) -> AppResult<EsSearchResult> {
        validate_index_name(&request.index)?;
        let started = Instant::now();
        self.run_cancellable(&request.operation_id, async {
            let mut body = if request.body.is_null() { json!({}) } else { request.body.clone() };
            if !body.is_object() {
                return Err(AppError::validation("The Query DSL body must be a JSON object."));
            }
            let object = body.as_object_mut().expect("checked object");
            object.insert("from".into(), json!(request.from));
            object.insert("size".into(), json!(request.size.clamp(1, 10_000)));
            object.entry("track_total_hits").or_insert(json!(true));
            let path = format!("/{}/_search", encode_path_segment(&request.index));
            let response = self.request_json(Method::POST, &path, Some(&body)).await?;
            let hits = response.get("hits").cloned().unwrap_or(Value::Null);
            let (total, total_relation) = parse_total(&hits);
            let mut documents = Vec::new();
            if let Some(items) = hits.get("hits").and_then(Value::as_array) {
                for item in items {
                    documents.push(EsHit {
                        id: item.get("_id").and_then(Value::as_str).unwrap_or("").to_string(),
                        index: item.get("_index").and_then(Value::as_str).unwrap_or(&request.index).to_string(),
                        score: item.get("_score").and_then(Value::as_f64),
                        source: item.get("_source").cloned().unwrap_or(Value::Null),
                    });
                }
            }
            Ok(EsSearchResult {
                took_ms: response.get("took").and_then(Value::as_u64).unwrap_or(0),
                total,
                total_relation,
                hits: documents,
                aggregations: response.get("aggregations").cloned(),
                duration_ms: started.elapsed().as_millis() as u64,
            })
        })
        .await
    }

    async fn get_document(&self, index: &str, id: &str, operation_id: &str) -> AppResult<EsDocument> {
        validate_index_name(index)?;
        if id.is_empty() {
            return Err(AppError::validation("A document id is required."));
        }
        self.run_cancellable(operation_id, async {
            let path = format!("/{}/_doc/{}", encode_path_segment(index), encode_path_segment(id));
            let (status, body) = self.send(Method::GET, &path, None).await?;
            if status == 404 {
                return Ok(EsDocument { index: index.to_string(), id: id.to_string(), found: false, version: None, source: Value::Null });
            }
            if !(200..300).contains(&status) {
                return Err(es_status_error(status, &body));
            }
            Ok(EsDocument {
                index: body.get("_index").and_then(Value::as_str).unwrap_or(index).to_string(),
                id: body.get("_id").and_then(Value::as_str).unwrap_or(id).to_string(),
                found: body.get("found").and_then(Value::as_bool).unwrap_or(true),
                version: body.get("_version").and_then(Value::as_i64),
                source: body.get("_source").cloned().unwrap_or(Value::Null),
            })
        })
        .await
    }

    async fn put_document(&self, index: &str, id: &str, source: Value, operation_id: &str) -> AppResult<EsDocument> {
        validate_index_name(index)?;
        if !source.is_object() {
            return Err(AppError::validation("A document source must be a JSON object."));
        }
        self.run_cancellable(operation_id, async {
            let (method, path) = if id.is_empty() {
                (Method::POST, format!("/{}/_doc", encode_path_segment(index)))
            } else {
                (Method::PUT, format!("/{}/_doc/{}", encode_path_segment(index), encode_path_segment(id)))
            };
            let body = self.request_json(method, &path, Some(&source)).await?;
            Ok(EsDocument {
                index: body.get("_index").and_then(Value::as_str).unwrap_or(index).to_string(),
                id: body.get("_id").and_then(Value::as_str).unwrap_or(id).to_string(),
                found: true,
                version: body.get("_version").and_then(Value::as_i64),
                source,
            })
        })
        .await
    }

    async fn delete_document(&self, index: &str, id: &str, confirmed_dangerous: bool, operation_id: &str) -> AppResult<()> {
        validate_index_name(index)?;
        if id.is_empty() {
            return Err(AppError::validation("A document id is required."));
        }
        if !confirmed_dangerous {
            return Err(AppError::validation("Deleting a document requires explicit second-step confirmation."));
        }
        self.run_cancellable(operation_id, async {
            let path = format!("/{}/_doc/{}", encode_path_segment(index), encode_path_segment(id));
            let (status, body) = self.send(Method::DELETE, &path, None).await?;
            if status == 404 {
                return Ok(());
            }
            if !(200..300).contains(&status) {
                return Err(es_status_error(status, &body));
            }
            Ok(())
        })
        .await
    }

    async fn execute_rest(&self, request: EsRestRequest) -> AppResult<EsRestResult> {
        let method = request.method.trim().to_ascii_uppercase();
        let method = Method::from_bytes(method.as_bytes())
            .map_err(|_| AppError::validation(format!("Unsupported HTTP method '{}'.", request.method)))?;
        if method == Method::DELETE && !request.confirmed_dangerous {
            return Err(AppError::validation("DELETE requests require explicit second-step confirmation."));
        }
        let started = Instant::now();
        self.run_cancellable(&request.operation_id, async {
            let (status, body) = self.send(method, &request.path, request.body.as_ref()).await?;
            Ok(EsRestResult { status, body, duration_ms: started.elapsed().as_millis() as u64 })
        })
        .await
    }

    async fn cancel(&self, operation_id: &str) -> AppResult<()> {
        let token = self
            .cancellations
            .lock()
            .get(operation_id)
            .cloned()
            .ok_or_else(|| AppError::validation(format!("Elasticsearch operation '{operation_id}' is not running.")))?;
        token.cancel();
        Ok(())
    }

    async fn close(&self) -> AppResult<()> {
        for (_, token) in self.cancellations.lock().drain() {
            token.cancel();
        }
        Ok(())
    }
}

impl EsDriver {
    async fn list_aliases_inner(&self) -> AppResult<Vec<EsAlias>> {
        let body = self.request_json(Method::GET, "/_aliases", None).await?;
        let mut aliases = Vec::new();
        if let Some(map) = body.as_object() {
            for (index, entry) in map {
                if let Some(alias_map) = entry.get("aliases").and_then(Value::as_object) {
                    for (alias, detail) in alias_map {
                        aliases.push(EsAlias {
                            alias: alias.clone(),
                            index: index.clone(),
                            is_write_index: detail.get("is_write_index").and_then(Value::as_bool).unwrap_or(false),
                            filter: detail.get("filter").cloned(),
                        });
                    }
                }
            }
        }
        aliases.sort_by(|a, b| a.alias.cmp(&b.alias).then(a.index.cmp(&b.index)));
        Ok(aliases)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(mode: Option<&str>, username: &str) -> ConnectionConfig {
        serde_json::from_value(json!({
            "id": "es-test",
            "name": "ES",
            "protocol": "elasticsearch",
            "host": "localhost",
            "port": 9200,
            "username": username,
            "esAuthMode": mode,
        }))
        .unwrap()
    }

    #[test]
    fn builds_basic_and_api_key_auth_headers() {
        let basic = config(Some("basic"), "elastic");
        assert_eq!(
            auth_header(&basic, Some("secret")),
            Some(format!("Basic {}", B64.encode("elastic:secret")))
        );
        let api = config(Some("apiKey"), "");
        assert_eq!(auth_header(&api, Some("encoded-key")), Some("ApiKey encoded-key".to_string()));
        let none = config(Some("none"), "ignored");
        assert_eq!(auth_header(&none, Some("secret")), None);
        let inferred = config(None, "elastic");
        assert!(auth_header(&inferred, Some("secret")).unwrap().starts_with("Basic "));
    }

    #[test]
    fn validates_index_names() {
        for good in ["logs-2026", "luxbase_test", "metrics.v1"] {
            assert!(validate_index_name(good).is_ok(), "{good}");
        }
        for bad in ["", ".", "..", "-leading", "_leading", "+leading", "has space", "a/b", "a*b", "a?b", "a\"b", "a<b", "a>b", "a|b", "a,b", "a#b", "a\\b"] {
            assert!(validate_index_name(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn encodes_path_segments() {
        assert_eq!(encode_path_segment("simple-id_1.~"), "simple-id_1.~");
        assert_eq!(encode_path_segment("a b/c"), "a%20b%2Fc");
        assert_eq!(encode_path_segment("中文"), "%E4%B8%AD%E6%96%87");
    }

    #[test]
    fn parses_total_for_es7_and_es6() {
        assert_eq!(parse_total(&json!({ "total": { "value": 12, "relation": "gte" } })), (12, "gte".to_string()));
        assert_eq!(parse_total(&json!({ "total": 7 })), (7, "eq".to_string()));
        assert_eq!(parse_total(&json!({})), (0, "eq".to_string()));
    }

    #[test]
    fn maps_http_status_to_error_class() {
        assert_eq!(es_status_error(401, &json!({})).class, ErrorClass::PermissionDenied);
        assert_eq!(es_status_error(403, &json!({})).class, ErrorClass::PermissionDenied);
        assert_eq!(es_status_error(400, &json!({ "error": { "type": "parsing_exception", "reason": "bad" } })).class, ErrorClass::SyntaxError);
        assert_eq!(es_status_error(409, &json!({})).class, ErrorClass::Conflict);
        let message = es_status_error(400, &json!({ "error": { "type": "parsing_exception", "reason": "bad query" } })).message;
        assert!(message.contains("parsing_exception") && message.contains("bad query"));
    }

    #[test]
    fn rejects_unsafe_rest_paths() {
        let driver = EsDriver::new("es".into());
        assert!(driver.url("/index/_search").is_ok());
        assert!(driver.url("index/_search").is_err());
        assert!(driver.url("/../secret").is_err());
        assert!(driver.url("/http://evil").is_err());
    }
}
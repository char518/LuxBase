use super::common::{validate_collection_name, HttpCore};
use super::r#trait::VectorDriver;
use crate::error::{AppError, AppResult};
use crate::models::{
    ConnectionConfig, ErrorClass, VectorCollection, VectorCollectionDetail, VectorCreateRequest,
    VectorDeleteRequest, VectorField, VectorIndex, VectorInfo, VectorQueryRequest, VectorQueryResult,
    VectorRestRequest, VectorRestResult, VectorSearchRequest, VectorSearchResult, VectorHit,
    VectorUpsertRequest, VectorWriteResult,
};
use async_trait::async_trait;
use futures::stream::{self, StreamExt};
use reqwest::Method;
use serde_json::{json, Map, Value};
use std::time::Instant;

const API_ROOT: &str = "/v2/vectordb";

pub struct MilvusDriver {
    connection_id: String,
    http: HttpCore,
    db_name: String,
}

impl MilvusDriver {
    pub fn new(connection_id: String) -> Self {
        Self { connection_id, http: HttpCore::new(), db_name: String::new() }
    }

    /// Base body carrying the optional database selector.
    fn base_body(&self) -> Map<String, Value> {
        let mut map = Map::new();
        if !self.db_name.is_empty() {
            map.insert("dbName".into(), json!(self.db_name));
        }
        map
    }

    fn collection_body(&self, name: &str) -> Value {
        let mut map = self.base_body();
        map.insert("collectionName".into(), json!(name));
        Value::Object(map)
    }

    /// POSTs to a v2 endpoint and unwraps the `{code, data, message}` envelope. Milvus returns
    /// HTTP 200 even for logical failures, so the body `code` is authoritative.
    async fn api(&self, suffix: &str, body: &Value) -> AppResult<Value> {
        let path = format!("{API_ROOT}/{suffix}");
        let (status, value) = self.http.send(Method::POST, &path, Some(body)).await?;
        unwrap_envelope(status, &value)
    }

    async fn stats_inner(&self, name: &str) -> Option<u64> {
        let data = self.api("collections/get_stats", &self.collection_body(name)).await.ok()?;
        data.get("rowCount").and_then(Value::as_u64)
            .or_else(|| data.get("row_count").and_then(Value::as_u64))
    }

    async fn describe_inner(&self, name: &str) -> AppResult<VectorCollectionDetail> {
        let data = self.api("collections/describe", &self.collection_body(name)).await?;
        let mut fields = Vec::new();
        if let Some(items) = data.get("fields").and_then(Value::as_array) {
            for field in items {
                let name = field.get("name").and_then(Value::as_str).unwrap_or("").to_string();
                let data_type = field.get("type").and_then(Value::as_str)
                    .or_else(|| field.get("dataType").and_then(Value::as_str))
                    .unwrap_or("Unknown").to_string();
                let is_vector = data_type.contains("Vector");
                let params = field.get("params").or_else(|| field.get("elementTypeParams"));
                let is_primary = field.get("primaryKey").and_then(Value::as_bool)
                    .or_else(|| field.get("isPrimary").and_then(Value::as_bool))
                    .unwrap_or(false);
                fields.push(VectorField {
                    name,
                    is_primary,
                    is_vector,
                    dim: param_value(params, "dim").and_then(|v| v.as_u64().or_else(|| v.as_str().and_then(|s| s.parse().ok()))),
                    metric: None,
                    description: field.get("description").and_then(Value::as_str).filter(|s| !s.is_empty()).map(str::to_string),
                    data_type,
                });
            }
        }
        let mut indexes = Vec::new();
        if let Some(items) = data.get("indexes").and_then(Value::as_array) {
            for index in items {
                let field = index.get("fieldName").and_then(Value::as_str).unwrap_or("").to_string();
                let metric = index.get("metricType").and_then(Value::as_str).map(str::to_string);
                if let Some(target) = fields.iter_mut().find(|f| f.name == field) {
                    target.metric = metric.clone();
                }
                indexes.push(VectorIndex {
                    name: index.get("indexName").and_then(Value::as_str).unwrap_or(&field).to_string(),
                    field,
                    index_type: index.get("indexType").and_then(Value::as_str).map(str::to_string),
                    metric,
                    params: index.clone(),
                });
            }
        }
        let load = data.get("load").and_then(Value::as_str)
            .or_else(|| data.get("loadState").and_then(Value::as_str))
            .unwrap_or("");
        let loaded = if load.is_empty() { None } else { Some(load.contains("Loaded") && !load.contains("NotLoad")) };
        Ok(VectorCollectionDetail {
            name: name.to_string(),
            row_count: self.stats_inner(name).await,
            loaded,
            fields,
            indexes,
            raw: data,
        })
    }
}

/// Milvus field params arrive either as an object `{"dim": 8}` or a list of `{key, value}` pairs
/// depending on server version; read a key from either shape.
fn param_value(params: Option<&Value>, key: &str) -> Option<Value> {
    match params? {
        Value::Object(map) => map.get(key).cloned(),
        Value::Array(items) => items.iter().find_map(|item| {
            let k = item.get("key").and_then(Value::as_str)?;
            if k == key { item.get("value").cloned() } else { None }
        }),
        _ => None,
    }
}

fn unwrap_envelope(status: u16, value: &Value) -> AppResult<Value> {
    if let Some(code) = value.get("code").and_then(Value::as_i64) {
        if code == 0 {
            return Ok(value.get("data").cloned().unwrap_or(Value::Null));
        }
        let message = value.get("message").and_then(Value::as_str).unwrap_or("Milvus request failed");
        return Err(milvus_error(code, message));
    }
    if (200..300).contains(&status) {
        Ok(value.clone())
    } else {
        Err(milvus_error(status as i64, &value.to_string()))
    }
}

fn milvus_error(code: i64, message: &str) -> AppError {
    let lower = message.to_ascii_lowercase();
    let class = if lower.contains("unauth") || lower.contains("permission") || lower.contains("auth") || lower.contains("token") {
        ErrorClass::PermissionDenied
    } else if lower.contains("timeout") || lower.contains("deadline") {
        ErrorClass::Timeout
    } else if lower.contains("not found") || lower.contains("doesn't exist") || lower.contains("does not exist") || lower.contains("not exist") {
        ErrorClass::Validation
    } else if lower.contains("already exist") {
        ErrorClass::Conflict
    } else {
        ErrorClass::Validation
    };
    AppError::new(class, format!("Milvus error {code}: {message}")).with_code(code.to_string())
}

/// Milvus filters are boolean expression strings; accept a string, treat null/empty as "no filter".
fn filter_expr(value: &Value) -> AppResult<Option<String>> {
    match value {
        Value::Null => Ok(None),
        Value::String(text) if text.trim().is_empty() => Ok(None),
        Value::String(text) => Ok(Some(text.clone())),
        _ => Err(AppError::validation("A Milvus filter must be a boolean expression string, e.g. `id > 0`.")),
    }
}

fn output_fields(fields: &[String]) -> Value {
    if fields.is_empty() { json!(["*"]) } else { json!(fields) }
}

fn auth_header(config: &ConnectionConfig, password: Option<&str>) -> AppResult<Option<String>> {
    let password = password.unwrap_or("");
    let mode = config.vector_auth_mode.as_deref().unwrap_or(if !config.username.is_empty() {
        "password"
    } else if !password.is_empty() {
        "apiKey"
    } else {
        "none"
    });
    match mode {
        "apiKey" => {
            if password.is_empty() {
                Ok(None)
            } else {
                Ok(Some(format!("Bearer {}", password.trim())))
            }
        }
        "password" => {
            let user = if config.username.trim().is_empty() { "root" } else { config.username.trim() };
            if password.is_empty() && config.username.trim().is_empty() {
                Ok(None)
            } else {
                Ok(Some(format!("Bearer {user}:{password}")))
            }
        }
        _ => Ok(None),
    }
}

#[async_trait]
impl VectorDriver for MilvusDriver {
    fn connection_id(&self) -> &str {
        &self.connection_id
    }

    async fn connect(&mut self, config: &ConnectionConfig, password: Option<String>) -> AppResult<()> {
        self.db_name = config.database.trim().to_string();
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(reqwest::header::ACCEPT, reqwest::header::HeaderValue::from_static("application/json"));
        if let Some(auth) = auth_header(config, password.as_deref())? {
            let value = reqwest::header::HeaderValue::from_str(&auth)
                .map_err(|_| AppError::validation("Milvus credential contains characters invalid in an HTTP header."))?;
            headers.insert(reqwest::header::AUTHORIZATION, value);
        }
        self.http.configure(config, 19530, headers, "Milvus")?;
        // Probe reachability + auth with a lightweight list call.
        self.http
            .run_cancellable("connect", async {
                self.api("collections/list", &Value::Object(self.base_body())).await?;
                Ok(())
            })
            .await
            .map_err(|error| match error.class {
                ErrorClass::ConnectionFailed | ErrorClass::Internal => {
                    AppError::connection(format!("Cannot reach Milvus at {}: {}", self.http.base_url(), error.message))
                }
                _ => error,
            })
    }

    async fn health_check(&self) -> AppResult<()> {
        self.api("collections/list", &Value::Object(self.base_body())).await?;
        Ok(())
    }

    async fn info(&self, operation_id: &str) -> AppResult<VectorInfo> {
        self.http.run_cancellable(operation_id, async {
            let data = self.api("collections/list", &Value::Object(self.base_body())).await?;
            let count = data.as_array().map(|a| a.len() as u64);
            Ok(VectorInfo {
                product: "Milvus".into(),
                version: None,
                collection_count: count,
                raw: json!({ "database": if self.db_name.is_empty() { "default" } else { &self.db_name }, "collections": count }),
            })
        }).await
    }

    async fn list_collections(&self, operation_id: &str) -> AppResult<Vec<VectorCollection>> {
        self.http.run_cancellable(operation_id, async {
            let data = self.api("collections/list", &Value::Object(self.base_body())).await?;
            let names: Vec<String> = data.as_array().map(|items| {
                items.iter().filter_map(|item| item.as_str().map(str::to_string)).collect()
            }).unwrap_or_default();
            let mut collections: Vec<VectorCollection> = stream::iter(names.into_iter().take(200))
                .map(|name| async move {
                    let detail = self.describe_inner(&name).await.ok();
                    let (status, vector_fields, description, row_count) = match &detail {
                        Some(d) => (
                            d.loaded.map(|loaded| if loaded { "Loaded".to_string() } else { "NotLoad".to_string() }),
                            d.fields.iter().filter(|f| f.is_vector).map(|f| f.name.clone()).collect::<Vec<String>>(),
                            d.raw.get("description").and_then(Value::as_str).filter(|s| !s.is_empty()).map(str::to_string),
                            d.row_count,
                        ),
                        None => (None, Vec::new(), None, None),
                    };
                    VectorCollection { name, row_count, status, vector_fields, description }
                })
                .buffer_unordered(8)
                .collect()
                .await;
            collections.sort_by(|a, b| a.name.cmp(&b.name));
            Ok(collections)
        }).await
    }

    async fn describe_collection(&self, name: &str, operation_id: &str) -> AppResult<VectorCollectionDetail> {
        validate_collection_name(name)?;
        self.http.run_cancellable(operation_id, async { self.describe_inner(name).await }).await
    }

    async fn create_collection(&self, request: VectorCreateRequest) -> AppResult<()> {
        validate_collection_name(&request.name)?;
        if !request.body.is_object() {
            return Err(AppError::validation("Provide a Milvus create body: either {\"schema\": {...}, \"indexParams\": [...]} or a quick-create {\"dimension\": 8, \"metricType\": \"L2\"}."));
        }
        self.http.run_cancellable(&request.operation_id, async {
            let mut map = request.body.as_object().unwrap().clone();
            map.insert("collectionName".into(), json!(request.name));
            if !self.db_name.is_empty() {
                map.entry("dbName").or_insert(json!(self.db_name));
            }
            self.api("collections/create", &Value::Object(map)).await?;
            Ok(())
        }).await
    }

    async fn drop_collection(&self, name: &str, confirmed_dangerous: bool, operation_id: &str) -> AppResult<()> {
        validate_collection_name(name)?;
        if !confirmed_dangerous {
            return Err(AppError::validation("Dropping a collection requires explicit second-step confirmation."));
        }
        self.http.run_cancellable(operation_id, async {
            self.api("collections/drop", &self.collection_body(name)).await?;
            Ok(())
        }).await
    }

    async fn search(&self, request: VectorSearchRequest) -> AppResult<VectorSearchResult> {
        validate_collection_name(&request.collection)?;
        if request.vector.is_empty() {
            return Err(AppError::validation("A query vector is required for vector search."));
        }
        let started = Instant::now();
        self.http.run_cancellable(&request.operation_id, async {
            let mut map = self.base_body();
            map.insert("collectionName".into(), json!(request.collection));
            map.insert("data".into(), json!([request.vector]));
            map.insert("limit".into(), json!(request.top_k.clamp(1, 16_384)));
            map.insert("outputFields".into(), output_fields(&request.output_fields));
            if let Some(field) = request.vector_field.as_deref().filter(|f| !f.trim().is_empty()) {
                map.insert("annsField".into(), json!(field));
            }
            if let Some(filter) = filter_expr(&request.filter)? {
                map.insert("filter".into(), json!(filter));
            }
            if let Some(extra) = request.params.as_object() {
                for (key, value) in extra {
                    map.insert(key.clone(), value.clone());
                }
            }
            let data = self.api("entities/search", &Value::Object(map)).await?;
            let mut hits = Vec::new();
            if let Some(items) = data.as_array() {
                for item in items {
                    let score = item.get("distance").and_then(Value::as_f64);
                    let id = item.get("id").cloned().unwrap_or(Value::Null);
                    let mut fields = item.clone();
                    if let Some(obj) = fields.as_object_mut() {
                        obj.remove("distance");
                    }
                    hits.push(VectorHit { id, score, fields });
                }
            }
            Ok(VectorSearchResult { hits, duration_ms: started.elapsed().as_millis() as u64 })
        }).await
    }

    async fn query(&self, request: VectorQueryRequest) -> AppResult<VectorQueryResult> {
        validate_collection_name(&request.collection)?;
        let started = Instant::now();
        self.http.run_cancellable(&request.operation_id, async {
            let mut map = self.base_body();
            map.insert("collectionName".into(), json!(request.collection));
            map.insert("filter".into(), json!(filter_expr(&request.filter)?.unwrap_or_default()));
            map.insert("outputFields".into(), output_fields(&request.output_fields));
            map.insert("limit".into(), json!(request.limit.clamp(1, 16_384)));
            map.insert("offset".into(), json!(request.offset));
            let data = self.api("entities/query", &Value::Object(map)).await?;
            let rows = data.as_array().cloned().unwrap_or_default();
            Ok(VectorQueryResult { rows, next_offset: None, duration_ms: started.elapsed().as_millis() as u64 })
        }).await
    }

    async fn get(&self, collection: &str, ids: Vec<Value>, operation_id: &str) -> AppResult<Vec<Value>> {
        validate_collection_name(collection)?;
        if ids.is_empty() {
            return Err(AppError::validation("Provide at least one id to fetch."));
        }
        self.http.run_cancellable(operation_id, async {
            let mut map = self.base_body();
            map.insert("collectionName".into(), json!(collection));
            map.insert("id".into(), json!(ids));
            map.insert("outputFields".into(), json!(["*"]));
            let data = self.api("entities/get", &Value::Object(map)).await?;
            Ok(data.as_array().cloned().unwrap_or_default())
        }).await
    }

    async fn upsert(&self, request: VectorUpsertRequest) -> AppResult<VectorWriteResult> {
        validate_collection_name(&request.collection)?;
        if request.rows.is_empty() {
            return Err(AppError::validation("Provide at least one entity to write."));
        }
        if request.rows.iter().any(|row| !row.is_object()) {
            return Err(AppError::validation("Each Milvus entity must be a JSON object of field/value pairs."));
        }
        let started = Instant::now();
        self.http.run_cancellable(&request.operation_id, async {
            let mut map = self.base_body();
            map.insert("collectionName".into(), json!(request.collection));
            map.insert("data".into(), json!(request.rows));
            let data = self.api("entities/upsert", &Value::Object(map)).await?;
            let affected = data.get("upsertCount").and_then(Value::as_u64).unwrap_or(request.rows.len() as u64);
            let ids = data.get("upsertIds").and_then(Value::as_array).cloned().unwrap_or_default();
            Ok(VectorWriteResult { affected, ids, duration_ms: started.elapsed().as_millis() as u64, raw: data })
        }).await
    }

    async fn delete(&self, request: VectorDeleteRequest, confirmed_dangerous: bool) -> AppResult<VectorWriteResult> {
        validate_collection_name(&request.collection)?;
        if !confirmed_dangerous {
            return Err(AppError::validation("Deleting entities requires explicit second-step confirmation."));
        }
        let by_ids = !request.ids.is_empty();
        let filter = filter_expr(&request.filter)?;
        if !by_ids && filter.is_none() {
            return Err(AppError::validation("Delete by explicit ids or a filter expression."));
        }
        let started = Instant::now();
        self.http.run_cancellable(&request.operation_id, async {
            let mut map = self.base_body();
            map.insert("collectionName".into(), json!(request.collection));
            if by_ids {
                map.insert("id".into(), json!(request.ids));
            } else if let Some(filter) = filter {
                map.insert("filter".into(), json!(filter));
            }
            let data = self.api("entities/delete", &Value::Object(map)).await?;
            let affected = data.get("deleteCount").and_then(Value::as_u64).unwrap_or(0);
            Ok(VectorWriteResult { affected, ids: Vec::new(), duration_ms: started.elapsed().as_millis() as u64, raw: data })
        }).await
    }

    async fn execute_rest(&self, request: VectorRestRequest) -> AppResult<VectorRestResult> {
        let method = request.method.trim().to_ascii_uppercase();
        let method = Method::from_bytes(method.as_bytes())
            .map_err(|_| AppError::validation(format!("Unsupported HTTP method '{}'.", request.method)))?;
        if method == Method::DELETE && !request.confirmed_dangerous {
            return Err(AppError::validation("DELETE requests require explicit second-step confirmation."));
        }
        let started = Instant::now();
        self.http.run_cancellable(&request.operation_id, async {
            let (status, body) = self.http.send(method, &request.path, request.body.as_ref()).await?;
            Ok(VectorRestResult { status, body, duration_ms: started.elapsed().as_millis() as u64 })
        }).await
    }

    async fn cancel(&self, operation_id: &str) -> AppResult<()> {
        self.http.cancel(operation_id)
    }

    async fn close(&self) -> AppResult<()> {
        self.http.close();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(mode: Option<&str>, username: &str) -> ConnectionConfig {
        serde_json::from_value(json!({
            "id": "mv-test", "name": "Milvus", "protocol": "milvus",
            "host": "localhost", "port": 19530, "username": username, "vectorAuthMode": mode,
        })).unwrap()
    }

    #[test]
    fn builds_token_auth_headers() {
        assert_eq!(auth_header(&config(Some("password"), "root"), Some("Milvus")).unwrap(), Some("Bearer root:Milvus".into()));
        assert_eq!(auth_header(&config(Some("apiKey"), ""), Some("zilliz-token")).unwrap(), Some("Bearer zilliz-token".into()));
        assert_eq!(auth_header(&config(Some("none"), "root"), Some("x")).unwrap(), None);
        assert_eq!(auth_header(&config(None, "root"), Some("pw")).unwrap(), Some("Bearer root:pw".into()));
        assert_eq!(auth_header(&config(None, ""), Some("")).unwrap(), None);
    }

    #[test]
    fn unwraps_code_envelope() {
        assert_eq!(unwrap_envelope(200, &json!({"code": 0, "data": ["a"]})).unwrap(), json!(["a"]));
        let err = unwrap_envelope(200, &json!({"code": 1100, "message": "collection not found"})).unwrap_err();
        assert_eq!(err.class, ErrorClass::Validation);
        assert!(err.message.contains("1100"));
    }

    #[test]
    fn reads_params_from_object_or_pairs() {
        assert_eq!(param_value(Some(&json!({"dim": 8})), "dim"), Some(json!(8)));
        assert_eq!(param_value(Some(&json!([{"key": "dim", "value": "8"}])), "dim"), Some(json!("8")));
        assert_eq!(param_value(Some(&json!({})), "dim"), None);
    }

    #[test]
    fn filter_must_be_string() {
        assert_eq!(filter_expr(&json!("id > 0")).unwrap(), Some("id > 0".into()));
        assert_eq!(filter_expr(&Value::Null).unwrap(), None);
        assert_eq!(filter_expr(&json!("")).unwrap(), None);
        assert!(filter_expr(&json!({"must": []})).is_err());
    }
}
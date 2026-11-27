use super::common::{encode_path_segment, validate_collection_name, HttpCore};
use super::r#trait::VectorDriver;
use crate::error::{AppError, AppResult};
use crate::models::{
    ConnectionConfig, ErrorClass, VectorCollection, VectorCollectionDetail, VectorCreateRequest,
    VectorDeleteRequest, VectorField, VectorIndex, VectorInfo, VectorQueryRequest, VectorQueryResult,
    VectorRestRequest, VectorRestResult, VectorSearchRequest, VectorSearchResult, VectorHit,
    VectorUpsertRequest, VectorWriteResult,
};
use async_trait::async_trait;
use reqwest::Method;
use serde_json::{json, Map, Value};
use std::time::Instant;

pub struct QdrantDriver {
    connection_id: String,
    http: HttpCore,
}

impl QdrantDriver {
    pub fn new(connection_id: String) -> Self {
        Self { connection_id, http: HttpCore::new() }
    }

    fn collection_path(&self, name: &str, suffix: &str) -> String {
        format!("/collections/{}{}", encode_path_segment(name), suffix)
    }

    /// Sends a request and unwraps Qdrant's `{result, status, time}` envelope.
    async fn api(&self, method: Method, path: &str, body: Option<&Value>) -> AppResult<Value> {
        let (status, value) = self.http.send(method, path, body).await?;
        if (200..300).contains(&status) {
            Ok(value.get("result").cloned().unwrap_or(value))
        } else {
            Err(qdrant_error(status, &value))
        }
    }

    async fn detail_inner(&self, name: &str) -> AppResult<VectorCollectionDetail> {
        let result = self.api(Method::GET, &self.collection_path(name, ""), None).await?;
        let vectors = result.pointer("/config/params/vectors").cloned().unwrap_or(Value::Null);
        let mut fields = Vec::new();
        let mut indexes = Vec::new();
        match &vectors {
            Value::Object(map) if map.contains_key("size") => {
                // Single unnamed vector.
                let dim = map.get("size").and_then(Value::as_u64);
                let metric = map.get("distance").and_then(Value::as_str).map(str::to_string);
                fields.push(VectorField {
                    name: String::new(), data_type: "DenseVector".into(), is_primary: false,
                    is_vector: true, dim, metric: metric.clone(), description: Some("default vector".into()),
                });
                indexes.push(VectorIndex { name: "default".into(), field: String::new(), index_type: Some("HNSW".into()), metric, params: vectors.clone() });
            }
            Value::Object(map) => {
                for (vname, config) in map {
                    let dim = config.get("size").and_then(Value::as_u64);
                    let metric = config.get("distance").and_then(Value::as_str).map(str::to_string);
                    fields.push(VectorField {
                        name: vname.clone(), data_type: "DenseVector".into(), is_primary: false,
                        is_vector: true, dim, metric: metric.clone(), description: None,
                    });
                    indexes.push(VectorIndex { name: vname.clone(), field: vname.clone(), index_type: Some("HNSW".into()), metric, params: config.clone() });
                }
            }
            _ => {}
        }
        if let Some(schema) = result.get("payload_schema").and_then(Value::as_object) {
            for (field, info) in schema {
                let data_type = info.get("data_type").and_then(Value::as_str).unwrap_or("payload").to_string();
                fields.push(VectorField {
                    name: field.clone(), data_type: data_type.clone(), is_primary: false,
                    is_vector: false, dim: None, metric: None, description: None,
                });
                indexes.push(VectorIndex { name: field.clone(), field: field.clone(), index_type: Some(data_type), metric: None, params: info.clone() });
            }
        }
        Ok(VectorCollectionDetail {
            name: name.to_string(),
            row_count: result.get("points_count").and_then(Value::as_u64),
            loaded: None,
            fields,
            indexes,
            raw: result,
        })
    }
}

fn qdrant_error(status: u16, body: &Value) -> AppError {
    let message = body
        .pointer("/status/error")
        .and_then(Value::as_str)
        .map(str::to_string)
        .unwrap_or_else(|| body.to_string());
    let class = match status {
        401 | 403 => ErrorClass::PermissionDenied,
        404 => ErrorClass::Validation,
        400 | 422 => ErrorClass::SyntaxError,
        409 => ErrorClass::Conflict,
        429 => ErrorClass::Timeout,
        503 | 504 => ErrorClass::ConnectionFailed,
        _ => ErrorClass::Internal,
    };
    AppError::new(class, format!("Qdrant returned HTTP {status}: {message}")).with_code(status.to_string())
}

/// Qdrant filters are JSON objects (`{ "must": [...] }`); null / empty object means "no filter".
fn filter_obj(value: &Value) -> AppResult<Option<Value>> {
    match value {
        Value::Null => Ok(None),
        Value::Object(map) if map.is_empty() => Ok(None),
        Value::Object(_) => Ok(Some(value.clone())),
        _ => Err(AppError::validation("A Qdrant filter must be a JSON object, e.g. {\"must\":[{\"key\":\"city\",\"match\":{\"value\":\"Berlin\"}}]}.")),
    }
}

fn auth_header(config: &ConnectionConfig, password: Option<&str>) -> Option<String> {
    let mode = config.vector_auth_mode.as_deref().unwrap_or("apiKey");
    match mode {
        "none" => None,
        _ => password.filter(|value| !value.is_empty()).map(|key| key.trim().to_string()),
    }
}

#[async_trait]
impl VectorDriver for QdrantDriver {
    fn connection_id(&self) -> &str {
        &self.connection_id
    }

    async fn connect(&mut self, config: &ConnectionConfig, password: Option<String>) -> AppResult<()> {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(reqwest::header::ACCEPT, reqwest::header::HeaderValue::from_static("application/json"));
        if let Some(key) = auth_header(config, password.as_deref()) {
            let value = reqwest::header::HeaderValue::from_str(&key)
                .map_err(|_| AppError::validation("Qdrant api-key contains characters invalid in an HTTP header."))?;
            headers.insert("api-key", value);
        }
        self.http.configure(config, 6333, headers, "Qdrant")?;
        self.http
            .run_cancellable("connect", async {
                self.api(Method::GET, "/collections", None).await?;
                Ok(())
            })
            .await
            .map_err(|error| match error.class {
                ErrorClass::ConnectionFailed | ErrorClass::Internal => {
                    AppError::connection(format!("Cannot reach Qdrant at {}: {}", self.http.base_url(), error.message))
                }
                _ => error,
            })
    }

    async fn health_check(&self) -> AppResult<()> {
        self.api(Method::GET, "/collections", None).await?;
        Ok(())
    }

    async fn info(&self, operation_id: &str) -> AppResult<VectorInfo> {
        self.http.run_cancellable(operation_id, async {
            let (_, root) = self.http.send(Method::GET, "/", None).await?;
            let version = root.get("version").and_then(Value::as_str).map(str::to_string);
            let collections = self.api(Method::GET, "/collections", None).await.unwrap_or(Value::Null);
            let count = collections.get("collections").and_then(Value::as_array).map(|a| a.len() as u64);
            Ok(VectorInfo { product: "Qdrant".into(), version, collection_count: count, raw: root })
        }).await
    }

    async fn list_collections(&self, operation_id: &str) -> AppResult<Vec<VectorCollection>> {
        self.http.run_cancellable(operation_id, async {
            let result = self.api(Method::GET, "/collections", None).await?;
            let names: Vec<String> = result.get("collections").and_then(Value::as_array).map(|items| {
                items.iter().filter_map(|item| item.get("name").and_then(Value::as_str).map(str::to_string)).collect()
            }).unwrap_or_default();
            let mut collections = Vec::new();
            for name in names.iter().take(200) {
                let detail = self.detail_inner(name).await.ok();
                let (status, vector_fields, row_count) = match &detail {
                    Some(d) => (
                        d.raw.get("status").and_then(Value::as_str).map(str::to_string),
                        d.fields.iter().filter(|f| f.is_vector).map(|f| if f.name.is_empty() { "<default>".to_string() } else { f.name.clone() }).collect(),
                        d.row_count,
                    ),
                    None => (None, Vec::new(), None),
                };
                collections.push(VectorCollection { name: name.clone(), row_count, status, vector_fields, description: None });
            }
            collections.sort_by(|a, b| a.name.cmp(&b.name));
            Ok(collections)
        }).await
    }

    async fn describe_collection(&self, name: &str, operation_id: &str) -> AppResult<VectorCollectionDetail> {
        validate_collection_name(name)?;
        self.http.run_cancellable(operation_id, async { self.detail_inner(name).await }).await
    }

    async fn create_collection(&self, request: VectorCreateRequest) -> AppResult<()> {
        validate_collection_name(&request.name)?;
        if !request.body.is_object() || request.body.get("vectors").is_none() {
            return Err(AppError::validation("Provide a Qdrant create body with a `vectors` config, e.g. {\"vectors\":{\"size\":4,\"distance\":\"Cosine\"}}."));
        }
        self.http.run_cancellable(&request.operation_id, async {
            self.api(Method::PUT, &self.collection_path(&request.name, ""), Some(&request.body)).await?;
            Ok(())
        }).await
    }

    async fn drop_collection(&self, name: &str, confirmed_dangerous: bool, operation_id: &str) -> AppResult<()> {
        validate_collection_name(name)?;
        if !confirmed_dangerous {
            return Err(AppError::validation("Dropping a collection requires explicit second-step confirmation."));
        }
        self.http.run_cancellable(operation_id, async {
            self.api(Method::DELETE, &self.collection_path(name, ""), None).await?;
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
            let vector = match request.vector_field.as_deref().filter(|f| !f.trim().is_empty()) {
                Some(field) => json!({ "name": field, "vector": request.vector }),
                None => json!(request.vector),
            };
            let mut body = Map::new();
            body.insert("vector".into(), vector);
            body.insert("limit".into(), json!(request.top_k.clamp(1, 16_384)));
            body.insert("with_payload".into(), json!(true));
            body.insert("with_vector".into(), json!(false));
            if let Some(filter) = filter_obj(&request.filter)? {
                body.insert("filter".into(), filter);
            }
            if let Some(extra) = request.params.as_object() {
                for (key, value) in extra {
                    body.insert(key.clone(), value.clone());
                }
            }
            let result = self.api(Method::POST, &self.collection_path(&request.collection, "/points/search"), Some(&Value::Object(body))).await?;
            let mut hits = Vec::new();
            if let Some(items) = result.as_array() {
                for item in items {
                    let id = item.get("id").cloned().unwrap_or(Value::Null);
                    let score = item.get("score").and_then(Value::as_f64);
                    let mut fields = item.clone();
                    if let Some(obj) = fields.as_object_mut() {
                        obj.remove("score");
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
            let mut body = Map::new();
            body.insert("limit".into(), json!(request.limit.clamp(1, 16_384)));
            body.insert("with_payload".into(), json!(true));
            body.insert("with_vector".into(), json!(request.with_vector));
            if let Some(token) = request.page_token.as_ref().filter(|v| !v.is_null()) {
                body.insert("offset".into(), token.clone());
            }
            if let Some(filter) = filter_obj(&request.filter)? {
                body.insert("filter".into(), filter);
            }
            let result = self.api(Method::POST, &self.collection_path(&request.collection, "/points/scroll"), Some(&Value::Object(body))).await?;
            let rows = result.get("points").and_then(Value::as_array).cloned().unwrap_or_default();
            let next_offset = result.get("next_page_offset").cloned().filter(|v| !v.is_null());
            Ok(VectorQueryResult { rows, next_offset, duration_ms: started.elapsed().as_millis() as u64 })
        }).await
    }

    async fn get(&self, collection: &str, ids: Vec<Value>, operation_id: &str) -> AppResult<Vec<Value>> {
        validate_collection_name(collection)?;
        if ids.is_empty() {
            return Err(AppError::validation("Provide at least one id to fetch."));
        }
        self.http.run_cancellable(operation_id, async {
            let body = json!({ "ids": ids, "with_payload": true, "with_vector": true });
            let result = self.api(Method::POST, &self.collection_path(collection, "/points"), Some(&body)).await?;
            Ok(result.as_array().cloned().unwrap_or_default())
        }).await
    }

    async fn upsert(&self, request: VectorUpsertRequest) -> AppResult<VectorWriteResult> {
        validate_collection_name(&request.collection)?;
        if request.rows.is_empty() {
            return Err(AppError::validation("Provide at least one point to write."));
        }
        for row in &request.rows {
            if !row.is_object() || row.get("id").is_none() || row.get("vector").is_none() {
                return Err(AppError::validation("Each Qdrant point must be an object with `id`, `vector` and optional `payload`."));
            }
        }
        let started = Instant::now();
        self.http.run_cancellable(&request.operation_id, async {
            let body = json!({ "points": request.rows });
            let path = self.collection_path(&request.collection, "/points?wait=true");
            let result = self.api(Method::PUT, &path, Some(&body)).await?;
            let ids = request.rows.iter().filter_map(|row| row.get("id").cloned()).collect();
            Ok(VectorWriteResult { affected: request.rows.len() as u64, ids, duration_ms: started.elapsed().as_millis() as u64, raw: result })
        }).await
    }

    async fn delete(&self, request: VectorDeleteRequest, confirmed_dangerous: bool) -> AppResult<VectorWriteResult> {
        validate_collection_name(&request.collection)?;
        if !confirmed_dangerous {
            return Err(AppError::validation("Deleting points requires explicit second-step confirmation."));
        }
        let by_ids = !request.ids.is_empty();
        let filter = filter_obj(&request.filter)?;
        if !by_ids && filter.is_none() {
            return Err(AppError::validation("Delete by explicit point ids or a filter object."));
        }
        let started = Instant::now();
        self.http.run_cancellable(&request.operation_id, async {
            let body = if by_ids { json!({ "points": request.ids }) } else { json!({ "filter": filter }) };
            let path = self.collection_path(&request.collection, "/points/delete?wait=true");
            let result = self.api(Method::POST, &path, Some(&body)).await?;
            let affected = if by_ids { request.ids.len() as u64 } else { 0 };
            Ok(VectorWriteResult { affected, ids: Vec::new(), duration_ms: started.elapsed().as_millis() as u64, raw: result })
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

    fn config(mode: Option<&str>) -> ConnectionConfig {
        serde_json::from_value(json!({
            "id": "qd-test", "name": "Qdrant", "protocol": "qdrant",
            "host": "localhost", "port": 6333, "vectorAuthMode": mode,
        })).unwrap()
    }

    #[test]
    fn api_key_header() {
        assert_eq!(auth_header(&config(Some("apiKey")), Some("secret")), Some("secret".into()));
        assert_eq!(auth_header(&config(None), Some("secret")), Some("secret".into()));
        assert_eq!(auth_header(&config(Some("none")), Some("secret")), None);
        assert_eq!(auth_header(&config(None), None), None);
    }

    #[test]
    fn filter_must_be_object() {
        assert!(filter_obj(&json!({"must": []})).unwrap().is_some());
        assert!(filter_obj(&Value::Null).unwrap().is_none());
        assert!(filter_obj(&json!({})).unwrap().is_none());
        assert!(filter_obj(&json!("city == Berlin")).is_err());
    }

    #[test]
    fn maps_http_status() {
        assert_eq!(qdrant_error(403, &json!({})).class, ErrorClass::PermissionDenied);
        assert_eq!(qdrant_error(404, &json!({})).class, ErrorClass::Validation);
        let err = qdrant_error(400, &json!({"status": {"error": "Wrong input: dim mismatch"}}));
        assert_eq!(err.class, ErrorClass::SyntaxError);
        assert!(err.message.contains("dim mismatch"));
    }

    #[test]
    fn encodes_collection_path() {
        let driver = QdrantDriver::new("qd".into());
        assert_eq!(driver.collection_path("my col", "/points/search"), "/collections/my%20col/points/search");
    }
}
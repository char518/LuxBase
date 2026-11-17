use crate::error::AppResult;
use crate::models::{
    ConnectionConfig, EsAlias, EsClusterInfo, EsDocument, EsIndex, EsRestRequest, EsRestResult,
    EsSearchRequest, EsSearchResult,
};
use async_trait::async_trait;
use serde_json::Value;

#[async_trait]
pub trait SearchDriver: Send + Sync {
    fn connection_id(&self) -> &str;
    async fn connect(&mut self, config: &ConnectionConfig, password: Option<String>) -> AppResult<()>;
    async fn health_check(&self) -> AppResult<()>;
    async fn info(&self, operation_id: &str) -> AppResult<EsClusterInfo>;
    async fn list_indices(&self, operation_id: &str) -> AppResult<Vec<EsIndex>>;
    async fn create_index(&self, name: &str, body: Value, operation_id: &str) -> AppResult<()>;
    async fn delete_index(&self, name: &str, confirmed_dangerous: bool, operation_id: &str) -> AppResult<()>;
    async fn get_mapping(&self, name: &str, operation_id: &str) -> AppResult<Value>;
    async fn list_aliases(&self, operation_id: &str) -> AppResult<Vec<EsAlias>>;
    async fn modify_aliases(&self, actions: Value, confirmed_dangerous: bool, operation_id: &str) -> AppResult<()>;
    async fn search(&self, request: EsSearchRequest) -> AppResult<EsSearchResult>;
    async fn get_document(&self, index: &str, id: &str, operation_id: &str) -> AppResult<EsDocument>;
    async fn put_document(&self, index: &str, id: &str, source: Value, operation_id: &str) -> AppResult<EsDocument>;
    async fn delete_document(&self, index: &str, id: &str, confirmed_dangerous: bool, operation_id: &str) -> AppResult<()>;
    async fn execute_rest(&self, request: EsRestRequest) -> AppResult<EsRestResult>;
    async fn cancel(&self, operation_id: &str) -> AppResult<()>;
    async fn close(&self) -> AppResult<()>;
}
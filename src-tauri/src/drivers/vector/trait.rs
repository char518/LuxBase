use crate::error::AppResult;
use crate::models::{
    ConnectionConfig, VectorCollection, VectorCollectionDetail, VectorCreateRequest,
    VectorDeleteRequest, VectorInfo, VectorQueryRequest, VectorQueryResult, VectorRestRequest,
    VectorRestResult, VectorSearchRequest, VectorSearchResult, VectorUpsertRequest,
    VectorWriteResult,
};
use async_trait::async_trait;

/// One interface over the vector stores (Milvus, Qdrant). Each driver translates these
/// unified requests into its native REST API so the workspace UI stays protocol-agnostic.
#[async_trait]
pub trait VectorDriver: Send + Sync {
    fn connection_id(&self) -> &str;
    async fn connect(&mut self, config: &ConnectionConfig, password: Option<String>) -> AppResult<()>;
    async fn health_check(&self) -> AppResult<()>;
    async fn info(&self, operation_id: &str) -> AppResult<VectorInfo>;
    async fn list_collections(&self, operation_id: &str) -> AppResult<Vec<VectorCollection>>;
    async fn describe_collection(&self, name: &str, operation_id: &str) -> AppResult<VectorCollectionDetail>;
    async fn create_collection(&self, request: VectorCreateRequest) -> AppResult<()>;
    async fn drop_collection(&self, name: &str, confirmed_dangerous: bool, operation_id: &str) -> AppResult<()>;
    async fn search(&self, request: VectorSearchRequest) -> AppResult<VectorSearchResult>;
    async fn query(&self, request: VectorQueryRequest) -> AppResult<VectorQueryResult>;
    async fn upsert(&self, request: VectorUpsertRequest) -> AppResult<VectorWriteResult>;
    async fn delete(&self, request: VectorDeleteRequest, confirmed_dangerous: bool) -> AppResult<VectorWriteResult>;
    async fn execute_rest(&self, request: VectorRestRequest) -> AppResult<VectorRestResult>;
    async fn cancel(&self, operation_id: &str) -> AppResult<()>;
    async fn close(&self) -> AppResult<()>;
}
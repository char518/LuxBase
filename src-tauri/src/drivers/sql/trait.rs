use crate::error::AppResult;
use crate::models::{
    ConnectionConfig, ExecuteResult, MetadataRequest, MetadataSnapshot, ObjectStat, PoolMetrics,
    QueryResult,
    SqlBatchRequest, SqlExportRequest, SqlExportResult, SqlImportRequest, SqlRequest,
    SqlScriptRequest, SqlScriptResult,
};
use async_trait::async_trait;

#[async_trait]
pub trait SqlDriver: Send + Sync {
    fn connection_id(&self) -> &str;
    async fn connect(&mut self, config: &ConnectionConfig, password: Option<String>) -> AppResult<()>;
    async fn execute(&self, request: SqlRequest) -> AppResult<ExecuteResult>;
    async fn execute_batch(&self, request: SqlBatchRequest) -> AppResult<Vec<ExecuteResult>>;
    async fn transaction(&self, request: SqlBatchRequest) -> AppResult<Vec<ExecuteResult>>;
    async fn stream_import(&self, request: SqlImportRequest) -> AppResult<u64>;
    async fn export_snapshot(&self, request: SqlExportRequest) -> AppResult<SqlExportResult>;
    async fn export_query(&self, request: crate::models::SqlQueryExportRequest) -> AppResult<SqlExportResult>;
    async fn fetch(&self, request: SqlRequest) -> AppResult<QueryResult>;
    /// Runs statements in order on one session; `transactional` wraps them in one transaction
    /// that is rolled back when any statement fails.
    async fn run_script(&self, request: SqlScriptRequest, transactional: bool) -> AppResult<SqlScriptResult>;
    async fn cancel(&self, operation_id: &str) -> AppResult<()>;
    async fn metadata(&self, request: MetadataRequest) -> AppResult<MetadataSnapshot>;
    /// The CREATE statement(s) for a table or view, as the server would show them.
    async fn ddl(&self, schema: &str, table: &str) -> AppResult<String>;
    async fn object_stats(&self, schema: &str) -> AppResult<Vec<ObjectStat>>;
    async fn metrics(&self) -> AppResult<PoolMetrics>;
    async fn health_check(&self) -> AppResult<()>;
    async fn close(&self) -> AppResult<()>;
}

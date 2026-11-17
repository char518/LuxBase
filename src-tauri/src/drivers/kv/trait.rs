use crate::error::AppResult;
use crate::models::{
    ConnectionConfig, RedisCommandRequest, RedisCommandResult, RedisInfo, RedisScanRequest,
    RedisScanResult, RedisValue,
};
use async_trait::async_trait;

#[async_trait]
pub trait KvDriver: Send + Sync {
    fn connection_id(&self) -> &str;
    async fn connect(&mut self, config: &ConnectionConfig, password: Option<String>) -> AppResult<()>;
    async fn scan(&self, request: RedisScanRequest) -> AppResult<RedisScanResult>;
    async fn get(&self, key: &[u8], db: u32, operation_id: &str) -> AppResult<RedisValue>;
    async fn set(&self, key: &[u8], db: u32, value: RedisValue, operation_id: &str) -> AppResult<()>;
    async fn create(&self, db: u32, value: RedisValue, operation_id: &str) -> AppResult<()>;
    async fn del(
        &self,
        keys: &[Vec<u8>],
        db: u32,
        confirmed_dangerous: bool,
        operation_id: &str,
    ) -> AppResult<u64>;
    async fn ttl(&self, key: &[u8], db: u32, operation_id: &str) -> AppResult<i64>;
    async fn key_type(&self, key: &[u8], db: u32, operation_id: &str) -> AppResult<String>;
    async fn dbsize(&self, db: u32, operation_id: &str) -> AppResult<u64>;
    async fn info(&self, operation_id: &str) -> AppResult<RedisInfo>;
    async fn health_check(&self) -> AppResult<()>;
    async fn execute_raw(&self, request: RedisCommandRequest) -> AppResult<RedisCommandResult>;
    async fn cancel(&self, operation_id: &str) -> AppResult<()>;
    async fn close(&self) -> AppResult<()>;
}

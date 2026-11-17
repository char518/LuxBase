mod redis_driver;
mod r#trait;

pub use r#trait::KvDriver;

use crate::error::{AppError, AppResult};
use crate::models::{
    ConnectionConfig, ConnectionTestResult, ErrorClass, Protocol, RedisCommandRequest,
    RedisCommandResult, RedisInfo, RedisScanRequest, RedisScanResult, RedisValue,
};
use parking_lot::{Mutex, RwLock};
use redis_driver::RedisDriver;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

#[derive(Default)]
pub struct KvConnectionManager {
    drivers: RwLock<HashMap<String, Arc<dyn KvDriver>>>,
    connect_locks: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    last_used: Mutex<HashMap<String, Instant>>,
}

const IDLE_PROBE_AFTER: std::time::Duration = std::time::Duration::from_secs(30);

impl KvConnectionManager {
    pub async fn connect(
        &self,
        config: &ConnectionConfig,
        password: Option<String>,
    ) -> AppResult<Arc<dyn KvDriver>> {
        if config.protocol != Protocol::Redis {
            return Err(AppError::unsupported(
                "SQL connections cannot be opened through the KV driver manager.",
            ));
        }
        let existing = self.drivers.read().get(&config.id).cloned();
        if let Some(existing) = existing {
            return Ok(existing);
        }
        let mut driver = RedisDriver::new(config.id.clone());
        driver.connect(config, password).await?;
        let driver: Arc<dyn KvDriver> = Arc::new(driver);
        self.drivers
            .write()
            .insert(config.id.clone(), Arc::clone(&driver));
        Ok(driver)
    }

    pub async fn get_or_connect(
        &self,
        config: &ConnectionConfig,
        password: Option<String>,
    ) -> AppResult<Arc<dyn KvDriver>> {
        let existing = self.drivers.read().get(&config.id).cloned();
        if let Some(driver) = existing {
            // The Redis connection manager reconnects by itself, but the first command after a
            // dropped socket still fails; only a connection that sat idle is probed first.
            let idle = self
                .last_used
                .lock()
                .insert(config.id.clone(), Instant::now())
                .is_some_and(|used| used.elapsed() >= IDLE_PROBE_AFTER);
            if !idle || driver.health_check().await.is_ok() || driver.health_check().await.is_ok() {
                return Ok(driver);
            }
            self.drivers.write().remove(&config.id);
            let _ = driver.close().await;
        }
        let lock = Arc::clone(self.connect_locks.lock().entry(config.id.clone()).or_default());
        let _guard = lock.lock().await;
        let driver = self.connect(config, password).await?;
        self.last_used.lock().insert(config.id.clone(), Instant::now());
        Ok(driver)
    }

    pub async fn test(
        &self,
        config: &ConnectionConfig,
        password: Option<String>,
    ) -> ConnectionTestResult {
        let started = Instant::now();
        let mut driver = RedisDriver::new(format!("test-{}", config.id));
        let result = driver.connect(config, password).await;
        let latency_ms = started.elapsed().as_millis() as u64;
        match result {
            Ok(()) => {
                let server_version = driver
                    .execute_raw(RedisCommandRequest {
                        connection_id: config.id.clone(),
                        command: "INFO".into(),
                        args: vec!["server".into()],
                        operation_id: format!("test-version-{}", config.id),
                        confirmed_dangerous: false,
                    })
                    .await
                    .ok()
                    .and_then(|result| result.value.as_str().map(str::to_string))
                    .and_then(|info| {
                        info.lines()
                            .find_map(|line| line.strip_prefix("redis_version:"))
                            .map(|version| format!("Redis {}", version.trim()))
                    });
                let _ = driver.close().await;
                ConnectionTestResult {
                    ok: true,
                    latency_ms,
                    server_version,
                    error_class: None,
                    message: None,
                }
            }
            Err(error) => ConnectionTestResult {
                ok: false,
                latency_ms,
                server_version: None,
                error_class: Some(error.class),
                message: Some(error.message),
            },
        }
    }

    pub async fn scan(
        &self,
        config: &ConnectionConfig,
        password: Option<String>,
        request: RedisScanRequest,
    ) -> AppResult<RedisScanResult> {
        let driver = self.get_or_connect(config, password).await?;
        driver.scan(request).await
    }

    pub async fn get(
        &self,
        config: &ConnectionConfig,
        password: Option<String>,
        key: &[u8],
        db: u32,
        operation_id: &str,
    ) -> AppResult<RedisValue> {
        let driver = self.get_or_connect(config, password).await?;
        driver.get(key, db, operation_id).await
    }

    pub async fn set(
        &self,
        config: &ConnectionConfig,
        password: Option<String>,
        key: &[u8],
        db: u32,
        value: RedisValue,
        operation_id: &str,
    ) -> AppResult<()> {
        let driver = self.get_or_connect(config, password).await?;
        driver.set(key, db, value, operation_id).await
    }

    pub async fn create(
        &self,
        config: &ConnectionConfig,
        password: Option<String>,
        db: u32,
        value: RedisValue,
        operation_id: &str,
    ) -> AppResult<()> {
        let driver = self.get_or_connect(config, password).await?;
        driver.create(db, value, operation_id).await
    }

    pub async fn del(
        &self,
        config: &ConnectionConfig,
        password: Option<String>,
        keys: &[Vec<u8>],
        db: u32,
        confirmed_dangerous: bool,
        operation_id: &str,
    ) -> AppResult<u64> {
        let driver = self.get_or_connect(config, password).await?;
        driver.del(keys, db, confirmed_dangerous, operation_id).await
    }

    pub async fn dbsize(
        &self,
        config: &ConnectionConfig,
        password: Option<String>,
        db: u32,
        operation_id: &str,
    ) -> AppResult<u64> {
        let driver = self.get_or_connect(config, password).await?;
        driver.dbsize(db, operation_id).await
    }

    pub async fn info(
        &self,
        config: &ConnectionConfig,
        password: Option<String>,
        operation_id: &str,
    ) -> AppResult<RedisInfo> {
        let driver = self.get_or_connect(config, password).await?;
        driver.info(operation_id).await
    }

    pub async fn execute_raw(
        &self,
        config: &ConnectionConfig,
        password: Option<String>,
        request: RedisCommandRequest,
    ) -> AppResult<RedisCommandResult> {
        let driver = self.get_or_connect(config, password).await?;
        driver.execute_raw(request).await
    }

    pub async fn cancel(&self, connection_id: &str, operation_id: &str) -> AppResult<()> {
        let driver = self
            .drivers
            .read()
            .get(connection_id)
            .cloned()
            .ok_or_else(|| AppError::new(ErrorClass::ConnectionFailed, "The Redis connection is not open."))?;
        driver.cancel(operation_id).await
    }

    pub async fn disconnect(&self, connection_id: &str) -> AppResult<()> {
        let driver = self.drivers.write().remove(connection_id);
        if let Some(driver) = driver {
            driver.close().await?;
        }
        Ok(())
    }

    pub async fn close_all(&self) {
        let drivers = self
            .drivers
            .write()
            .drain()
            .map(|(_, driver)| driver)
            .collect::<Vec<_>>();
        for driver in drivers {
            let _ = driver.close().await;
        }
    }
}

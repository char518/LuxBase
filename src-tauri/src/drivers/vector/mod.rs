mod common;
mod milvus_driver;
mod qdrant_driver;
mod r#trait;

pub use r#trait::VectorDriver;

use crate::error::{AppError, AppResult};
use crate::models::{
    ConnectionConfig, ConnectionTestResult, ErrorClass, Protocol, VectorCollection,
    VectorCollectionDetail, VectorCreateRequest, VectorDeleteRequest, VectorInfo,
    VectorQueryRequest, VectorQueryResult, VectorRestRequest, VectorRestResult,
    VectorSearchRequest, VectorSearchResult, VectorUpsertRequest, VectorWriteResult,
};
use milvus_driver::MilvusDriver;
use parking_lot::{Mutex, RwLock};
use qdrant_driver::QdrantDriver;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

#[derive(Default)]
pub struct VectorConnectionManager {
    drivers: RwLock<HashMap<String, Arc<dyn VectorDriver>>>,
    connect_locks: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    last_used: Mutex<HashMap<String, Instant>>,
}

const IDLE_PROBE_AFTER: std::time::Duration = std::time::Duration::from_secs(30);

fn build_driver(config: &ConnectionConfig, id: String) -> AppResult<Box<dyn VectorDriver>> {
    match config.protocol {
        Protocol::Milvus => Ok(Box::new(MilvusDriver::new(id))),
        Protocol::Qdrant => Ok(Box::new(QdrantDriver::new(id))),
        _ => Err(AppError::unsupported(
            "Only Milvus and Qdrant connections can be opened through the vector driver manager.",
        )),
    }
}

impl VectorConnectionManager {
    pub async fn connect(
        &self,
        config: &ConnectionConfig,
        password: Option<String>,
    ) -> AppResult<Arc<dyn VectorDriver>> {
        if config.protocol.family() != crate::models::DriverFamily::Vector {
            return Err(AppError::unsupported(
                "Only vector-store connections can be opened through the vector driver manager.",
            ));
        }
        if let Some(existing) = self.drivers.read().get(&config.id).cloned() {
            return Ok(existing);
        }
        let mut driver = build_driver(config, config.id.clone())?;
        driver.connect(config, password).await?;
        let driver: Arc<dyn VectorDriver> = Arc::from(driver);
        self.drivers.write().insert(config.id.clone(), Arc::clone(&driver));
        Ok(driver)
    }

    pub async fn get_or_connect(
        &self,
        config: &ConnectionConfig,
        password: Option<String>,
    ) -> AppResult<Arc<dyn VectorDriver>> {
        let existing = self.drivers.read().get(&config.id).cloned();
        if let Some(driver) = existing {
            let idle = self
                .last_used
                .lock()
                .insert(config.id.clone(), Instant::now())
                .is_some_and(|used| used.elapsed() >= IDLE_PROBE_AFTER);
            if !idle || driver.health_check().await.is_ok() {
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
        let mut driver = match build_driver(config, format!("test-{}", config.id)) {
            Ok(driver) => driver,
            Err(error) => {
                return ConnectionTestResult {
                    ok: false,
                    latency_ms: started.elapsed().as_millis() as u64,
                    server_version: None,
                    error_class: Some(error.class),
                    message: Some(error.message),
                };
            }
        };
        let result = driver.connect(config, password).await;
        let latency_ms = started.elapsed().as_millis() as u64;
        match result {
            Ok(()) => {
                let server_version = driver
                    .info(&format!("test-version-{}", config.id))
                    .await
                    .ok()
                    .map(|info| match info.version {
                        Some(version) => format!("{} {version}", info.product),
                        None => info.product,
                    });
                let _ = driver.close().await;
                ConnectionTestResult { ok: true, latency_ms, server_version, error_class: None, message: None }
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

    async fn driver(&self, config: &ConnectionConfig, password: Option<String>) -> AppResult<Arc<dyn VectorDriver>> {
        self.get_or_connect(config, password).await
    }

    pub async fn info(&self, config: &ConnectionConfig, password: Option<String>, operation_id: &str) -> AppResult<VectorInfo> {
        self.driver(config, password).await?.info(operation_id).await
    }

    pub async fn list_collections(&self, config: &ConnectionConfig, password: Option<String>, operation_id: &str) -> AppResult<Vec<VectorCollection>> {
        self.driver(config, password).await?.list_collections(operation_id).await
    }

    pub async fn describe_collection(&self, config: &ConnectionConfig, password: Option<String>, name: &str, operation_id: &str) -> AppResult<VectorCollectionDetail> {
        self.driver(config, password).await?.describe_collection(name, operation_id).await
    }

    pub async fn create_collection(&self, config: &ConnectionConfig, password: Option<String>, request: VectorCreateRequest) -> AppResult<()> {
        self.driver(config, password).await?.create_collection(request).await
    }

    pub async fn drop_collection(&self, config: &ConnectionConfig, password: Option<String>, name: &str, confirmed: bool, operation_id: &str) -> AppResult<()> {
        self.driver(config, password).await?.drop_collection(name, confirmed, operation_id).await
    }

    pub async fn search(&self, config: &ConnectionConfig, password: Option<String>, request: VectorSearchRequest) -> AppResult<VectorSearchResult> {
        self.driver(config, password).await?.search(request).await
    }

    pub async fn query(&self, config: &ConnectionConfig, password: Option<String>, request: VectorQueryRequest) -> AppResult<VectorQueryResult> {
        self.driver(config, password).await?.query(request).await
    }

    pub async fn get(&self, config: &ConnectionConfig, password: Option<String>, collection: &str, ids: Vec<serde_json::Value>, operation_id: &str) -> AppResult<Vec<serde_json::Value>> {
        self.driver(config, password).await?.get(collection, ids, operation_id).await
    }

    pub async fn upsert(&self, config: &ConnectionConfig, password: Option<String>, request: VectorUpsertRequest) -> AppResult<VectorWriteResult> {
        self.driver(config, password).await?.upsert(request).await
    }

    pub async fn delete(&self, config: &ConnectionConfig, password: Option<String>, request: VectorDeleteRequest, confirmed: bool) -> AppResult<VectorWriteResult> {
        self.driver(config, password).await?.delete(request, confirmed).await
    }

    pub async fn execute_rest(&self, config: &ConnectionConfig, password: Option<String>, request: VectorRestRequest) -> AppResult<VectorRestResult> {
        self.driver(config, password).await?.execute_rest(request).await
    }

    pub async fn cancel(&self, connection_id: &str, operation_id: &str) -> AppResult<()> {
        let driver = self
            .drivers
            .read()
            .get(connection_id)
            .cloned()
            .ok_or_else(|| AppError::new(ErrorClass::ConnectionFailed, "The vector-store connection is not open."))?;
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
        let drivers = self.drivers.write().drain().map(|(_, driver)| driver).collect::<Vec<_>>();
        for driver in drivers {
            let _ = driver.close().await;
        }
    }
}
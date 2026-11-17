mod es_driver;
mod r#trait;

pub use r#trait::SearchDriver;

use crate::error::{AppError, AppResult};
use crate::models::{
    ConnectionConfig, ConnectionTestResult, ErrorClass, EsAlias, EsClusterInfo,
    EsDocument, EsIndex, EsRestRequest, EsRestResult, EsSearchRequest, EsSearchResult, Protocol,
};
use es_driver::EsDriver;
use parking_lot::{Mutex, RwLock};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

#[derive(Default)]
pub struct SearchConnectionManager {
    drivers: RwLock<HashMap<String, Arc<dyn SearchDriver>>>,
    connect_locks: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    last_used: Mutex<HashMap<String, Instant>>,
}

const IDLE_PROBE_AFTER: std::time::Duration = std::time::Duration::from_secs(30);

impl SearchConnectionManager {
    pub async fn connect(
        &self,
        config: &ConnectionConfig,
        password: Option<String>,
    ) -> AppResult<Arc<dyn SearchDriver>> {
        if config.protocol != Protocol::Elasticsearch {
            return Err(AppError::unsupported(
                "Only Elasticsearch connections can be opened through the search driver manager.",
            ));
        }
        if let Some(existing) = self.drivers.read().get(&config.id).cloned() {
            return Ok(existing);
        }
        let mut driver = EsDriver::new(config.id.clone());
        driver.connect(config, password).await?;
        let driver: Arc<dyn SearchDriver> = Arc::new(driver);
        self.drivers.write().insert(config.id.clone(), Arc::clone(&driver));
        Ok(driver)
    }

    pub async fn get_or_connect(
        &self,
        config: &ConnectionConfig,
        password: Option<String>,
    ) -> AppResult<Arc<dyn SearchDriver>> {
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
        let mut driver = EsDriver::new(format!("test-{}", config.id));
        let result = driver.connect(config, password).await;
        let latency_ms = started.elapsed().as_millis() as u64;
        match result {
            Ok(()) => {
                let server_version = driver
                    .info(&format!("test-version-{}", config.id))
                    .await
                    .ok()
                    .and_then(|info| info.version.map(|version| format!("Elasticsearch {version}")));
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

    async fn driver(&self, config: &ConnectionConfig, password: Option<String>) -> AppResult<Arc<dyn SearchDriver>> {
        self.get_or_connect(config, password).await
    }

    pub async fn info(&self, config: &ConnectionConfig, password: Option<String>, operation_id: &str) -> AppResult<EsClusterInfo> {
        self.driver(config, password).await?.info(operation_id).await
    }

    pub async fn list_indices(&self, config: &ConnectionConfig, password: Option<String>, operation_id: &str) -> AppResult<Vec<EsIndex>> {
        self.driver(config, password).await?.list_indices(operation_id).await
    }

    pub async fn create_index(&self, config: &ConnectionConfig, password: Option<String>, name: &str, body: Value, operation_id: &str) -> AppResult<()> {
        self.driver(config, password).await?.create_index(name, body, operation_id).await
    }

    pub async fn delete_index(&self, config: &ConnectionConfig, password: Option<String>, name: &str, confirmed: bool, operation_id: &str) -> AppResult<()> {
        self.driver(config, password).await?.delete_index(name, confirmed, operation_id).await
    }

    pub async fn get_mapping(&self, config: &ConnectionConfig, password: Option<String>, name: &str, operation_id: &str) -> AppResult<Value> {
        self.driver(config, password).await?.get_mapping(name, operation_id).await
    }

    pub async fn list_aliases(&self, config: &ConnectionConfig, password: Option<String>, operation_id: &str) -> AppResult<Vec<EsAlias>> {
        self.driver(config, password).await?.list_aliases(operation_id).await
    }

    pub async fn modify_aliases(&self, config: &ConnectionConfig, password: Option<String>, actions: Value, confirmed: bool, operation_id: &str) -> AppResult<()> {
        self.driver(config, password).await?.modify_aliases(actions, confirmed, operation_id).await
    }

    pub async fn search(&self, config: &ConnectionConfig, password: Option<String>, request: EsSearchRequest) -> AppResult<EsSearchResult> {
        self.driver(config, password).await?.search(request).await
    }

    pub async fn get_document(&self, config: &ConnectionConfig, password: Option<String>, index: &str, id: &str, operation_id: &str) -> AppResult<EsDocument> {
        self.driver(config, password).await?.get_document(index, id, operation_id).await
    }

    pub async fn put_document(&self, config: &ConnectionConfig, password: Option<String>, index: &str, id: &str, source: Value, operation_id: &str) -> AppResult<EsDocument> {
        self.driver(config, password).await?.put_document(index, id, source, operation_id).await
    }

    pub async fn delete_document(&self, config: &ConnectionConfig, password: Option<String>, index: &str, id: &str, confirmed: bool, operation_id: &str) -> AppResult<()> {
        self.driver(config, password).await?.delete_document(index, id, confirmed, operation_id).await
    }

    pub async fn execute_rest(&self, config: &ConnectionConfig, password: Option<String>, request: EsRestRequest) -> AppResult<EsRestResult> {
        self.driver(config, password).await?.execute_rest(request).await
    }

    pub async fn cancel(&self, connection_id: &str, operation_id: &str) -> AppResult<()> {
        let driver = self
            .drivers
            .read()
            .get(connection_id)
            .cloned()
            .ok_or_else(|| AppError::new(ErrorClass::ConnectionFailed, "The Elasticsearch connection is not open."))?;
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
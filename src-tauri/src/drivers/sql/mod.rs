mod export;
mod import;
mod metadata;
mod sqlite_driver;
mod sqlx_driver;
mod tracker;
mod r#trait;

pub use r#trait::SqlDriver;

use crate::error::{AppError, AppResult};
use crate::models::{
    ConnectionConfig, ConnectionTestResult, ExecuteResult, MetadataRequest, MetadataSnapshot,
    PoolMetrics, Protocol, SqlBatchRequest, SqlExportRequest, SqlExportResult, SqlImportRequest,
    SqlRequest, SqlScriptRequest, SqlScriptResult,
};
use parking_lot::{Mutex, RwLock};
use sqlite_driver::SqliteDriver;
use sqlx_driver::SqlxDriver;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

pub(super) fn integer_cell<T: Into<i128>>(value: T) -> serde_json::Value {
    let value = value.into();
    if value.unsigned_abs() <= 9_007_199_254_740_991 {
        serde_json::Value::from(value as i64)
    } else {
        serde_json::Value::String(value.to_string())
    }
}

/// Ceiling kept below the smallest per-statement bind limit of the supported servers.
pub(crate) const MAX_BIND_PARAMETERS: usize = 30_000;

/// One `VALUES` list of `row_count` tuples with `column_count` bound placeholders each.
pub(crate) fn value_placeholders(
    protocol: Protocol,
    column_count: usize,
    row_count: usize,
) -> AppResult<String> {
    if column_count == 0 || row_count == 0 {
        return Err(AppError::validation(
            "A batched insert needs at least one column and one row.",
        ));
    }
    if column_count.saturating_mul(row_count) > MAX_BIND_PARAMETERS {
        return Err(AppError::validation(format!(
            "{row_count} rows over {column_count} columns exceed the {MAX_BIND_PARAMETERS} bound parameters allowed per statement."
        )));
    }
    Ok(match protocol {
        Protocol::Postgres => (0..row_count)
            .map(|row| {
                let start = row * column_count + 1;
                let end = start + column_count - 1;
                format!(
                    "({})",
                    (start..=end)
                        .map(|index| format!("${index}"))
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            })
            .collect::<Vec<_>>()
            .join(", "),
        Protocol::Mysql | Protocol::Sqlite => {
            vec![format!("({})", vec!["?"; column_count].join(", ")); row_count].join(", ")
        }
        Protocol::Redis | Protocol::Elasticsearch => {
            return Err(AppError::unsupported("Redis and Elasticsearch accept no SQL placeholders."))
        }
    })
}

#[derive(Default)]
pub struct SqlConnectionManager {
    drivers: RwLock<HashMap<String, Arc<dyn SqlDriver>>>,
    connect_locks: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
}

impl SqlConnectionManager {
    pub async fn connect(
        &self,
        config: &ConnectionConfig,
        password: Option<String>,
    ) -> AppResult<Arc<dyn SqlDriver>> {
        if config.protocol.family() != crate::models::DriverFamily::Sql {
            return Err(AppError::unsupported(
                "A Redis connection cannot be opened through the SQL driver manager.",
            ));
        }
        let existing = self.drivers.read().get(&config.id).cloned();
        if let Some(existing) = existing {
            return Ok(existing);
        }

        let driver: Arc<dyn SqlDriver> = match config.protocol {
            Protocol::Sqlite => {
                let mut driver = SqliteDriver::new(config.id.clone());
                driver.connect(config, password).await?;
                Arc::new(driver)
            }
            Protocol::Mysql | Protocol::Postgres => {
                let mut driver = SqlxDriver::new(config.id.clone(), config.protocol);
                driver.connect(config, password).await?;
                Arc::new(driver)
            }
            Protocol::Redis | Protocol::Elasticsearch => {
                return Err(AppError::unsupported(
                    "Redis and Elasticsearch are handled exclusively through their own driver managers.",
                ))
            }
        };
        self.drivers
            .write()
            .insert(config.id.clone(), Arc::clone(&driver));
        Ok(driver)
    }

    /// Pools reconnect on their own (and ping connections that sat idle), so an open driver is
    /// reused without an extra health round trip. Opening is serialized per connection so a burst
    /// of first requests (tree + editor + inspector) creates one pool instead of racing several.
    pub async fn get_or_connect(
        &self,
        config: &ConnectionConfig,
        password: Option<String>,
    ) -> AppResult<Arc<dyn SqlDriver>> {
        if let Some(driver) = self.drivers.read().get(&config.id).cloned() {
            return Ok(driver);
        }
        let lock = Arc::clone(self.connect_locks.lock().entry(config.id.clone()).or_default());
        let _guard = lock.lock().await;
        self.connect(config, password).await
    }

    pub async fn test(
        &self,
        config: &ConnectionConfig,
        password: Option<String>,
    ) -> ConnectionTestResult {
        let started = Instant::now();
        let driver: AppResult<Box<dyn SqlDriver>> = match config.protocol {
            Protocol::Sqlite => {
                let mut driver = SqliteDriver::new(format!("test-{}", config.id));
                match driver.connect(config, password).await {
                    Ok(()) => Ok(Box::new(driver)),
                    Err(error) => Err(error),
                }
            }
            Protocol::Mysql | Protocol::Postgres => {
                let mut driver = SqlxDriver::new(format!("test-{}", config.id), config.protocol);
                match driver.connect(config, password).await {
                    Ok(()) => Ok(Box::new(driver)),
                    Err(error) => Err(error),
                }
            }
            Protocol::Redis | Protocol::Elasticsearch => {
                return ConnectionTestResult {
                    ok: false,
                    latency_ms: started.elapsed().as_millis() as u64,
                    server_version: None,
                    error_class: Some(crate::models::ErrorClass::ProtocolUnsupported),
                    message: Some("Use the protocol-specific test path for KV/search connections.".into()),
                }
            }
        };
        match driver {
            Ok(driver) => {
                let latency_ms = started.elapsed().as_millis() as u64;
                let version_sql = match config.protocol {
                    Protocol::Sqlite => "SELECT sqlite_version()",
                    _ => "SELECT VERSION()",
                };
                let server_version = driver
                    .fetch(SqlRequest {
                        connection_id: config.id.clone(),
                        sql: version_sql.into(),
                        params: Vec::new(),
                        operation_id: format!("test-version-{}", config.id),
                        page_size: Some(1),
                        offset: None,
                        schema: None,
                    })
                    .await
                    .ok()
                    .and_then(|result| result.rows.into_iter().next())
                    .and_then(|row| row.into_iter().next())
                    .and_then(|value| value.as_str().map(str::to_string));
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
                latency_ms: started.elapsed().as_millis() as u64,
                server_version: None,
                error_class: Some(error.class),
                message: Some(error.message),
            },
        }
    }

    pub async fn execute(
        &self,
        config: &ConnectionConfig,
        password: Option<String>,
        request: SqlRequest,
    ) -> AppResult<ExecuteResult> {
        let driver = self.get_or_connect(config, password).await?;
        driver.execute(request).await
    }

    pub async fn execute_batch(
        &self,
        config: &ConnectionConfig,
        password: Option<String>,
        request: SqlBatchRequest,
    ) -> AppResult<Vec<ExecuteResult>> {
        let driver = self.get_or_connect(config, password).await?;
        driver.transaction(request).await
    }

    pub async fn fetch(
        &self,
        config: &ConnectionConfig,
        password: Option<String>,
        request: SqlRequest,
    ) -> AppResult<crate::models::QueryResult> {
        let driver = self.get_or_connect(config, password).await?;
        driver.fetch(request).await
    }

    pub async fn run_script(
        &self,
        config: &ConnectionConfig,
        password: Option<String>,
        request: SqlScriptRequest,
        transactional: bool,
    ) -> AppResult<SqlScriptResult> {
        let driver = self.get_or_connect(config, password).await?;
        driver.run_script(request, transactional).await
    }

    pub async fn stream_import(
        &self,
        config: &ConnectionConfig,
        password: Option<String>,
        request: SqlImportRequest,
    ) -> AppResult<u64> {
        let driver = self.get_or_connect(config, password).await?;
        driver.stream_import(request).await
    }

    pub async fn export_query(
        &self,
        config: &ConnectionConfig,
        password: Option<String>,
        request: crate::models::SqlQueryExportRequest,
    ) -> AppResult<SqlExportResult> {
        let driver = self.get_or_connect(config, password).await?;
        driver.export_query(request).await
    }

    pub fn export_result(
        protocol: Protocol,
        format: &str,
        path: &str,
        columns: Vec<crate::models::ResultColumn>,
        rows: Vec<Vec<serde_json::Value>>,
    ) -> AppResult<SqlExportResult> {
        let mut writer = export::SqlSnapshotWriter::new(protocol, format, path, "", false)?;
        writer.begin_table("query_result", columns.into_iter().map(|column| column.name).collect())?;
        for row in rows { writer.write_row(&row)?; }
        let (rows_exported, tables_exported) = writer.finish()?;
        Ok(SqlExportResult { rows_exported, tables_exported, output_path: path.into(), snapshot_isolated: false })
    }

    pub async fn export_snapshot(
        &self,
        config: &ConnectionConfig,
        password: Option<String>,
        request: SqlExportRequest,
    ) -> AppResult<SqlExportResult> {
        let driver = self.get_or_connect(config, password).await?;
        driver.export_snapshot(request).await
    }

    pub async fn cancel(&self, connection_id: &str, operation_id: &str) -> AppResult<()> {
        let driver = self
            .drivers
            .read()
            .get(connection_id)
            .cloned()
            .ok_or_else(|| AppError::connection("The SQL connection is not open."))?;
        driver.cancel(operation_id).await
    }

    pub async fn metadata(
        &self,
        config: &ConnectionConfig,
        password: Option<String>,
        request: MetadataRequest,
    ) -> AppResult<MetadataSnapshot> {
        let driver = self.get_or_connect(config, password).await?;
        driver.metadata(request).await
    }

    pub async fn object_stats(
        &self,
        config: &ConnectionConfig,
        password: Option<String>,
        schema: &str,
    ) -> AppResult<Vec<crate::models::ObjectStat>> {
        let driver = self.get_or_connect(config, password).await?;
        driver.object_stats(schema).await
    }

    pub async fn ddl(
        &self,
        config: &ConnectionConfig,
        password: Option<String>,
        schema: &str,
        table: &str,
    ) -> AppResult<String> {
        let driver = self.get_or_connect(config, password).await?;
        driver.ddl(schema, table).await
    }

    pub async fn metrics(&self, connection_id: &str) -> AppResult<PoolMetrics> {
        let driver = self
            .drivers
            .read()
            .get(connection_id)
            .cloned()
            .ok_or_else(|| AppError::connection("The SQL connection is not open."))?;
        driver.metrics().await
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

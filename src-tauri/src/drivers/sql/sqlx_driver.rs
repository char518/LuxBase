use super::export::{qualified_table, SqlSnapshotWriter};
use super::import::SqlImportSource;
use super::metadata::{
    mysql_metadata, mysql_object_stats, postgres_ddl, postgres_metadata, postgres_object_stats,
};
use super::r#trait::SqlDriver;
use super::tracker::PoolTracker;
use crate::error::{AppError, AppResult};
use crate::models::{
    ConnectionConfig, ErrorClass, ExecuteResult, MetadataRequest, MetadataSnapshot, ParamValue,
    PoolMetrics, Protocol, QueryResult, ResultColumn, ResultPage, ScriptFailure,
    ScriptStatementResult, SqlBatchRequest, SqlExportRequest, SqlExportResult, SqlImportRequest,
    SqlQueryExportRequest, SqlRequest, SqlScriptRequest, SqlScriptResult, TlsMode,
};
use crate::ssh::SshTunnel;
use async_trait::async_trait;
use base64::{engine::general_purpose::STANDARD as B64, Engine};
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use futures::TryStreamExt;
use parking_lot::Mutex;
use serde_json::{json, Value};
use sqlx::{
    mysql::{MySqlConnectOptions, MySqlConnection, MySqlPoolOptions, MySqlRow, MySqlSslMode},
    pool::PoolConnectionMetadata,
    postgres::{types::Oid, PgConnectOptions, PgConnection, PgPoolOptions, PgRow, PgSslMode},
    types::BigDecimal,
    Acquire, Column, Either, Executor, MySql, MySqlPool, PgPool, Postgres, Row, Statement as _,
    TypeInfo,
};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

enum SqlxPool {
    MySql(MySqlPool),
    Postgres(PgPool),
}

#[derive(Clone)]
struct ActiveOperation {
    token: CancellationToken,
    backend_id: Option<u32>,
    in_transaction: bool,
}

pub struct SqlxDriver {
    connection_id: String,
    protocol: Protocol,
    pool: Option<SqlxPool>,
    tracker: PoolTracker,
    cancellations: Mutex<HashMap<String, ActiveOperation>>,
    tunnel: Option<SshTunnel>,
    query_timeout: Duration,
    max_size: u32,
    default_schema: Option<String>,
}

impl SqlxDriver {
    pub fn new(connection_id: String, protocol: Protocol) -> Self {
        Self {
            connection_id,
            protocol,
            pool: None,
            tracker: PoolTracker::default(),
            cancellations: Mutex::new(HashMap::new()),
            tunnel: None,
            query_timeout: Duration::from_secs(60),
            max_size: 20,
            default_schema: None,
        }
    }

    pub fn connection_id_value(&self) -> &str {
        &self.connection_id
    }

    fn pool(&self) -> AppResult<&SqlxPool> {
        self.pool
            .as_ref()
            .ok_or_else(|| AppError::connection("The SQL connection pool is not initialized."))
    }

    fn operation_token(&self, operation_id: &str) -> CancellationToken {
        let token = CancellationToken::new();
        self.cancellations.lock().insert(
            operation_id.to_string(),
            ActiveOperation {
                token: token.clone(),
                backend_id: None,
                in_transaction: false,
            },
        );
        token
    }

    fn record_backend_id(&self, operation_id: &str, backend_id: u32) {
        if let Some(operation) = self.cancellations.lock().get_mut(operation_id) {
            operation.backend_id = Some(backend_id);
        }
    }

    fn record_transaction(&self, operation_id: &str, in_transaction: bool) {
        if let Some(operation) = self.cancellations.lock().get_mut(operation_id) {
            operation.in_transaction = in_transaction;
        }
    }

    fn operation_snapshot(&self, operation_id: &str) -> Option<ActiveOperation> {
        self.cancellations.lock().get(operation_id).cloned()
    }

    fn finish_operation(&self, operation_id: &str) {
        self.cancellations.lock().remove(operation_id);
    }

    async fn cancel_backend_query(&self, operation: &ActiveOperation) -> AppResult<()> {
        operation.token.cancel();
        let Some(backend_id) = operation.backend_id else {
            return Ok(());
        };
        match self.pool()? {
            SqlxPool::MySql(pool) => {
                let statement = format!("KILL QUERY {backend_id}");
                sqlx::query(&statement).execute(pool).await?;
            }
            SqlxPool::Postgres(pool) => {
                let cancelled: bool = sqlx::query_scalar("SELECT pg_cancel_backend($1)")
                    .bind(backend_id as i32)
                    .fetch_one(pool)
                    .await?;
                if !cancelled {
                    return Err(AppError::new(
                        ErrorClass::ConnectionFailed,
                        format!("PostgreSQL backend {backend_id} was no longer running."),
                    ));
                }
            }
        }
        Ok(())
    }

    async fn with_timeout<T>(
        &self,
        operation_id: &str,
        token: CancellationToken,
        future: impl std::future::Future<Output = AppResult<T>>,
    ) -> AppResult<T> {
        tokio::select! {
            biased;
            _ = token.cancelled() => Err(AppError::cancelled()),
            result = tokio::time::timeout(self.query_timeout, future) => {
                match result {
                    Ok(value) => value,
                    Err(_) => {
                        self.tracker.timeout();
                        if let Some(operation) = self.operation_snapshot(operation_id) {
                            let _ = self.cancel_backend_query(&operation).await;
                        } else {
                            token.cancel();
                        }
                        Err(AppError::timeout("The query exceeded the configured timeout and was cancelled."))
                    }
                }
            }
        }
    }

    /// Pooled connections keep session state, so every operation re-selects the database the query
    /// tab is bound to and records the backend id for cancellation in a single round trip.
    async fn start_mysql_session(
        &self,
        connection: &mut MySqlConnection,
        operation_id: &str,
        schema: Option<&str>,
    ) -> AppResult<()> {
        let schema = schema
            .filter(|value| !value.trim().is_empty())
            .or(self.default_schema.as_deref());
        let backend_id: u64 = match schema {
            Some(schema) => {
                let sql = format!("USE `{}`; SELECT CONNECTION_ID()", schema.replace('`', "``"));
                // Executor methods return boxed futures; RawSql's own async fns trip rustc's
                // higher-ranked Send inference inside async_trait bodies.
                (&mut *connection).fetch_one(sqlx::raw_sql(&sql)).await?.try_get(0)?
            }
            None => {
                sqlx::query_scalar("SELECT CONNECTION_ID()")
                    .fetch_one(&mut *connection)
                    .await?
            }
        };
        self.record_backend_id(operation_id, backend_id.min(u32::MAX as u64) as u32);
        Ok(())
    }

    async fn start_postgres_session(
        &self,
        connection: &mut PgConnection,
        operation_id: &str,
        schema: Option<&str>,
    ) -> AppResult<()> {
        let search_path = schema
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|schema| format!("\"{}\", public", schema.replace('"', "\"\"")));
        let (backend_id, _): (i32, String) = sqlx::query_as(
            "SELECT pg_backend_pid(), set_config('search_path', \
             COALESCE($1, (SELECT reset_val FROM pg_settings WHERE name = 'search_path')), false)",
        )
        .bind(search_path)
        .fetch_one(&mut *connection)
        .await?;
        self.record_backend_id(operation_id, backend_id.max(0) as u32);
        Ok(())
    }

    async fn acquire_mysql(&self, pool: &MySqlPool) -> AppResult<sqlx::pool::PoolConnection<MySql>> {
        pool.acquire().await.map_err(|error| {
            if matches!(error, sqlx::Error::PoolTimedOut) {
                self.tracker.timeout();
            }
            AppError::from(error)
        })
    }

    async fn acquire_postgres(&self, pool: &PgPool) -> AppResult<sqlx::pool::PoolConnection<Postgres>> {
        pool.acquire().await.map_err(|error| {
            if matches!(error, sqlx::Error::PoolTimedOut) {
                self.tracker.timeout();
            }
            AppError::from(error)
        })
    }
}

#[async_trait]
impl SqlDriver for SqlxDriver {
    fn connection_id(&self) -> &str {
        &self.connection_id
    }

    async fn connect(
        &mut self,
        config: &ConnectionConfig,
        password: Option<String>,
    ) -> AppResult<()> {
        self.query_timeout = Duration::from_millis(config.query_timeout_ms);
        self.max_size = config.pool.max_size;
        self.default_schema = Some(config.database.clone()).filter(|value| !value.trim().is_empty());
        let mut host = config.host.clone();
        let mut port = config.port;
        if let Some(ssh) = &config.ssh {
            let tunnel = SshTunnel::start(
                ssh,
                config.ssh_password.clone(),
                config.ssh_passphrase.clone(),
                &config.host,
                config.port,
            )?;
            host = "127.0.0.1".to_string();
            port = tunnel.local_port();
            self.tunnel = Some(tunnel);
        }
        let tls_mode = config.tls_mode.unwrap_or(if config.ssh.is_some() {
            TlsMode::Disabled
        } else {
            TlsMode::Preferred
        });

        let pool = match config.protocol {
            Protocol::Mysql => {
                let ssl = match tls_mode {
                    TlsMode::Disabled => MySqlSslMode::Disabled,
                    TlsMode::Preferred => MySqlSslMode::Preferred,
                    TlsMode::Required => MySqlSslMode::Required,
                    TlsMode::VerifyFull => MySqlSslMode::VerifyIdentity,
                };
                // Keep the server's own sql_mode and time zone so results match what the user's
                // other clients show; sqlx would otherwise add PIPES_AS_CONCAT and force UTC.
                let mut options = MySqlConnectOptions::new()
                    .host(&host)
                    .port(port)
                    .username(&config.username)
                    .database(&config.database)
                    .ssl_mode(ssl)
                    .pipes_as_concat(false)
                    .no_engine_substitution(false)
                    .timezone(None::<String>);
                if let Some(password) = password.as_deref() {
                    options = options.password(password);
                }
                if let Some(path) = config.ca_cert_path.as_deref() {
                    options = options.ssl_ca(path);
                }
                if let Some(path) = config.client_cert_path.as_deref() {
                    options = options.ssl_client_cert(path);
                }
                let pool = MySqlPoolOptions::new()
                    .min_connections(config.pool.min_idle)
                    .max_connections(config.pool.max_size)
                    .acquire_timeout(Duration::from_millis(config.pool.connection_timeout_ms))
                    .idle_timeout(Some(Duration::from_millis(config.pool.idle_timeout_ms)))
                    .max_lifetime(Some(Duration::from_millis(config.pool.max_lifetime_ms)))
                    .test_before_acquire(false)
                    .before_acquire(|connection, meta| Box::pin(ping_if_idle(connection, meta)))
                    .connect_with(options)
                    .await?;
                sqlx::query("SELECT 1").execute(&pool).await?;
                SqlxPool::MySql(pool)
            }
            Protocol::Postgres => {
                let ssl = match tls_mode {
                    TlsMode::Disabled => PgSslMode::Disable,
                    TlsMode::Preferred => PgSslMode::Prefer,
                    TlsMode::Required => PgSslMode::Require,
                    TlsMode::VerifyFull => PgSslMode::VerifyFull,
                };
                let mut options = PgConnectOptions::new()
                    .host(&host)
                    .port(port)
                    .username(&config.username)
                    .database(&config.database)
                    .ssl_mode(ssl);
                if let Some(password) = password.as_deref() {
                    options = options.password(password);
                }
                if let Some(path) = config.ca_cert_path.as_deref() {
                    options = options.ssl_root_cert(path);
                }
                if let Some(path) = config.client_cert_path.as_deref() {
                    options = options.ssl_client_cert(path);
                }
                let pool = PgPoolOptions::new()
                    .min_connections(config.pool.min_idle)
                    .max_connections(config.pool.max_size)
                    .acquire_timeout(Duration::from_millis(config.pool.connection_timeout_ms))
                    .idle_timeout(Some(Duration::from_millis(config.pool.idle_timeout_ms)))
                    .max_lifetime(Some(Duration::from_millis(config.pool.max_lifetime_ms)))
                    .test_before_acquire(false)
                    .before_acquire(|connection, meta| Box::pin(ping_if_idle(connection, meta)))
                    .connect_with(options)
                    .await?;
                sqlx::query("SELECT 1").execute(&pool).await?;
                SqlxPool::Postgres(pool)
            }
            _ => return Err(AppError::unsupported("This SQLx driver only supports MySQL and PostgreSQL.")),
        };
        self.pool = Some(pool);
        self.tracker.acquired();
        Ok(())
    }

    async fn execute(&self, request: SqlRequest) -> AppResult<ExecuteResult> {
        if request.params.is_empty() {
            // Safety guard: DDL is permitted, but values must never be interpolated by callers.
            // A parameter-free statement is still sent as opaque SQL through the protocol layer.
        }
        let token = self.operation_token(&request.operation_id);
        let started = Instant::now();
        let operation_id = request.operation_id.clone();
        let result = self
            .with_timeout(&operation_id, token, async {
                match self.pool()? {
                    SqlxPool::MySql(pool) => {
                        let _waiter = self.tracker.waiter();
                        let mut connection = pool.acquire().await.map_err(|error| {
                            if matches!(error, sqlx::Error::PoolTimedOut) {
                                self.tracker.timeout();
                            }
                            AppError::from(error)
                        })?;
                        self.start_mysql_session(&mut connection, &operation_id, request.schema.as_deref()).await?;
                        let result = mysql_execute(&mut connection, &request.sql, &request.params).await?;
                        self.tracker.acquired();
                        Ok(ExecuteResult {
                            operation_id: operation_id.clone(),
                            rows_affected: result.rows_affected(),
                            last_insert_id: Some(result.last_insert_id() as i64),
                            duration_ms: started.elapsed().as_millis() as u64,
                            committed: !request.sql.trim_start().to_ascii_uppercase().starts_with("BEGIN"),
                        })
                    }
                    SqlxPool::Postgres(pool) => {
                        let _waiter = self.tracker.waiter();
                        let mut connection = pool.acquire().await.map_err(|error| {
                            if matches!(error, sqlx::Error::PoolTimedOut) {
                                self.tracker.timeout();
                            }
                            AppError::from(error)
                        })?;
                        self.start_postgres_session(&mut connection, &operation_id, request.schema.as_deref()).await?;
                        let binds =
                            coerce_pg_params(&mut *connection, &request.sql, &request.params).await?;
                        let mut query = sqlx::query(&request.sql);
                        for bind in binds {
                            query = bind_postgres(query, bind);
                        }
                        let result = query.execute(&mut *connection).await?;
                        self.tracker.acquired();
                        Ok(ExecuteResult {
                            operation_id: operation_id.clone(),
                            rows_affected: result.rows_affected(),
                            last_insert_id: None,
                            duration_ms: started.elapsed().as_millis() as u64,
                            committed: !request.sql.trim_start().to_ascii_uppercase().starts_with("BEGIN"),
                        })
                    }
                }
            })
            .await;
        self.finish_operation(&request.operation_id);
        result
    }

    async fn execute_batch(&self, request: SqlBatchRequest) -> AppResult<Vec<ExecuteResult>> {
        if !request.wrap_in_transaction {
            return Err(AppError::validation(
                "SQL batches must be wrapped in a transaction. Partial commits are disabled.",
            ));
        }
        let token = self.operation_token(&request.operation_id);
        let started = Instant::now();
        let operation_id = request.operation_id.clone();
        let result = self
            .with_timeout(&operation_id, token.clone(), async {
                match self.pool()? {
                    SqlxPool::MySql(pool) => {
                        let _waiter = self.tracker.waiter();
                        let mut connection = pool.acquire().await.map_err(|error| {
                            if matches!(error, sqlx::Error::PoolTimedOut) {
                                self.tracker.timeout();
                            }
                            AppError::from(error)
                        })?;
                        self.record_transaction(&operation_id, true);
                        let batch_schema = request.statements.first().and_then(|statement| statement.schema.clone());
                        self.start_mysql_session(&mut connection, &operation_id, batch_schema.as_deref()).await?;
                        let mut tx = connection.begin().await?;
                        let mut results = Vec::with_capacity(request.statements.len());
                        for statement in &request.statements {
                            if token.is_cancelled() {
                                tx.rollback().await.ok();
                                return Err(AppError::cancelled());
                            }
                            let mut query = sqlx::query(&statement.sql);
                            for param in &statement.params {
                                query = bind_mysql(query, param);
                            }
                            match query.execute(&mut *tx).await {
                                Ok(done) => results.push(ExecuteResult {
                                    operation_id: statement.operation_id.clone(),
                                    rows_affected: done.rows_affected(),
                                    last_insert_id: Some(done.last_insert_id() as i64),
                                    duration_ms: started.elapsed().as_millis() as u64,
                                    committed: false,
                                }),
                                Err(error) => {
                                    tx.rollback().await.ok();
                                    if token.is_cancelled() {
                                        return Err(AppError::cancelled());
                                    }
                                    return Err(error.into());
                                }
                            }
                        }
                        if token.is_cancelled() {
                            tx.rollback().await.ok();
                            return Err(AppError::cancelled());
                        }
                        tx.commit().await?;
                        for result in &mut results {
                            result.committed = true;
                        }
                        Ok(results)
                    }
                    SqlxPool::Postgres(pool) => {
                        let _waiter = self.tracker.waiter();
                        let mut connection = pool.acquire().await.map_err(|error| {
                            if matches!(error, sqlx::Error::PoolTimedOut) {
                                self.tracker.timeout();
                            }
                            AppError::from(error)
                        })?;
                        self.record_transaction(&operation_id, true);
                        let batch_schema = request.statements.first().and_then(|statement| statement.schema.clone());
                        self.start_postgres_session(&mut connection, &operation_id, batch_schema.as_deref()).await?;
                        let mut tx = connection.begin().await?;
                        let mut results = Vec::with_capacity(request.statements.len());
                        for statement in &request.statements {
                            if token.is_cancelled() {
                                tx.rollback().await.ok();
                                return Err(AppError::cancelled());
                            }
                            let binds = match coerce_pg_params(&mut *tx, &statement.sql, &statement.params).await {
                                Ok(binds) => binds,
                                Err(error) => {
                                    tx.rollback().await.ok();
                                    return Err(error);
                                }
                            };
                            let mut query = sqlx::query(&statement.sql);
                            for bind in binds {
                                query = bind_postgres(query, bind);
                            }
                            match query.execute(&mut *tx).await {
                                Ok(done) => results.push(ExecuteResult {
                                    operation_id: statement.operation_id.clone(),
                                    rows_affected: done.rows_affected(),
                                    last_insert_id: None,
                                    duration_ms: started.elapsed().as_millis() as u64,
                                    committed: false,
                                }),
                                Err(error) => {
                                    tx.rollback().await.ok();
                                    if token.is_cancelled() {
                                        return Err(AppError::cancelled());
                                    }
                                    return Err(error.into());
                                }
                            }
                        }
                        if token.is_cancelled() {
                            tx.rollback().await.ok();
                            return Err(AppError::cancelled());
                        }
                        tx.commit().await?;
                        for result in &mut results {
                            result.committed = true;
                        }
                        Ok(results)
                    }
                }
            })
            .await;
        self.finish_operation(&operation_id);
        result
    }

    async fn transaction(&self, request: SqlBatchRequest) -> AppResult<Vec<ExecuteResult>> {
        self.execute_batch(request).await
    }

    async fn stream_import(&self, request: SqlImportRequest) -> AppResult<u64> {
        let token = self.operation_token(&request.operation_id);
        let operation_id = request.operation_id.clone();
        let result = self
            .with_timeout(&operation_id, token.clone(), async {
                match self.pool()? {
                    SqlxPool::MySql(pool) => {
                        let _waiter = self.tracker.waiter();
                        let mut connection = pool.acquire().await.map_err(|error| {
                            if matches!(error, sqlx::Error::PoolTimedOut) {
                                self.tracker.timeout();
                            }
                            AppError::from(error)
                        })?;
                        self.start_mysql_session(&mut connection, &operation_id, None).await?;
                        self.record_transaction(&operation_id, true);
                        let mut source = SqlImportSource::open(
                            Protocol::Mysql,
                            &request.path,
                            &request.format,
                            request.schema.as_deref(),
                            &request.table,
                        )?;
                        let batch_size = source.safe_batch_size(request.batch_size);
                        let mut tx = connection.begin().await?;
                        let mut imported = 0u64;
                        loop {
                            if token.is_cancelled() {
                                tx.rollback().await.ok();
                                return Err(AppError::cancelled());
                            }
                            let rows = source.next_batch(batch_size)?;
                            if rows.is_empty() {
                                break;
                            }
                            let row_count = rows.len();
                            let sql = source.insert_sql(row_count)?;
                            let params = rows.into_iter().flatten().collect::<Vec<_>>();
                            let mut query = sqlx::query(&sql);
                            for param in &params {
                                query = bind_mysql(query, param);
                            }
                            match query.execute(&mut *tx).await {
                                Ok(_) => {
                                    imported = imported.saturating_add(row_count as u64);
                                }
                                Err(error) => {
                                    tx.rollback().await.ok();
                                    if token.is_cancelled() {
                                        return Err(AppError::cancelled());
                                    }
                                    return Err(error.into());
                                }
                            }
                        }
                        if imported == 0 {
                            tx.rollback().await.ok();
                            return Err(AppError::validation(
                                "Import file contains no data rows.",
                            ));
                        }
                        if token.is_cancelled() {
                            tx.rollback().await.ok();
                            return Err(AppError::cancelled());
                        }
                        tx.commit().await?;
                        Ok(imported)
                    }
                    SqlxPool::Postgres(pool) => {
                        let _waiter = self.tracker.waiter();
                        let mut connection = pool.acquire().await.map_err(|error| {
                            if matches!(error, sqlx::Error::PoolTimedOut) {
                                self.tracker.timeout();
                            }
                            AppError::from(error)
                        })?;
                        self.start_postgres_session(&mut connection, &operation_id, None).await?;
                        self.record_transaction(&operation_id, true);
                        let mut source = SqlImportSource::open(
                            Protocol::Postgres,
                            &request.path,
                            &request.format,
                            request.schema.as_deref(),
                            &request.table,
                        )?;
                        let batch_size = source.safe_batch_size(request.batch_size);
                        let mut tx = connection.begin().await?;
                        let mut imported = 0u64;
                        loop {
                            if token.is_cancelled() {
                                tx.rollback().await.ok();
                                return Err(AppError::cancelled());
                            }
                            let rows = source.next_batch(batch_size)?;
                            if rows.is_empty() {
                                break;
                            }
                            let row_count = rows.len();
                            let sql = source.insert_sql(row_count)?;
                            let params = rows.into_iter().flatten().collect::<Vec<_>>();
                            let binds = match coerce_pg_params(&mut *tx, &sql, &params).await {
                                Ok(binds) => binds,
                                Err(error) => {
                                    tx.rollback().await.ok();
                                    return Err(error);
                                }
                            };
                            let mut query = sqlx::query(&sql);
                            for bind in binds {
                                query = bind_postgres(query, bind);
                            }
                            match query.execute(&mut *tx).await {
                                Ok(_) => {
                                    imported = imported.saturating_add(row_count as u64);
                                }
                                Err(error) => {
                                    tx.rollback().await.ok();
                                    if token.is_cancelled() {
                                        return Err(AppError::cancelled());
                                    }
                                    return Err(error.into());
                                }
                            }
                        }
                        if imported == 0 {
                            tx.rollback().await.ok();
                            return Err(AppError::validation(
                                "Import file contains no data rows.",
                            ));
                        }
                        if token.is_cancelled() {
                            tx.rollback().await.ok();
                            return Err(AppError::cancelled());
                        }
                        tx.commit().await?;
                        Ok(imported)
                    }
                }
            })
            .await;
        self.finish_operation(&operation_id);
        result
    }

    async fn export_snapshot(&self, request: SqlExportRequest) -> AppResult<SqlExportResult> {
        let token = self.operation_token(&request.operation_id);
        let operation_id = request.operation_id.clone();
        let result = self
            .with_timeout(&operation_id, token.clone(), async {
                match self.pool()? {
                    SqlxPool::MySql(pool) => {
                        let _waiter = self.tracker.waiter();
                        let mut connection = pool.acquire().await.map_err(|error| {
                            if matches!(error, sqlx::Error::PoolTimedOut) {
                                self.tracker.timeout();
                            }
                            AppError::from(error)
                        })?;
                        self.start_mysql_session(&mut connection, &operation_id, None).await?;
                        self.record_transaction(&operation_id, true);
                        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                            .execute(&mut *connection)
                            .await?;
                        let mut transaction = connection.begin().await?;
                        let table_names: Vec<String> = if let Some(table) = request.table.clone() {
                            vec![sqlx::query_scalar(
                                "SELECT table_name FROM information_schema.tables \
                                 WHERE table_schema = ? AND table_name = ? \
                                 AND table_type IN ('BASE TABLE','VIEW') LIMIT 1",
                            )
                            .bind(&request.schema)
                            .bind(&table)
                            .fetch_optional(&mut *transaction)
                            .await?
                            .ok_or_else(|| {
                                AppError::validation(format!(
                                    "Table '{}' was not found in schema '{}'.",
                                    table, request.schema
                                ))
                            })?]
                        } else {
                            sqlx::query_scalar(
                                "SELECT table_name FROM information_schema.tables \
                                 WHERE table_schema = ? AND table_type IN ('BASE TABLE','VIEW') \
                                 ORDER BY table_name",
                            )
                            .bind(&request.schema)
                            .fetch_all(&mut *transaction)
                            .await?
                        };
                        if table_names.is_empty() {
                            return Err(AppError::validation(
                                "The selected schema contains no tables or views to export.",
                            ));
                        }
                        let mut writer = SqlSnapshotWriter::new(
                            Protocol::Mysql,
                            &request.format,
                            &request.path,
                            &request.schema,
                            request.table.is_none(),
                        )?;
                        for table in table_names {
                            let sql = format!(
                                "SELECT * FROM {}",
                                qualified_table(&Protocol::Mysql, &request.schema, &table)
                            );
                            let description = (&mut *transaction).describe(&sql).await?;
                            let columns = description
                                .columns()
                                .iter()
                                .map(|column| column.name().to_string())
                                .collect::<Vec<_>>();
                            writer.begin_table(&table, columns)?;
                            let query = sqlx::query(&sql);
                            let mut stream = query.fetch(&mut *transaction);
                            while let Some(row) = stream.try_next().await? {
                                if token.is_cancelled() {
                                    return Err(AppError::cancelled());
                                }
                                let cells = (0..row.columns().len())
                                    .map(|index| mysql_cell_to_json(&row, index))
                                    .collect::<Vec<_>>();
                                writer.write_row(&cells)?;
                            }
                            drop(stream);
                            writer.finish_table()?;
                        }
                        let (rows_exported, tables_exported) = writer.finish()?;
                        transaction.commit().await?;
                        self.record_transaction(&operation_id, false);
                        Ok(SqlExportResult {
                            rows_exported,
                            tables_exported,
                            output_path: request.path.clone(),
                            snapshot_isolated: true,
                        })
                    }
                    SqlxPool::Postgres(pool) => {
                        let _waiter = self.tracker.waiter();
                        let mut connection = pool.acquire().await.map_err(|error| {
                            if matches!(error, sqlx::Error::PoolTimedOut) {
                                self.tracker.timeout();
                            }
                            AppError::from(error)
                        })?;
                        self.start_postgres_session(&mut connection, &operation_id, None).await?;
                        self.record_transaction(&operation_id, true);
                        let mut transaction = connection.begin().await?;
                        sqlx::query(
                            "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY",
                        )
                        .execute(&mut *transaction)
                        .await?;
                        let table_names: Vec<String> = if let Some(table) = request.table.clone() {
                            vec![sqlx::query_scalar(
                                "SELECT table_name FROM information_schema.tables \
                                 WHERE table_schema = $1 AND table_name = $2 \
                                 AND table_type IN ('BASE TABLE','VIEW') LIMIT 1",
                            )
                            .bind(&request.schema)
                            .bind(&table)
                            .fetch_optional(&mut *transaction)
                            .await?
                            .ok_or_else(|| {
                                AppError::validation(format!(
                                    "Table '{}' was not found in schema '{}'.",
                                    table, request.schema
                                ))
                            })?]
                        } else {
                            sqlx::query_scalar(
                                "SELECT table_name FROM information_schema.tables \
                                 WHERE table_schema = $1 AND table_type IN ('BASE TABLE','VIEW') \
                                 ORDER BY table_name",
                            )
                            .bind(&request.schema)
                            .fetch_all(&mut *transaction)
                            .await?
                        };
                        if table_names.is_empty() {
                            return Err(AppError::validation(
                                "The selected schema contains no tables or views to export.",
                            ));
                        }
                        let mut writer = SqlSnapshotWriter::new(
                            Protocol::Postgres,
                            &request.format,
                            &request.path,
                            &request.schema,
                            request.table.is_none(),
                        )?;
                        for table in table_names {
                            let sql = format!(
                                "SELECT * FROM {}",
                                qualified_table(&Protocol::Postgres, &request.schema, &table)
                            );
                            let description = (&mut *transaction).describe(&sql).await?;
                            let columns = description
                                .columns()
                                .iter()
                                .map(|column| column.name().to_string())
                                .collect::<Vec<_>>();
                            writer.begin_table(&table, columns)?;
                            let query = sqlx::query(&sql);
                            let mut stream = query.fetch(&mut *transaction);
                            while let Some(row) = stream.try_next().await? {
                                if token.is_cancelled() {
                                    return Err(AppError::cancelled());
                                }
                                let cells = (0..row.columns().len())
                                    .map(|index| pg_cell_to_json(&row, index))
                                    .collect::<Vec<_>>();
                                writer.write_row(&cells)?;
                            }
                            drop(stream);
                            writer.finish_table()?;
                        }
                        let (rows_exported, tables_exported) = writer.finish()?;
                        transaction.commit().await?;
                        self.record_transaction(&operation_id, false);
                        Ok(SqlExportResult {
                            rows_exported,
                            tables_exported,
                            output_path: request.path.clone(),
                            snapshot_isolated: true,
                        })
                    }
                }
            })
            .await;
        self.finish_operation(&operation_id);
        result
    }

    async fn export_query(&self, request: SqlQueryExportRequest) -> AppResult<SqlExportResult> {
        if !matches!(request.format.as_str(), "csv" | "jsonl") {
            return Err(AppError::validation(
                "Query export only supports csv and jsonl formats.",
            ));
        }
        let pool = self.pool()?;
        let operation_id = request.request.operation_id.clone();
        let token = self.operation_token(&operation_id);
        let result = match pool {
            SqlxPool::MySql(pool) => {
                // Keep the connection outside the timed future: dropping a SQLx transaction
                // only queues its rollback. Cleanup below awaits it before releasing the opId.
                let mut connection = None;
                let result = self
                    .with_timeout(&operation_id, token.clone(), async {
                        let _waiter = self.tracker.waiter();
                        let connection = connection.insert(self.acquire_mysql(pool).await?);
                        self.start_mysql_session(
                            connection,
                            &operation_id,
                            request.request.schema.as_deref(),
                        )
                        .await?;
                        self.record_transaction(&operation_id, true);
                        sqlx::query(
                            "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY",
                        )
                        .execute(&mut **connection)
                        .await?;
                        let mut transaction = connection.begin().await?;
                        let columns = (&mut *transaction)
                            .prepare(&request.request.sql)
                            .await?
                            .columns()
                            .iter()
                            .map(|column| column.name().to_string())
                            .collect::<Vec<_>>();
                        if columns.is_empty() {
                            return Err(AppError::validation(
                                "Query export requires a read-only query returning columns.",
                            ));
                        }
                        let mut writer = SqlSnapshotWriter::new(
                            Protocol::Mysql,
                            &request.format,
                            &request.path,
                            request.request.schema.as_deref().unwrap_or(""),
                            false,
                        )?;
                        writer.begin_table("query_result", columns)?;
                        // Execute the original SQL once; UI page_size and offset are ignored.
                        let mut query = sqlx::query(&request.request.sql);
                        for param in &request.request.params {
                            query = bind_mysql(query, param);
                        }
                        let mut stream = query.fetch(&mut *transaction);
                        while let Some(row) = stream.try_next().await? {
                            if token.is_cancelled() {
                                return Err(AppError::cancelled());
                            }
                            let cells = (0..row.columns().len())
                                .map(|index| mysql_cell_to_json(&row, index))
                                .collect::<Vec<_>>();
                            writer.write_row(&cells)?;
                        }
                        drop(stream);
                        if token.is_cancelled() {
                            return Err(AppError::cancelled());
                        }
                        let (rows_exported, tables_exported) = writer.finish()?;
                        if token.is_cancelled() {
                            return Err(AppError::cancelled());
                        }
                        transaction.commit().await?;
                        self.tracker.acquired();
                        Ok(SqlExportResult {
                            rows_exported,
                            tables_exported,
                            output_path: request.path.clone(),
                            snapshot_isolated: true,
                        })
                    })
                    .await;
                if let Some(mut connection) = connection {
                    if result.is_err() {
                        let rollback = sqlx::query("ROLLBACK").execute(&mut *connection).await;
                        // Never reuse a connection whose rollback failed or whose backend
                        // cancellation may still be in flight.
                        if rollback.is_err() || token.is_cancelled() {
                            let _ = connection.close().await;
                        }
                    }
                }
                self.record_transaction(&operation_id, false);
                result
            }
            SqlxPool::Postgres(pool) => {
                let mut connection = None;
                let result = self
                    .with_timeout(&operation_id, token.clone(), async {
                        let _waiter = self.tracker.waiter();
                        let connection = connection.insert(self.acquire_postgres(pool).await?);
                        self.start_postgres_session(
                            connection,
                            &operation_id,
                            request.request.schema.as_deref(),
                        )
                        .await?;
                        self.record_transaction(&operation_id, true);
                        let mut transaction = connection.begin().await?;
                        sqlx::query(
                            "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY",
                        )
                        .execute(&mut *transaction)
                        .await?;
                        let (columns, types) = {
                            let statement = (&mut *transaction).prepare(&request.request.sql).await?;
                            (
                                statement
                                    .columns()
                                    .iter()
                                    .map(|column| column.name().to_string())
                                    .collect::<Vec<_>>(),
                                pg_statement_param_types(&statement),
                            )
                        };
                        if columns.is_empty() {
                            return Err(AppError::validation(
                                "Query export requires a read-only query returning columns.",
                            ));
                        }
                        // Match the server-inferred wire types, just as ordinary PG fetch does.
                        let binds =
                            pg_binds(&mut transaction, &request.request.params, &types).await?;
                        let mut writer = SqlSnapshotWriter::new(
                            Protocol::Postgres,
                            &request.format,
                            &request.path,
                            request.request.schema.as_deref().unwrap_or(""),
                            false,
                        )?;
                        writer.begin_table("query_result", columns)?;
                        let mut query = sqlx::query(&request.request.sql);
                        for bind in binds {
                            query = bind_postgres(query, bind);
                        }
                        let mut stream = query.fetch(&mut *transaction);
                        while let Some(row) = stream.try_next().await? {
                            if token.is_cancelled() {
                                return Err(AppError::cancelled());
                            }
                            let cells = (0..row.columns().len())
                                .map(|index| pg_cell_to_json(&row, index))
                                .collect::<Vec<_>>();
                            writer.write_row(&cells)?;
                        }
                        drop(stream);
                        if token.is_cancelled() {
                            return Err(AppError::cancelled());
                        }
                        let (rows_exported, tables_exported) = writer.finish()?;
                        if token.is_cancelled() {
                            return Err(AppError::cancelled());
                        }
                        transaction.commit().await?;
                        self.tracker.acquired();
                        Ok(SqlExportResult {
                            rows_exported,
                            tables_exported,
                            output_path: request.path.clone(),
                            snapshot_isolated: true,
                        })
                    })
                    .await;
                if let Some(mut connection) = connection {
                    if result.is_err() {
                        let rollback = sqlx::query("ROLLBACK").execute(&mut *connection).await;
                        if rollback.is_err() || token.is_cancelled() {
                            let _ = connection.close().await;
                        }
                    }
                }
                self.record_transaction(&operation_id, false);
                result
            }
        };
        self.finish_operation(&operation_id);
        result
    }

    async fn fetch(&self, request: SqlRequest) -> AppResult<QueryResult> {
        let token = self.operation_token(&request.operation_id);
        let started = Instant::now();
        let operation_id = request.operation_id.clone();
        let page_size = request.page_size.unwrap_or(500).clamp(1, 10_000);
        let requested_offset = request.offset.unwrap_or(0);
        let paged_sql = paginate_sql(&request.sql, page_size, requested_offset);
        let sql = paged_sql.as_deref().unwrap_or(&request.sql);
        // A server-side page returns at most page_size + 1 rows; otherwise rows before the
        // offset are streamed and skipped client-side.
        let limit = if paged_sql.is_some() {
            page_size as usize + 1
        } else {
            requested_offset.saturating_add(page_size as u64).saturating_add(1) as usize
        };
        let skip = if paged_sql.is_some() { 0 } else { requested_offset.min(usize::MAX as u64) as usize };
        let result = self
            .with_timeout(&operation_id, token, async {
                match self.pool()? {
                    SqlxPool::MySql(pool) => {
                        let _waiter = self.tracker.waiter();
                        let mut connection = self.acquire_mysql(pool).await?;
                        self.start_mysql_session(&mut connection, &operation_id, request.schema.as_deref()).await?;
                        let (columns, rows) = mysql_fetch_rows(&mut connection, sql, &request.params, limit).await?;
                        self.tracker.acquired();
                        Ok(rows_to_result(
                            operation_id.clone(),
                            columns,
                            rows.into_iter().skip(skip).collect(),
                            page_size,
                            requested_offset,
                            started,
                            mysql_cell_to_json,
                        ))
                    }
                    SqlxPool::Postgres(pool) => {
                        let _waiter = self.tracker.waiter();
                        let mut connection = self.acquire_postgres(pool).await?;
                        self.start_postgres_session(&mut connection, &operation_id, request.schema.as_deref()).await?;
                        let (columns, rows) = pg_fetch_rows_retrying(&mut connection, sql, &request.params, limit).await?;
                        self.tracker.acquired();
                        Ok(rows_to_result(
                            operation_id.clone(),
                            columns,
                            rows.into_iter().skip(skip).collect(),
                            page_size,
                            requested_offset,
                            started,
                            pg_cell_to_json,
                        ))
                    }
                }
            })
            .await;
        self.finish_operation(&request.operation_id);
        result
    }

    async fn run_script(&self, request: SqlScriptRequest, transactional: bool) -> AppResult<SqlScriptResult> {
        let token = self.operation_token(&request.operation_id);
        let started = Instant::now();
        let operation_id = request.operation_id.clone();
        let page_size = request.page_size.unwrap_or(500).clamp(1, 10_000);
        let schema = request.statements.first().and_then(|statement| statement.schema.clone());
        let result = self
            .with_timeout(&operation_id, token.clone(), async {
                let mut results = Vec::with_capacity(request.statements.len());
                let mut failure = None;
                let mut committed = false;
                match self.pool()? {
                    SqlxPool::MySql(pool) => {
                        let _waiter = self.tracker.waiter();
                        let mut connection = self.acquire_mysql(pool).await?;
                        self.start_mysql_session(&mut connection, &operation_id, schema.as_deref()).await?;
                        if transactional {
                            self.record_transaction(&operation_id, true);
                            let mut tx = connection.begin().await?;
                            for (index, statement) in request.statements.iter().enumerate() {
                                if token.is_cancelled() {
                                    tx.rollback().await.ok();
                                    return Err(AppError::cancelled());
                                }
                                match mysql_script_step(&mut tx, index, statement, page_size).await {
                                    Ok(step) => results.push(step),
                                    Err(error) => {
                                        failure = Some(script_failure(index, error));
                                        break;
                                    }
                                }
                            }
                            if failure.is_some() || token.is_cancelled() {
                                tx.rollback().await.ok();
                            } else {
                                tx.commit().await?;
                                committed = true;
                            }
                        } else {
                            for (index, statement) in request.statements.iter().enumerate() {
                                if token.is_cancelled() {
                                    return Err(AppError::cancelled());
                                }
                                match mysql_script_step(&mut connection, index, statement, page_size).await {
                                    Ok(step) => results.push(step),
                                    Err(error) => {
                                        failure = Some(script_failure(index, error));
                                        break;
                                    }
                                }
                            }
                            committed = !results.is_empty();
                        }
                    }
                    SqlxPool::Postgres(pool) => {
                        let _waiter = self.tracker.waiter();
                        let mut connection = self.acquire_postgres(pool).await?;
                        self.start_postgres_session(&mut connection, &operation_id, schema.as_deref()).await?;
                        if transactional {
                            self.record_transaction(&operation_id, true);
                            let mut tx = connection.begin().await?;
                            for (index, statement) in request.statements.iter().enumerate() {
                                if token.is_cancelled() {
                                    tx.rollback().await.ok();
                                    return Err(AppError::cancelled());
                                }
                                match pg_script_step(&mut tx, index, statement, page_size).await {
                                    Ok(step) => results.push(step),
                                    Err(error) => {
                                        failure = Some(script_failure(index, error));
                                        break;
                                    }
                                }
                            }
                            if failure.is_some() || token.is_cancelled() {
                                tx.rollback().await.ok();
                            } else {
                                tx.commit().await?;
                                committed = true;
                            }
                            if failure.as_ref().is_some_and(|failure| is_stale_pg_plan(&failure.message)) {
                                let _ = sqlx::Connection::clear_cached_statements(&mut *connection).await;
                            }
                        } else {
                            for (index, statement) in request.statements.iter().enumerate() {
                                if token.is_cancelled() {
                                    return Err(AppError::cancelled());
                                }
                                let step = match pg_script_step(&mut connection, index, statement, page_size).await {
                                    Err(error) if is_stale_pg_plan(&error.message) => {
                                        sqlx::Connection::clear_cached_statements(&mut *connection).await?;
                                        pg_script_step(&mut connection, index, statement, page_size).await
                                    }
                                    other => other,
                                };
                                match step {
                                    Ok(step) => results.push(step),
                                    Err(error) => {
                                        failure = Some(script_failure(index, error));
                                        break;
                                    }
                                }
                            }
                            committed = !results.is_empty();
                        }
                    }
                }
                self.tracker.acquired();
                if token.is_cancelled() {
                    return Err(AppError::cancelled());
                }
                Ok(SqlScriptResult {
                    operation_id: operation_id.clone(),
                    results,
                    transactional,
                    committed,
                    failure,
                    duration_ms: started.elapsed().as_millis() as u64,
                })
            })
            .await;
        self.finish_operation(&operation_id);
        result
    }

    async fn cancel(&self, operation_id: &str) -> AppResult<()> {
        let operation = self.operation_snapshot(operation_id).ok_or_else(|| {
            AppError::validation(format!(
                "Operation '{operation_id}' is not running or has already completed."
            ))
        })?;
        self.cancel_backend_query(&operation).await
    }

    async fn metadata(&self, request: MetadataRequest) -> AppResult<MetadataSnapshot> {
        match self.pool()? {
            SqlxPool::MySql(pool) => {
                mysql_metadata(
                    pool,
                    request.schema.as_deref(),
                    request.table.as_deref(),
                    request.include_tables,
                    request.include_columns,
                )
                .await
            }
            SqlxPool::Postgres(pool) => {
                postgres_metadata(
                    pool,
                    request.schema.as_deref(),
                    request.table.as_deref(),
                    request.include_tables,
                    request.include_columns,
                )
                .await
            }
        }
    }

    async fn ddl(&self, schema: &str, table: &str) -> AppResult<String> {
        match self.pool()? {
            SqlxPool::MySql(pool) => {
                let sql = format!("SHOW CREATE TABLE {}", qualified_table(&Protocol::Mysql, schema, table));
                let row = sqlx::query(&sql).fetch_one(pool).await?;
                let statement = mysql_cell_to_json(&row, 1);
                statement
                    .as_str()
                    .map(|text| format!("{text};"))
                    .ok_or_else(|| AppError::new(ErrorClass::Internal, "SHOW CREATE TABLE returned no statement."))
            }
            SqlxPool::Postgres(pool) => postgres_ddl(pool, schema, table).await,
        }
    }

    async fn object_stats(&self, schema: &str) -> AppResult<Vec<crate::models::ObjectStat>> {
        match self.pool()? {
            SqlxPool::MySql(pool) => mysql_object_stats(pool, schema).await,
            SqlxPool::Postgres(pool) => postgres_object_stats(pool, schema).await,
        }
    }

    async fn metrics(&self) -> AppResult<PoolMetrics> {
        match self.pool()? {
            SqlxPool::MySql(pool) => Ok(self.tracker.metrics(
                pool.size() as u64,
                pool.num_idle() as u64,
                self.max_size,
            )),
            SqlxPool::Postgres(pool) => Ok(self.tracker.metrics(
                pool.size() as u64,
                pool.num_idle() as u64,
                self.max_size,
            )),
        }
    }

    async fn health_check(&self) -> AppResult<()> {
        match self.pool()? {
            SqlxPool::MySql(pool) => {
                sqlx::query("SELECT 1").execute(pool).await?;
            }
            SqlxPool::Postgres(pool) => {
                sqlx::query("SELECT 1").execute(pool).await?;
            }
        }
        Ok(())
    }

    async fn close(&self) -> AppResult<()> {
        let active = self
            .cancellations
            .lock()
            .drain()
            .map(|(_, operation)| operation)
            .collect::<Vec<_>>();
        for operation in active {
            let _ = self.cancel_backend_query(&operation).await;
        }
        match self.pool()? {
            SqlxPool::MySql(pool) => pool.close().await,
            SqlxPool::Postgres(pool) => pool.close().await,
        }
        Ok(())
    }
}

fn paginate_sql(sql: &str, page_size: u32, offset: u64) -> Option<String> {
    let body = sql.trim().trim_end_matches(';').trim_end();
    let keyword = body
        .split_whitespace()
        .next()
        .unwrap_or("")
        .trim_start_matches('(')
        .to_ascii_uppercase();
    if !matches!(keyword.as_str(), "SELECT" | "WITH" | "VALUES") {
        return None;
    }
    if body.to_ascii_uppercase().contains(" FOR UPDATE") {
        return None;
    }
    // MySQL discards ORDER BY inside a derived table that has no LIMIT, so a statement without its
    // own LIMIT gets the page appended directly; only statements that already limit are wrapped.
    let has_limit = body
        .to_ascii_uppercase()
        .split(|c: char| !c.is_ascii_alphanumeric() && c != '_')
        .any(|word| word == "LIMIT" || word == "FETCH");
    if !has_limit && keyword != "VALUES" {
        // A newline keeps the page clause out of a trailing `--` comment.
        return Some(format!(
            "{body}\nLIMIT {} OFFSET {}",
            page_size.saturating_add(1),
            offset
        ));
    }
    Some(format!(
        "SELECT * FROM ({body}\n) AS __luxbase_page LIMIT {} OFFSET {}",
        page_size.saturating_add(1),
        offset
    ))
}

const IDLE_PING_AFTER: Duration = Duration::from_secs(30);

/// Replaces `test_before_acquire`, which pinged on every checkout: only connections that sat
/// idle long enough to be dropped by a firewall or server timeout are verified.
async fn ping_if_idle<C: sqlx::Connection>(
    connection: &mut C,
    meta: PoolConnectionMetadata,
) -> Result<bool, sqlx::Error> {
    if meta.idle_for >= IDLE_PING_AFTER {
        connection.ping().await?;
    }
    Ok(true)
}

fn result_columns<C: Column>(columns: &[C]) -> Vec<ResultColumn> {
    columns
        .iter()
        .map(|column| ResultColumn {
            name: column.name().to_string(),
            type_name: column.type_info().name().to_string(),
            nullable: None,
        })
        .collect()
}

async fn collect_rows<R, S>(mut stream: S, limit: usize) -> AppResult<Vec<R>>
where
    S: futures::Stream<Item = Result<R, sqlx::Error>> + Unpin,
{
    let mut rows = Vec::with_capacity(limit.min(1_024));
    while let Some(row) = stream.try_next().await? {
        rows.push(row);
        if rows.len() >= limit {
            break;
        }
    }
    Ok(rows)
}

/// ER_UNSUPPORTED_PS: statements such as CREATE PROCEDURE cannot use the binary protocol.
fn mysql_unpreparable(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .and_then(|error| error.try_downcast_ref::<sqlx::mysql::MySqlDatabaseError>())
        .is_some_and(|error| error.number() == 1295)
}

async fn mysql_execute(
    connection: &mut MySqlConnection,
    sql: &str,
    params: &[ParamValue],
) -> AppResult<sqlx::mysql::MySqlQueryResult> {
    let mut query = sqlx::query(sql);
    for param in params {
        query = bind_mysql(query, param);
    }
    match query.execute(&mut *connection).await {
        Err(error) if params.is_empty() && mysql_unpreparable(&error) => {
            Ok((&mut *connection).execute(sqlx::raw_sql(sql)).await?)
        }
        other => Ok(other?),
    }
}

/// Prepares through the statement cache (the execution below then reuses it), and prefers the
/// column list of a returned row because a cached statement can predate an ALTER TABLE.
async fn mysql_fetch_rows(
    connection: &mut MySqlConnection,
    sql: &str,
    params: &[ParamValue],
    limit: usize,
) -> AppResult<(Vec<ResultColumn>, Vec<MySqlRow>)> {
    let prepared = match (&mut *connection).prepare(sql).await {
        Ok(statement) => Some(result_columns(statement.columns())),
        Err(error) if params.is_empty() && mysql_unpreparable(&error) => None,
        Err(error) => return Err(error.into()),
    };
    let rows = if prepared.is_some() {
        let mut query = sqlx::query(sql);
        for param in params {
            query = bind_mysql(query, param);
        }
        collect_rows(query.fetch(&mut *connection), limit).await?
    } else {
        collect_rows((&mut *connection).fetch(sqlx::raw_sql(sql)), limit).await?
    };
    let columns = rows
        .first()
        .map(|row| result_columns(row.columns()))
        .or(prepared)
        .unwrap_or_default();
    Ok((columns, rows))
}

fn is_stale_pg_plan(message: &str) -> bool {
    message.contains("cached plan must not change result type")
}

async fn pg_fetch_rows(
    connection: &mut PgConnection,
    sql: &str,
    params: &[ParamValue],
    limit: usize,
) -> AppResult<(Vec<ResultColumn>, Vec<PgRow>)> {
    let (prepared, types) = {
        let statement = (&mut *connection).prepare(sql).await?;
        (result_columns(statement.columns()), pg_statement_param_types(&statement))
    };
    let binds = pg_binds(connection, params, &types).await?;
    let mut query = sqlx::query(sql);
    for bind in binds {
        query = bind_postgres(query, bind);
    }
    let rows = collect_rows(query.fetch(&mut *connection), limit).await?;
    let columns = rows
        .first()
        .map(|row| result_columns(row.columns()))
        .unwrap_or(prepared);
    Ok((columns, rows))
}

/// PostgreSQL rejects a cached `SELECT *` statement once the table's columns change; the
/// connection's statement cache is dropped and the statement prepared again once.
async fn pg_fetch_rows_retrying(
    connection: &mut PgConnection,
    sql: &str,
    params: &[ParamValue],
    limit: usize,
) -> AppResult<(Vec<ResultColumn>, Vec<PgRow>)> {
    match pg_fetch_rows(connection, sql, params, limit).await {
        Err(error) if is_stale_pg_plan(&error.message) => {
            sqlx::Connection::clear_cached_statements(&mut *connection).await?;
            pg_fetch_rows(connection, sql, params, limit).await
        }
        other => other,
    }
}

fn script_failure(index: usize, error: AppError) -> ScriptFailure {
    ScriptFailure {
        index,
        class: error.class,
        message: error.message,
    }
}

fn script_rows<R>(
    index: usize,
    columns: Vec<ResultColumn>,
    rows: Vec<R>,
    page_size: u32,
    started: Instant,
    cell: fn(&R, usize) -> Value,
) -> ScriptStatementResult {
    let has_more = rows.len() > page_size as usize;
    let rows = rows
        .iter()
        .take(page_size as usize)
        .map(|row| (0..columns.len()).map(|column| cell(row, column)).collect())
        .collect::<Vec<_>>();
    ScriptStatementResult {
        index,
        returns_rows: true,
        columns,
        rows,
        has_more,
        page_size,
        rows_affected: 0,
        last_insert_id: None,
        duration_ms: started.elapsed().as_millis() as u64,
    }
}

fn script_affected(
    index: usize,
    rows_affected: u64,
    last_insert_id: Option<i64>,
    page_size: u32,
    started: Instant,
) -> ScriptStatementResult {
    ScriptStatementResult {
        index,
        returns_rows: false,
        columns: Vec::new(),
        rows: Vec::new(),
        has_more: false,
        page_size,
        rows_affected,
        last_insert_id,
        duration_ms: started.elapsed().as_millis() as u64,
    }
}

async fn mysql_script_step(
    connection: &mut MySqlConnection,
    index: usize,
    statement: &SqlRequest,
    page_size: u32,
) -> AppResult<ScriptStatementResult> {
    let started = Instant::now();
    let paged = paginate_sql(&statement.sql, page_size, 0);
    let sql = paged.as_deref().unwrap_or(&statement.sql);
    let returns_rows = match (&mut *connection).prepare(sql).await {
        Ok(prepared) => !prepared.columns().is_empty(),
        Err(error) if statement.params.is_empty() && mysql_unpreparable(&error) => false,
        Err(error) => return Err(error.into()),
    };
    if returns_rows {
        let (columns, rows) =
            mysql_fetch_rows(connection, sql, &statement.params, page_size as usize + 1).await?;
        return Ok(script_rows(index, columns, rows, page_size, started, mysql_cell_to_json));
    }
    let done = mysql_execute(connection, &statement.sql, &statement.params).await?;
    let last_insert_id = Some(done.last_insert_id() as i64).filter(|id| *id > 0);
    Ok(script_affected(index, done.rows_affected(), last_insert_id, page_size, started))
}

async fn pg_script_step(
    connection: &mut PgConnection,
    index: usize,
    statement: &SqlRequest,
    page_size: u32,
) -> AppResult<ScriptStatementResult> {
    let started = Instant::now();
    let paged = paginate_sql(&statement.sql, page_size, 0);
    let sql = paged.as_deref().unwrap_or(&statement.sql);
    let returns_rows = !(&mut *connection).prepare(sql).await?.columns().is_empty();
    if returns_rows {
        let (columns, rows) =
            pg_fetch_rows(connection, sql, &statement.params, page_size as usize + 1).await?;
        return Ok(script_rows(index, columns, rows, page_size, started, pg_cell_to_json));
    }
    let binds = coerce_pg_params(&mut *connection, &statement.sql, &statement.params).await?;
    let mut query = sqlx::query(&statement.sql);
    for bind in binds {
        query = bind_postgres(query, bind);
    }
    let done = query.execute(&mut *connection).await?;
    Ok(script_affected(index, done.rows_affected(), None, page_size, started))
}

fn rows_to_result<R>(
    operation_id: String,
    columns: Vec<ResultColumn>,
    rows: Vec<R>,
    page_size: u32,
    offset: u64,
    started: Instant,
    cell: fn(&R, usize) -> Value,
) -> QueryResult {
    let has_more = rows.len() > page_size as usize;
    let limited = rows
        .into_iter()
        .take(page_size as usize)
        .map(|row| {
            (0..columns.len())
                .map(|index| cell(&row, index))
                .collect::<Vec<_>>()
        })
        .collect();
    QueryResult {
        operation_id,
        columns,
        rows: limited,
        rows_affected: 0,
        page: ResultPage {
            offset,
            limit: page_size,
            has_more,
            streamed: true,
        },
        duration_ms: started.elapsed().as_millis() as u64,
    }
}

macro_rules! try_cell {
    ($row:expr, $index:expr, $ty:ty, $map:expr) => {
        if let Ok(value) = $row.try_get::<Option<$ty>, _>($index) {
            return value.map($map).unwrap_or(Value::Null);
        }
    };
}

fn text_cell<T: ToString>(value: T) -> Value {
    Value::String(value.to_string())
}

fn bytes_cell(bytes: Vec<u8>) -> Value {
    json!({ "bytesBase64": B64.encode(bytes) })
}

fn float_cell(value: f32) -> Value {
    // Format through the f32 shortest representation so 0.1 does not become 0.10000000149.
    value
        .to_string()
        .parse::<f64>()
        .ok()
        .and_then(serde_json::Number::from_f64)
        .map(Value::Number)
        .unwrap_or(Value::Null)
}

fn unsupported_cell(type_name: &str) -> Value {
    json!({ "unsupportedCellType": type_name })
}

fn mysql_cell_to_json(row: &MySqlRow, index: usize) -> Value {
    try_cell!(row, index, String, Value::String);
    try_cell!(row, index, i64, super::integer_cell);
    try_cell!(row, index, u64, super::integer_cell);
    try_cell!(row, index, f64, Value::from);
    try_cell!(row, index, f32, float_cell);
    try_cell!(row, index, bool, Value::Bool);
    try_cell!(row, index, BigDecimal, text_cell);
    try_cell!(row, index, NaiveDateTime, text_cell);
    try_cell!(row, index, NaiveDate, text_cell);
    try_cell!(row, index, NaiveTime, text_cell);
    try_cell!(row, index, Vec<u8>, bytes_cell);
    try_cell!(row, index, Value, text_cell);
    // Binary-collation text, SET, DECIMAL variants and similar arrive as length-encoded strings.
    if let Ok(value) = row.try_get_unchecked::<Option<String>, _>(index) {
        return value.map(Value::String).unwrap_or(Value::Null);
    }
    if let Ok(value) = row.try_get_unchecked::<Option<Vec<u8>>, _>(index) {
        return value.map(bytes_cell).unwrap_or(Value::Null);
    }
    unsupported_cell(row.columns().get(index).map(|column| column.type_info().name()).unwrap_or("unknown"))
}

/// Render PostgreSQL's binary NUMERIC (base-10000 digit groups plus display scale) exactly as the
/// server would print it; BigDecimal decoding drops the declared scale (12.50 becomes 12.5000).
fn pg_numeric_text(bytes: &[u8]) -> Option<String> {
    let word = |offset: usize| -> Option<u16> {
        Some(u16::from_be_bytes([*bytes.get(offset)?, *bytes.get(offset + 1)?]))
    };
    let ndigits = word(0)? as usize;
    let weight = word(2)? as i16 as i32;
    let sign = word(4)?;
    let dscale = word(6)? as usize;
    match sign {
        0xC000 => return Some("NaN".into()),
        0xD000 => return Some("Infinity".into()),
        0xF000 => return Some("-Infinity".into()),
        _ => {}
    }
    let digits = (0..ndigits).map(|index| word(8 + index * 2)).collect::<Option<Vec<_>>>()?;
    let group = |index: i32| -> u16 {
        usize::try_from(index)
            .ok()
            .and_then(|index| digits.get(index).copied())
            .unwrap_or(0)
    };
    let mut text = String::new();
    if sign == 0x4000 {
        text.push('-');
    }
    if weight < 0 {
        text.push('0');
    } else {
        for index in 0..=weight {
            if index == 0 {
                text.push_str(&group(index).to_string());
            } else {
                text.push_str(&format!("{:04}", group(index)));
            }
        }
    }
    if dscale > 0 {
        let mut fraction = String::new();
        let mut index = weight + 1;
        while fraction.len() < dscale {
            fraction.push_str(&format!("{:04}", group(index)));
            index += 1;
        }
        fraction.truncate(dscale);
        text.push('.');
        text.push_str(&fraction);
    }
    Some(text)
}

fn pg_cell_to_json(row: &PgRow, index: usize) -> Value {
    let Some(column) = row.columns().get(index) else {
        return Value::Null;
    };
    if column.type_info().name() == "NUMERIC" {
        use sqlx::ValueRef;
        if let Ok(raw) = row.try_get_raw(index) {
            if raw.is_null() {
                return Value::Null;
            }
            if let Some(text) = raw.as_bytes().ok().and_then(pg_numeric_text) {
                return Value::String(text);
            }
        }
    }
    try_cell!(row, index, String, Value::String);
    try_cell!(row, index, i64, super::integer_cell);
    try_cell!(row, index, i32, Value::from);
    try_cell!(row, index, i16, Value::from);
    try_cell!(row, index, f64, Value::from);
    try_cell!(row, index, f32, float_cell);
    try_cell!(row, index, bool, Value::Bool);
    try_cell!(row, index, BigDecimal, text_cell);
    try_cell!(row, index, DateTime<Utc>, |value: DateTime<Utc>| Value::String(value.to_rfc3339()));
    try_cell!(row, index, NaiveDateTime, text_cell);
    try_cell!(row, index, NaiveDate, text_cell);
    try_cell!(row, index, NaiveTime, text_cell);
    try_cell!(row, index, Uuid, text_cell);
    try_cell!(row, index, Value, text_cell);
    try_cell!(row, index, Oid, |value: Oid| Value::from(value.0));
    try_cell!(row, index, Vec<u8>, bytes_cell);
    try_cell!(row, index, Vec<String>, |values: Vec<String>| json!(values));
    try_cell!(row, index, Vec<i64>, |values: Vec<i64>| json!(values));
    try_cell!(row, index, Vec<i32>, |values: Vec<i32>| json!(values));
    try_cell!(row, index, Vec<bool>, |values: Vec<bool>| json!(values));
    let type_info = column.type_info();
    if matches!(type_info.kind(), sqlx::postgres::PgTypeKind::Enum(_)) {
        if let Ok(value) = row.try_get_unchecked::<Option<String>, _>(index) {
            return value.map(Value::String).unwrap_or(Value::Null);
        }
    }
    unsupported_cell(type_info.name())
}

macro_rules! bind_mysql_param {
    ($query:expr, $param:expr) => {
        match $param {
            ParamValue::Null => $query.bind(Option::<String>::None),
            ParamValue::Bool(value) => $query.bind(*value),
            ParamValue::I64(value) => $query.bind(*value),
            ParamValue::U64(value) => $query.bind(*value),
            ParamValue::F64(value) => $query.bind(*value),
            ParamValue::Text(value) => $query.bind(value.clone()),
            ParamValue::Bytes(value) => $query.bind(value.clone()),
            ParamValue::Json(value) => $query.bind(value.to_string()),
        }
    };
}

fn bind_mysql<'q>(
    query: sqlx::query::Query<'q, MySql, sqlx::mysql::MySqlArguments>,
    param: &'q ParamValue,
) -> sqlx::query::Query<'q, MySql, sqlx::mysql::MySqlArguments> {
    bind_mysql_param!(query, param)
}

/// A PostgreSQL bind value whose wire type matches what the server inferred for `$n`.
///
/// sqlx caches the statement prepared by `describe`, so the server-inferred parameter types are
/// what the Bind message is decoded against. Every value must therefore be converted to exactly
/// that type; sending an INT8 where the server inferred INT4 is rejected as malformed binary data.
enum PgBind {
    Null,
    Bool(bool),
    I16(i16),
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
    Numeric(BigDecimal),
    Text(String),
    Bytes(Vec<u8>),
    Json(Value),
    Timestamp(NaiveDateTime),
    TimestampTz(DateTime<Utc>),
    /// A timestamp typed without an offset; the server converts it using the session TimeZone.
    LocalTimestampTz(String),
    Date(NaiveDate),
    Time(NaiveTime),
    Uuid(Uuid),
}

struct PgParamType {
    name: String,
    textual: bool,
}

fn pg_param_type(type_info: &sqlx::postgres::PgTypeInfo) -> PgParamType {
    use sqlx::postgres::PgTypeKind;
    match type_info.kind() {
        PgTypeKind::Domain(base) => pg_param_type(base),
        // Enum values use their label text as the binary representation.
        PgTypeKind::Enum(_) => PgParamType {
            name: type_info.name().to_ascii_uppercase(),
            textual: true,
        },
        _ => {
            let name = type_info.name().to_ascii_uppercase();
            let textual = matches!(
                name.as_str(),
                "TEXT" | "VARCHAR" | "CHAR" | "BPCHAR" | "NAME" | "CITEXT" | "UNKNOWN" | "\"CHAR\""
            );
            PgParamType { name, textual }
        }
    }
}

fn parse_naive_datetime(text: &str) -> Option<NaiveDateTime> {
    const FORMATS: [&str; 4] = [
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%dT%H:%M",
    ];
    FORMATS
        .iter()
        .find_map(|format| NaiveDateTime::parse_from_str(text, format).ok())
        .or_else(|| {
            NaiveDate::parse_from_str(text, "%Y-%m-%d")
                .ok()
                .and_then(|date| date.and_hms_opt(0, 0, 0))
        })
}

fn pg_invalid(position: usize, expected: &str, shown: &str) -> AppError {
    AppError::validation(format!(
        "Parameter ${position} must be {expected} for this statement; \"{shown}\" could not be converted."
    ))
}

fn pg_text_param(text: &str, type_name: &str, position: usize) -> AppResult<PgBind> {
    let value = text.trim();
    let invalid = |expected: &str| pg_invalid(position, expected, text);
    Ok(match type_name {
        "INT2" => PgBind::I16(value.parse().map_err(|_| invalid("a SMALLINT"))?),
        "INT4" => PgBind::I32(value.parse().map_err(|_| invalid("an INTEGER"))?),
        "INT8" => PgBind::I64(value.parse().map_err(|_| invalid("a BIGINT"))?),
        "FLOAT4" => PgBind::F32(value.parse().map_err(|_| invalid("a REAL"))?),
        "FLOAT8" => PgBind::F64(value.parse().map_err(|_| invalid("a DOUBLE PRECISION"))?),
        "NUMERIC" => PgBind::Numeric(value.parse::<BigDecimal>().map_err(|_| invalid("a NUMERIC"))?),
        "BOOL" => PgBind::Bool(match value.to_ascii_lowercase().as_str() {
            "true" | "t" | "1" | "yes" | "y" | "on" => true,
            "false" | "f" | "0" | "no" | "n" | "off" => false,
            _ => return Err(invalid("a BOOLEAN")),
        }),
        "DATE" => PgBind::Date(
            NaiveDate::parse_from_str(value, "%Y-%m-%d").map_err(|_| invalid("a DATE (YYYY-MM-DD)"))?,
        ),
        "TIME" => PgBind::Time(
            NaiveTime::parse_from_str(value, "%H:%M:%S%.f")
                .or_else(|_| NaiveTime::parse_from_str(value, "%H:%M"))
                .map_err(|_| invalid("a TIME (HH:MM[:SS])"))?,
        ),
        "TIMESTAMP" => PgBind::Timestamp(
            parse_naive_datetime(value).ok_or_else(|| invalid("a TIMESTAMP (YYYY-MM-DD HH:MM:SS)"))?,
        ),
        "TIMESTAMPTZ" => match DateTime::parse_from_rfc3339(value) {
            Ok(value) => PgBind::TimestampTz(value.with_timezone(&Utc)),
            Err(_) if parse_naive_datetime(value).is_some() => PgBind::LocalTimestampTz(value.to_string()),
            Err(_) => return Err(invalid("a TIMESTAMPTZ")),
        },
        "UUID" => PgBind::Uuid(Uuid::parse_str(value).map_err(|_| invalid("a UUID"))?),
        "JSONB" => PgBind::Json(serde_json::from_str(text).map_err(|_| invalid("valid JSON"))?),
        "JSON" => {
            serde_json::from_str::<Value>(text).map_err(|_| invalid("valid JSON"))?;
            PgBind::Text(text.to_string())
        }
        "BYTEA" => PgBind::Bytes(text.as_bytes().to_vec()),
        other => {
            return Err(AppError::validation(format!(
                "Parameter ${position} targets PostgreSQL type {other}, which cannot be bound from text directly. \
                 Cast it in SQL instead, for example ${position}::text::{}.",
                other.to_ascii_lowercase()
            )))
        }
    })
}

fn pg_number_param(text: String, as_f64: f64, integral: Option<i128>, type_name: &str, position: usize) -> AppResult<PgBind> {
    let out_of_range = |expected: &str| pg_invalid(position, expected, &text);
    Ok(match type_name {
        "INT2" => PgBind::I16(integral.and_then(|value| i16::try_from(value).ok()).ok_or_else(|| out_of_range("a SMALLINT"))?),
        "INT4" => PgBind::I32(integral.and_then(|value| i32::try_from(value).ok()).ok_or_else(|| out_of_range("an INTEGER"))?),
        "INT8" => PgBind::I64(integral.and_then(|value| i64::try_from(value).ok()).ok_or_else(|| out_of_range("a BIGINT"))?),
        "FLOAT4" => PgBind::F32(as_f64 as f32),
        "FLOAT8" => PgBind::F64(as_f64),
        "NUMERIC" => PgBind::Numeric(text.parse::<BigDecimal>().map_err(|_| out_of_range("a NUMERIC"))?),
        "JSONB" => PgBind::Json(serde_json::from_str(&text).map_err(|_| out_of_range("valid JSON"))?),
        "JSON" => PgBind::Text(text),
        other => pg_text_param(&text, other, position)?,
    })
}

fn pg_param(param: &ParamValue, target: Option<&PgParamType>, position: usize) -> AppResult<PgBind> {
    let Some(target) = target else {
        return Ok(match param {
            ParamValue::Null => PgBind::Null,
            ParamValue::Bool(value) => PgBind::Bool(*value),
            ParamValue::I64(value) => PgBind::I64(*value),
            ParamValue::U64(value) => match i64::try_from(*value) {
                Ok(value) => PgBind::I64(value),
                Err(_) => PgBind::Numeric(BigDecimal::from(*value)),
            },
            ParamValue::F64(value) => PgBind::F64(*value),
            ParamValue::Text(value) => PgBind::Text(value.clone()),
            ParamValue::Bytes(value) => PgBind::Bytes(value.clone()),
            ParamValue::Json(value) => PgBind::Json(value.clone()),
        });
    };
    let name = target.name.as_str();
    if target.textual {
        return Ok(match param {
            ParamValue::Null => PgBind::Null,
            ParamValue::Text(value) => PgBind::Text(value.clone()),
            ParamValue::Bool(value) => PgBind::Text(value.to_string()),
            ParamValue::I64(value) => PgBind::Text(value.to_string()),
            ParamValue::U64(value) => PgBind::Text(value.to_string()),
            ParamValue::F64(value) => PgBind::Text(value.to_string()),
            ParamValue::Json(Value::String(value)) => PgBind::Text(value.clone()),
            ParamValue::Json(value) => PgBind::Text(value.to_string()),
            ParamValue::Bytes(value) => PgBind::Text(String::from_utf8_lossy(value).into_owned()),
        });
    }
    match param {
        ParamValue::Null => Ok(PgBind::Null),
        ParamValue::Text(text) => pg_text_param(text, name, position),
        ParamValue::Bool(value) => match name {
            "BOOL" => Ok(PgBind::Bool(*value)),
            _ => pg_number_param(u8::from(*value).to_string(), f64::from(u8::from(*value)), Some(i128::from(u8::from(*value))), name, position),
        },
        ParamValue::I64(value) => pg_number_param(value.to_string(), *value as f64, Some(i128::from(*value)), name, position),
        ParamValue::U64(value) => pg_number_param(value.to_string(), *value as f64, Some(i128::from(*value)), name, position),
        ParamValue::F64(value) => {
            let integral = (value.fract() == 0.0 && value.is_finite()).then(|| *value as i128);
            pg_number_param(value.to_string(), *value, integral, name, position)
        }
        ParamValue::Json(value) => match (name, value) {
            ("JSONB", _) => Ok(PgBind::Json(value.clone())),
            ("JSON", _) => Ok(PgBind::Text(value.to_string())),
            (_, Value::String(text)) => pg_text_param(text, name, position),
            (_, other) => pg_text_param(&other.to_string(), name, position),
        },
        ParamValue::Bytes(value) => match name {
            "BYTEA" => Ok(PgBind::Bytes(value.clone())),
            other => Err(AppError::validation(format!(
                "Parameter ${position} is binary data but the statement expects {other}."
            ))),
        },
    }
}

fn pg_statement_param_types(statement: &sqlx::postgres::PgStatement<'_>) -> Vec<PgParamType> {
    match statement.parameters() {
        Some(Either::Left(types)) => types.iter().map(pg_param_type).collect(),
        _ => Vec::new(),
    }
}

async fn pg_binds(
    connection: &mut sqlx::postgres::PgConnection,
    params: &[ParamValue],
    types: &[PgParamType],
) -> AppResult<Vec<PgBind>> {
    let mut binds = Vec::with_capacity(params.len());
    for (index, param) in params.iter().enumerate() {
        let bind = match pg_param(param, types.get(index), index + 1)? {
            PgBind::LocalTimestampTz(text) => {
                let converted: DateTime<Utc> = sqlx::query_scalar("SELECT $1::text::timestamptz")
                    .bind(&text)
                    .fetch_one(&mut *connection)
                    .await
                    .map_err(|_| pg_invalid(index + 1, "a TIMESTAMPTZ", &text))?;
                PgBind::TimestampTz(converted)
            }
            bind => bind,
        };
        binds.push(bind);
    }
    Ok(binds)
}

async fn coerce_pg_params(
    connection: &mut sqlx::postgres::PgConnection,
    sql: &str,
    params: &[ParamValue],
) -> AppResult<Vec<PgBind>> {
    if params.is_empty() {
        return Ok(Vec::new());
    }
    let types = pg_statement_param_types(&(&mut *connection).prepare(sql).await?);
    pg_binds(connection, params, &types).await
}

fn bind_postgres<'q>(
    query: sqlx::query::Query<'q, Postgres, sqlx::postgres::PgArguments>,
    param: PgBind,
) -> sqlx::query::Query<'q, Postgres, sqlx::postgres::PgArguments> {
    match param {
        PgBind::Null => query.bind(None::<String>),
        PgBind::Bool(value) => query.bind(value),
        PgBind::I16(value) => query.bind(value),
        PgBind::I32(value) => query.bind(value),
        PgBind::I64(value) => query.bind(value),
        PgBind::F32(value) => query.bind(value),
        PgBind::F64(value) => query.bind(value),
        PgBind::Numeric(value) => query.bind(value),
        PgBind::Text(value) => query.bind(value),
        PgBind::Bytes(value) => query.bind(value),
        PgBind::Json(value) => query.bind(value),
        PgBind::Timestamp(value) => query.bind(value),
        PgBind::TimestampTz(value) => query.bind(value),
        PgBind::LocalTimestampTz(value) => query.bind(value),
        PgBind::Date(value) => query.bind(value),
        PgBind::Time(value) => query.bind(value),
        PgBind::Uuid(value) => query.bind(value),
    }
}


use super::export::{qualified_table, SqlSnapshotWriter};
use super::import::SqlImportSource;
use super::r#trait::SqlDriver;
use super::tracker::PoolTracker;
use crate::error::{AppError, AppResult};
use crate::models::{
    ColumnMetadata, ConnectionConfig, DatabaseMetadata, ExecuteResult, ForeignKeyMetadata,
    IndexMetadata, MetadataRequest, MetadataSnapshot, ParamValue, PoolMetrics, Protocol, QueryResult,
    ResultColumn, ResultPage, ScriptFailure, ScriptStatementResult, SqlBatchRequest,
    SqlExportRequest, SqlExportResult, SqlImportRequest, SqlQueryExportRequest, SqlRequest,
    SqlScriptRequest, SqlScriptResult, TableMetadata,
};
use async_trait::async_trait;
use base64::Engine;
use parking_lot::Mutex;
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::types::{Value as SqlValue, ValueRef};
use rusqlite::{
    params_from_iter, Connection as RusqliteConnection, OptionalExtension,
    TransactionBehavior,
};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::path::Path;
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

pub struct SqliteDriver {
    connection_id: String,
    pool: Option<Pool<SqliteConnectionManager>>,
    tracker: PoolTracker,
    cancellations: Mutex<HashMap<String, CancellationToken>>,
    query_timeout: Duration,
    max_size: u32,
}

impl SqliteDriver {
    pub fn new(connection_id: String) -> Self {
        Self {
            connection_id,
            pool: None,
            tracker: PoolTracker::default(),
            cancellations: Mutex::new(HashMap::new()),
            query_timeout: Duration::from_secs(60),
            max_size: 20,
        }
    }

    fn pool(&self) -> AppResult<Pool<SqliteConnectionManager>> {
        self.pool
            .clone()
            .ok_or_else(|| AppError::connection("The SQLite connection pool is not initialized."))
    }

    fn operation_token(&self, operation_id: &str) -> CancellationToken {
        let token = CancellationToken::new();
        self.cancellations
            .lock()
            .insert(operation_id.to_string(), token.clone());
        token
    }

    fn finish_operation(&self, operation_id: &str) {
        self.cancellations.lock().remove(operation_id);
    }

    async fn run_with_timeout<T, F>(&self, token: CancellationToken, work: F) -> AppResult<T>
    where
        T: Send + 'static,
        F: FnOnce(CancellationToken) -> AppResult<T> + Send + 'static,
    {
        let work_token = token.clone();
        let mut task = tokio::task::spawn_blocking(move || work(work_token));
        tokio::select! {
            biased;
            _ = token.cancelled() => {
                // A blocking worker cannot be aborted. Waiting for the SQLite
                // progress handler to observe the token guarantees that a
                // timed-out transaction has rolled back before we release the
                // application-level operation slot.
                let _ = task.await;
                Err(AppError::cancelled())
            }
            result = tokio::time::timeout(self.query_timeout, &mut task) => {
                match result {
                    Ok(Ok(result)) => result,
                    Ok(Err(error)) => Err(AppError::new(
                        crate::models::ErrorClass::Internal,
                        format!("SQLite worker failed: {error}"),
                    )),
                    Err(_) => {
                        // The token is still owned here; cancelling it makes
                        // the connection progress handler stop the statement.
                        token.cancel();
                        let _ = task.await;
                        self.tracker.timeout();
                        Err(AppError::timeout(
                            "The SQLite operation exceeded its configured timeout and was interrupted.",
                        ))
                    }
                }
            }
        }
    }
}

#[async_trait]
impl SqlDriver for SqliteDriver {
    fn connection_id(&self) -> &str {
        &self.connection_id
    }

    async fn connect(
        &mut self,
        config: &ConnectionConfig,
        _password: Option<String>,
    ) -> AppResult<()> {
        let path = config
            .file_path
            .clone()
            .ok_or_else(|| AppError::validation("SQLite database file is required."))?;
        if !Path::new(&path).exists() {
            return Err(AppError::connection(format!(
                "SQLite database file does not exist: {path}"
            )));
        }
        self.query_timeout = Duration::from_millis(config.query_timeout_ms);
        self.max_size = config.pool.max_size;
        let manager = SqliteConnectionManager::file(path).with_init(|connection| {
            connection.busy_timeout(Duration::from_secs(5))?;
            connection.pragma_update(None, "foreign_keys", "ON")?;
            connection.pragma_update(None, "journal_mode", "WAL")?;
            Ok(())
        });
        let pool = r2d2::Builder::new()
            .min_idle(Some(config.pool.min_idle))
            .max_size(config.pool.max_size)
            .connection_timeout(Duration::from_millis(config.pool.connection_timeout_ms))
            .idle_timeout(Some(Duration::from_millis(config.pool.idle_timeout_ms)))
            .max_lifetime(Some(Duration::from_millis(config.pool.max_lifetime_ms)))
            .build(manager)?;
        let check = pool.get()?;
        check.query_row("SELECT 1", [], |_| Ok(()))?;
        self.pool = Some(pool);
        self.tracker.acquired();
        Ok(())
    }

    async fn execute(&self, request: SqlRequest) -> AppResult<ExecuteResult> {
        let token = self.operation_token(&request.operation_id);
        let pool = self.pool()?;
        let tracker = self.tracker.clone();
        let operation_id = request.operation_id.clone();
        let started = Instant::now();
        let sql = request.sql;
        let params = request.params;
        let result = self
            .run_with_timeout(token, move |token| {
                let _waiter = tracker.waiter();
                let conn = pool.get().map_err(|error| {
                    tracker.timeout();
                    AppError::from(error)
                })?;
                conn.progress_handler(1_000, Some(move || token.is_cancelled()));
                let values = params.into_iter().map(sql_value).collect::<Vec<_>>();
                let mut statement = conn.prepare(&sql)?;
                let rows_affected = statement.execute(params_from_iter(values.iter()))?;
                let last_insert_id = conn.last_insert_rowid();
                Ok(ExecuteResult {
                    operation_id,
                    rows_affected: rows_affected as u64,
                    last_insert_id: Some(last_insert_id),
                    duration_ms: started.elapsed().as_millis() as u64,
                    committed: true,
                })
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
        let pool = self.pool()?;
        let tracker = self.tracker.clone();
        let operation_id = request.operation_id.clone();
        let statements = request.statements;
        let started = Instant::now();
        let result = self
            .run_with_timeout(token, move |token| {
                let _waiter = tracker.waiter();
                let mut conn = pool.get().map_err(|error| {
                    tracker.timeout();
                    AppError::from(error)
                })?;
                let callback_token = token.clone();
                conn.progress_handler(1_000, Some(move || callback_token.is_cancelled()));
                let transaction = conn.transaction()?;
                let mut results = Vec::with_capacity(statements.len());
                for statement_request in statements {
                    if token.is_cancelled() {
                        transaction.rollback()?;
                        return Err(AppError::cancelled());
                    }
                    let values = statement_request
                        .params
                        .into_iter()
                        .map(sql_value)
                        .collect::<Vec<_>>();
                    match transaction.execute(&statement_request.sql, params_from_iter(values.iter())) {
                        Ok(rows_affected) => results.push(ExecuteResult {
                            operation_id: statement_request.operation_id,
                            rows_affected: rows_affected as u64,
                            last_insert_id: Some(transaction.last_insert_rowid()),
                            duration_ms: started.elapsed().as_millis() as u64,
                            committed: false,
                        }),
                        Err(error) => {
                            transaction.rollback()?;
                            return Err(AppError::from(error));
                        }
                    }
                }
                if token.is_cancelled() {
                    transaction.rollback()?;
                    return Err(AppError::cancelled());
                }
                transaction.commit()?;
                for result in &mut results {
                    result.committed = true;
                }
                Ok(results)
            })
            .await;
        self.finish_operation(&operation_id);
        result
    }

    async fn transaction(&self, request: SqlBatchRequest) -> AppResult<Vec<ExecuteResult>> {
        self.execute_batch(request).await
    }

    async fn stream_import(&self, request: SqlImportRequest) -> AppResult<u64> {
        let operation_id = request.operation_id.clone();
        let token = self.operation_token(&operation_id);
        let pool = self.pool()?;
        let tracker = self.tracker.clone();
        let result = self
            .run_with_timeout(token, move |token| {
                let _waiter = tracker.waiter();
                let mut conn = pool.get().map_err(|error| {
                    tracker.timeout();
                    AppError::from(error)
                })?;
                let callback_token = token.clone();
                conn.progress_handler(1_000, Some(move || callback_token.is_cancelled()));
                let mut source = SqlImportSource::open(
                    Protocol::Sqlite,
                    &request.path,
                    &request.format,
                    request.schema.as_deref(),
                    &request.table,
                )?;
                let batch_size = source.safe_batch_size(request.batch_size);
                let transaction = conn.transaction()?;
                let mut imported = 0u64;
                loop {
                    if token.is_cancelled() {
                        transaction.rollback()?;
                        return Err(AppError::cancelled());
                    }
                    let rows = source.next_batch(batch_size)?;
                    if rows.is_empty() {
                        break;
                    }
                    let row_count = rows.len();
                    let sql = source.insert_sql(row_count)?;
                    let values = rows
                        .into_iter()
                        .flatten()
                        .map(sql_value)
                        .collect::<Vec<_>>();
                    transaction.execute(&sql, params_from_iter(values.iter()))?;
                    imported = imported.saturating_add(row_count as u64);
                }
                if imported == 0 {
                    transaction.rollback()?;
                    return Err(AppError::validation("Import file contains no data rows."));
                }
                if token.is_cancelled() {
                    transaction.rollback()?;
                    return Err(AppError::cancelled());
                }
                transaction.commit()?;
                Ok(imported)
            })
            .await;
        self.finish_operation(&operation_id);
        result
    }

    async fn export_snapshot(&self, request: SqlExportRequest) -> AppResult<SqlExportResult> {
        let operation_id = request.operation_id.clone();
        let token = self.operation_token(&operation_id);
        let pool = self.pool()?;
        let tracker = self.tracker.clone();
        let result = self
            .run_with_timeout(token, move |token| {
                let _waiter = tracker.waiter();
                let mut connection = pool.get().map_err(|error| {
                    tracker.timeout();
                    AppError::from(error)
                })?;
                let callback_token = token.clone();
                connection.progress_handler(1_000, Some(move || callback_token.is_cancelled()));
                let transaction =
                    connection.transaction_with_behavior(TransactionBehavior::Deferred)?;
                let table_names: Vec<String> = if let Some(table) = request.table.clone() {
                    let exists = {
                        let mut statement = transaction.prepare(
                            "SELECT name FROM sqlite_schema \
                             WHERE type IN ('table','view') AND name NOT LIKE 'sqlite_%' \
                             AND name = ?1 LIMIT 1",
                        )?;
                        statement
                            .query_row([table.as_str()], |row| row.get::<_, String>(0))
                            .optional()?
                    };
                    vec![exists.ok_or_else(|| {
                        AppError::validation(format!(
                            "Table '{table}' was not found in the SQLite schema."
                        ))
                    })?]
                } else {
                    let mut statement = transaction.prepare(
                        "SELECT name FROM sqlite_schema \
                         WHERE type IN ('table','view') AND name NOT LIKE 'sqlite_%' \
                         ORDER BY name",
                    )?;
                    let names = statement
                        .query_map([], |row| row.get::<_, String>(0))?
                        .collect::<Result<Vec<_>, _>>()?;
                    names
                };
                if table_names.is_empty() {
                    return Err(AppError::validation(
                        "The SQLite database contains no tables or views to export.",
                    ));
                }
                let mut writer = SqlSnapshotWriter::new(
                    Protocol::Sqlite,
                    &request.format,
                    &request.path,
                    &request.schema,
                    request.table.is_none(),
                )?;
                for table in table_names {
                    let sql = format!(
                        "SELECT * FROM {}",
                        qualified_table(&Protocol::Sqlite, &request.schema, &table)
                    );
                    let mut statement = transaction.prepare(&sql)?;
                    let columns = statement
                        .columns()
                        .iter()
                        .map(|column| column.name().to_string())
                        .collect::<Vec<_>>();
                    writer.begin_table(&table, columns)?;
                    let column_count = statement.column_count();
                    let mut rows = statement.query([])?;
                    while let Some(row) = rows.next()? {
                        if token.is_cancelled() {
                            return Err(AppError::cancelled());
                        }
                        let cells = (0..column_count)
                            .map(|index| row.get_ref(index).map(sqlite_value_to_json))
                            .collect::<Result<Vec<_>, _>>()?;
                        writer.write_row(&cells)?;
                    }
                    drop(rows);
                    drop(statement);
                    writer.finish_table()?;
                }
                let (rows_exported, tables_exported) = writer.finish()?;
                transaction.commit()?;
                Ok(SqlExportResult {
                    rows_exported,
                    tables_exported,
                    output_path: request.path.clone(),
                    snapshot_isolated: true,
                })
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
        let tracker = self.tracker.clone();
        let result = self
            .run_with_timeout(token, move |token| {
                let _waiter = tracker.waiter();
                let mut connection = pool.get().map_err(|error| {
                    tracker.timeout();
                    AppError::from(error)
                })?;
                let callback_token = token.clone();
                connection.progress_handler(1_000, Some(move || callback_token.is_cancelled()));
                let result = (|| {
                    if token.is_cancelled() {
                        return Err(AppError::cancelled());
                    }
                    let transaction =
                        connection.transaction_with_behavior(TransactionBehavior::Deferred)?;
                    let exported = (|| -> AppResult<(u64, u64)> {
                        // Prepare the original SQL once, without any UI pagination clauses.
                        let mut statement = transaction.prepare(&request.request.sql)?;
                        if !statement.readonly() || statement.column_count() == 0 {
                            return Err(AppError::validation(
                                "Query export requires a read-only query returning columns.",
                            ));
                        }
                        let columns = statement
                            .column_names()
                            .into_iter()
                            .map(String::from)
                            .collect::<Vec<_>>();
                        let column_count = statement.column_count();
                        let mut writer = SqlSnapshotWriter::new(
                            Protocol::Sqlite,
                            &request.format,
                            &request.path,
                            request.request.schema.as_deref().unwrap_or("main"),
                            false,
                        )?;
                        // Begin before stepping rows so even an empty CSV retains its header.
                        writer.begin_table("query_result", columns)?;
                        let values = request
                            .request
                            .params
                            .iter()
                            .cloned()
                            .map(sql_value)
                            .collect::<Vec<_>>();
                        let mut rows = statement.query(params_from_iter(values.iter()))?;
                        while let Some(row) = rows.next()? {
                            if token.is_cancelled() {
                                return Err(AppError::cancelled());
                            }
                            let cells = (0..column_count)
                                .map(|index| row.get_ref(index).map(sqlite_value_to_json))
                                .collect::<Result<Vec<_>, _>>()?;
                            writer.write_row(&cells)?;
                        }
                        drop(rows);
                        drop(statement);
                        if token.is_cancelled() {
                            return Err(AppError::cancelled());
                        }
                        writer.finish()
                    })();
                    // An interrupted statement must not interrupt its own rollback, nor
                    // leave a cancelled progress handler installed on a pooled connection.
                    transaction.progress_handler(0, None::<fn() -> bool>);
                    match exported {
                        Ok((rows_exported, tables_exported)) if !token.is_cancelled() => {
                            transaction.commit()?;
                            tracker.acquired();
                            Ok(SqlExportResult {
                                rows_exported,
                                tables_exported,
                                output_path: request.path.clone(),
                                snapshot_isolated: true,
                            })
                        }
                        Ok(_) => {
                            transaction.rollback()?;
                            Err(AppError::cancelled())
                        }
                        Err(error) => {
                            transaction.rollback()?;
                            Err(error)
                        }
                    }
                })();
                connection.progress_handler(0, None::<fn() -> bool>);
                result
            })
            .await;
        self.finish_operation(&operation_id);
        result
    }

    async fn fetch(&self, request: SqlRequest) -> AppResult<QueryResult> {
        let token = self.operation_token(&request.operation_id);
        let pool = self.pool()?;
        let tracker = self.tracker.clone();
        let operation_id = request.operation_id.clone();
        let started = Instant::now();
        let page_size = request.page_size.unwrap_or(500).clamp(1, 10_000) as usize;
        let offset = request.offset.unwrap_or(0);
        let sql = request.sql;
        let paged_sql = paginate_sql(&sql, page_size as u32, offset);
        let use_server_page = paged_sql.is_some();
        let query_sql = paged_sql.unwrap_or(sql);
        let params = request.params;
        let result = self
            .run_with_timeout(token, move |token| {
                let _waiter = tracker.waiter();
                let conn = pool.get().map_err(|error| {
                    tracker.timeout();
                    AppError::from(error)
                })?;
                let callback_token = token.clone();
                conn.progress_handler(1_000, Some(move || callback_token.is_cancelled()));
                let values = params.into_iter().map(sql_value).collect::<Vec<_>>();
                let mut statement = conn.prepare(&query_sql)?;
                let column_names = statement.column_names().into_iter().map(String::from).collect::<Vec<_>>();
                let columns = statement
                    .columns()
                    .iter()
                    .map(|column| ResultColumn {
                        name: column.name().to_string(),
                        type_name: rusqlite_type_name(column.decl_type()),
                        nullable: None,
                    })
                    .collect::<Vec<_>>();
                let mut rows = statement.query(params_from_iter(values.iter()))?;
                if !use_server_page {
                    for _ in 0..offset {
                        if rows.next()?.is_none() {
                            break;
                        }
                    }
                }
                let mut values = Vec::with_capacity(page_size + 1);
                while let Some(row) = rows.next()? {
                    let mut cells = Vec::with_capacity(column_names.len());
                    for index in 0..column_names.len() {
                        cells.push(sqlite_value_to_json(row.get_ref(index)?));
                    }
                    values.push(cells);
                    if values.len() > page_size {
                        break;
                    }
                }
                let has_more = values.len() > page_size;
                values.truncate(page_size);
                Ok(QueryResult {
                    operation_id,
                    columns,
                    rows: values,
                    rows_affected: 0,
                    page: ResultPage {
                        offset,
                        limit: page_size as u32,
                        has_more,
                        streamed: true,
                    },
                    duration_ms: started.elapsed().as_millis() as u64,
                })
            })
            .await;
        self.finish_operation(&request.operation_id);
        result
    }

    async fn run_script(&self, request: SqlScriptRequest, transactional: bool) -> AppResult<SqlScriptResult> {
        let token = self.operation_token(&request.operation_id);
        let pool = self.pool()?;
        let tracker = self.tracker.clone();
        let operation_id = request.operation_id.clone();
        let result_operation_id = operation_id.clone();
        let page_size = request.page_size.unwrap_or(500).clamp(1, 10_000);
        let statements = request.statements;
        let started = Instant::now();
        let result = self
            .run_with_timeout(token, move |token| {
                let _waiter = tracker.waiter();
                let mut conn = pool.get().map_err(|error| {
                    tracker.timeout();
                    AppError::from(error)
                })?;
                let callback_token = token.clone();
                conn.progress_handler(1_000, Some(move || callback_token.is_cancelled()));
                let mut results = Vec::with_capacity(statements.len());
                let mut failure = None;
                let committed = if transactional {
                    let transaction = conn.transaction()?;
                    for (index, statement) in statements.iter().enumerate() {
                        if token.is_cancelled() {
                            break;
                        }
                        match sqlite_script_step(&transaction, index, statement, page_size) {
                            Ok(step) => results.push(step),
                            Err(error) => {
                                failure = Some(ScriptFailure { index, class: error.class, message: error.message });
                                break;
                            }
                        }
                    }
                    if failure.is_some() || token.is_cancelled() {
                        transaction.rollback()?;
                        false
                    } else {
                        transaction.commit()?;
                        true
                    }
                } else {
                    for (index, statement) in statements.iter().enumerate() {
                        if token.is_cancelled() {
                            break;
                        }
                        match sqlite_script_step(&conn, index, statement, page_size) {
                            Ok(step) => results.push(step),
                            Err(error) => {
                                failure = Some(ScriptFailure { index, class: error.class, message: error.message });
                                break;
                            }
                        }
                    }
                    !results.is_empty()
                };
                if token.is_cancelled() {
                    return Err(AppError::cancelled());
                }
                Ok(SqlScriptResult {
                    operation_id: result_operation_id,
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
        if let Some(token) = self.cancellations.lock().get(operation_id) {
            token.cancel();
            Ok(())
        } else {
            Err(AppError::validation(format!(
                "Operation '{operation_id}' is not running or has already completed."
            )))
        }
    }

    async fn metadata(&self, request: MetadataRequest) -> AppResult<MetadataSnapshot> {
        let pool = self.pool()?;
        tokio::task::spawn_blocking(move || {
            sqlite_metadata(
                &pool,
                request.schema.as_deref(),
                request.table.as_deref(),
                request.include_tables,
                request.include_columns,
            )
        })
            .await
            .map_err(|error| AppError::new(crate::models::ErrorClass::Internal, error.to_string()))?
    }

    async fn ddl(&self, _schema: &str, table: &str) -> AppResult<String> {
        let pool = self.pool()?;
        let table = table.to_string();
        tokio::task::spawn_blocking(move || {
            let connection = checkout_idle(&pool)?;
            let mut statement = connection.prepare(
                "SELECT sql FROM sqlite_schema WHERE sql IS NOT NULL AND \
                 ((type IN ('table', 'view') AND name = ?1) OR (type IN ('index', 'trigger') AND tbl_name = ?1)) \
                 ORDER BY CASE type WHEN 'table' THEN 0 WHEN 'view' THEN 0 WHEN 'index' THEN 1 ELSE 2 END, name",
            )?;
            let statements = statement
                .query_map([table.as_str()], |row| row.get::<_, String>(0))?
                .collect::<Result<Vec<_>, _>>()?;
            if statements.is_empty() {
                return Err(AppError::validation(format!("Table '{table}' was not found.")));
            }
            Ok(statements
                .into_iter()
                .map(|sql| format!("{};", sql.trim_end_matches(';')))
                .collect::<Vec<_>>()
                .join("\n\n"))
        })
        .await
        .map_err(|error| AppError::new(crate::models::ErrorClass::Internal, error.to_string()))?
    }

    async fn object_stats(&self, _schema: &str) -> AppResult<Vec<crate::models::ObjectStat>> {
        let pool = self.pool()?;
        tokio::task::spawn_blocking(move || {
            let connection = checkout_idle(&pool)?;
            let mut statement = connection.prepare(
                "SELECT name, CASE type WHEN 'view' THEN 'VIEW' ELSE 'TABLE' END FROM sqlite_schema \
                 WHERE type IN ('table', 'view') AND name NOT LIKE 'sqlite_%' ORDER BY name",
            )?;
            let stats = statement
                .query_map([], |row| {
                    Ok(crate::models::ObjectStat {
                        name: row.get(0)?,
                        object_type: row.get(1)?,
                        ..Default::default()
                    })
                })?
                .collect::<Result<Vec<_>, _>>()?;
            Ok::<_, AppError>(stats)
        })
        .await
        .map_err(|error| AppError::new(crate::models::ErrorClass::Internal, error.to_string()))?
    }

    async fn metrics(&self) -> AppResult<PoolMetrics> {
        let pool = self.pool()?;
        let state = pool.state();
        Ok(self.tracker.metrics(state.connections as u64, state.idle_connections as u64, self.max_size))
    }

    async fn health_check(&self) -> AppResult<()> {
        let pool = self.pool()?;
        tokio::task::spawn_blocking(move || {
            let connection = checkout_idle(&pool)?;
            connection.query_row("SELECT 1", [], |_| Ok(()))?;
            Ok::<_, AppError>(())
        })
        .await
        .map_err(|error| AppError::new(crate::models::ErrorClass::Internal, error.to_string()))?
    }

    async fn close(&self) -> AppResult<()> {
        for (_, token) in self.cancellations.lock().drain() {
            token.cancel();
        }
        // Dropping the pooled connections occurs when the driver is removed from AppState
        // during Tauri shutdown. This method first guarantees all in-flight operations are cancelled.
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
    Some(format!(
        "SELECT * FROM ({body}\n) AS __luxbase_page LIMIT {} OFFSET {}",
        page_size.saturating_add(1),
        offset
    ))
}

fn sqlite_script_step(
    connection: &RusqliteConnection,
    index: usize,
    request: &SqlRequest,
    page_size: u32,
) -> AppResult<ScriptStatementResult> {
    let started = Instant::now();
    let values = request.params.iter().cloned().map(sql_value).collect::<Vec<_>>();
    let mut statement = connection.prepare(&request.sql)?;
    if statement.column_count() == 0 {
        let rows_affected = statement.execute(params_from_iter(values.iter()))?;
        return Ok(ScriptStatementResult {
            index,
            returns_rows: false,
            columns: Vec::new(),
            rows: Vec::new(),
            has_more: false,
            page_size,
            rows_affected: rows_affected as u64,
            last_insert_id: Some(connection.last_insert_rowid()).filter(|id| *id > 0),
            duration_ms: started.elapsed().as_millis() as u64,
        });
    }
    let columns = statement
        .columns()
        .iter()
        .map(|column| ResultColumn {
            name: column.name().to_string(),
            type_name: rusqlite_type_name(column.decl_type()),
            nullable: None,
        })
        .collect::<Vec<_>>();
    let column_count = columns.len();
    let mut rows = statement.query(params_from_iter(values.iter()))?;
    let mut values = Vec::new();
    let mut has_more = false;
    while let Some(row) = rows.next()? {
        if values.len() == page_size as usize {
            has_more = true;
            break;
        }
        let mut cells = Vec::with_capacity(column_count);
        for column in 0..column_count {
            cells.push(sqlite_value_to_json(row.get_ref(column)?));
        }
        values.push(cells);
    }
    Ok(ScriptStatementResult {
        index,
        returns_rows: true,
        columns,
        rows: values,
        has_more,
        page_size,
        rows_affected: 0,
        last_insert_id: None,
        duration_ms: started.elapsed().as_millis() as u64,
    })
}

fn sql_value(value: ParamValue) -> SqlValue {
    match value {
        ParamValue::Null => SqlValue::Null,
        ParamValue::Bool(value) => SqlValue::Integer(if value { 1 } else { 0 }),
        ParamValue::I64(value) => SqlValue::Integer(value),
        ParamValue::U64(value) => SqlValue::Integer(value.min(i64::MAX as u64) as i64),
        ParamValue::F64(value) => SqlValue::Real(value),
        ParamValue::Text(value) => SqlValue::Text(value),
        ParamValue::Bytes(value) => SqlValue::Blob(value),
        ParamValue::Json(value) => SqlValue::Text(value.to_string()),
    }
}

fn sqlite_value_to_json(value: ValueRef<'_>) -> Value {
    match value {
        ValueRef::Null => Value::Null,
        ValueRef::Integer(value) => super::integer_cell(value),
        ValueRef::Real(value) => Value::from(value),
        ValueRef::Text(value) => Value::String(String::from_utf8_lossy(value).to_string()),
        ValueRef::Blob(value) => json!({
            "bytesBase64": base64::engine::general_purpose::STANDARD.encode(value)
        }),
    }
}

fn rusqlite_type_name(declared: Option<&str>) -> String {
    declared
        .map(ToString::to_string)
        .unwrap_or_else(|| "ANY".to_string())
}

fn sqlite_metadata(
    pool: &Pool<SqliteConnectionManager>,
    schema: Option<&str>,
    only_table: Option<&str>,
    include_tables: bool,
    include_columns: bool,
) -> AppResult<MetadataSnapshot> {
    if schema.is_some_and(|value| value != "main") {
        return Err(AppError::validation(
            "SQLite only exposes the embedded 'main' schema.",
        ));
    }
    if !include_tables && only_table.is_none() && !include_columns {
        return Ok(MetadataSnapshot {
            databases: vec![DatabaseMetadata {
                name: "main".into(),
                tables: Vec::new(),
            }],
        });
    }
    let connection = checkout_idle(pool)?;
    let mut statement = connection.prepare(
        "SELECT name, type FROM sqlite_schema WHERE type IN ('table', 'view') \
         AND name NOT LIKE 'sqlite_%' ORDER BY name",
    )?;
    let entries = statement
        .query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)))?
        .collect::<Result<Vec<_>, _>>()?;

    let mut tables = Vec::with_capacity(entries.len());
    for (name, kind) in entries {
        if only_table.is_some_and(|filter| filter != name) {
            continue;
        }
        let detailed = include_columns || only_table.is_some();
        let is_view = kind == "view";
        tables.push(TableMetadata {
            schema: "main".into(),
            table_type: if is_view { "VIEW" } else { "TABLE" }.into(),
            columns: if detailed { sqlite_columns(&connection, &name)? } else { Vec::new() },
            indexes: if detailed && !is_view { sqlite_indexes(&connection, &name)? } else { Vec::new() },
            foreign_keys: if detailed && !is_view { sqlite_foreign_keys(&connection, &name)? } else { Vec::new() },
            name,
        });
    }
    Ok(MetadataSnapshot {
        databases: vec![DatabaseMetadata {
            name: "main".into(),
            tables,
        }],
    })
}

/// Checks out a connection for catalog work. A previous operation's progress handler may still
/// be installed (and its token cancelled), which would interrupt every statement here.
fn checkout_idle(
    pool: &Pool<SqliteConnectionManager>,
) -> AppResult<r2d2::PooledConnection<SqliteConnectionManager>> {
    let connection = pool.get()?;
    connection.progress_handler(0, None::<fn() -> bool>);
    Ok(connection)
}

fn sqlite_columns(
    connection: &RusqliteConnection,
    table: &str,
) -> AppResult<Vec<ColumnMetadata>> {
    let escaped = table.replace('"', "\"\"");
    let mut statement = connection.prepare(&format!("PRAGMA table_info(\"{escaped}\")"))?;
    let rows = statement.query_map([], |row| {
        Ok(ColumnMetadata {
            name: row.get(1)?,
            data_type: row.get(2)?,
            nullable: row.get::<_, i64>(3)? == 0,
            default_value: row.get(4)?,
            is_primary_key: row.get::<_, i64>(5)? > 0,
            ordinal: row.get::<_, i64>(0)?,
            comment: None,
            extra: None,
        })
    })?;
    rows.collect::<Result<Vec<_>, _>>().map_err(AppError::from)
}

fn sqlite_indexes(
    connection: &RusqliteConnection,
    table: &str,
) -> AppResult<Vec<IndexMetadata>> {
    let escaped = table.replace('"', "\"\"");
    let mut statement = connection.prepare(&format!("PRAGMA index_list(\"{escaped}\")"))?;
    let indexes = statement
        .query_map([], |row| {
            Ok((row.get::<_, String>(1)?, row.get::<_, i64>(2)? == 1))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    let mut result = Vec::with_capacity(indexes.len());
    for (name, unique) in indexes {
        let escaped_index = name.replace('"', "\"\"");
        let mut info = connection.prepare(&format!("PRAGMA index_info(\"{escaped_index}\")"))?;
        let columns = info
            .query_map([], |row| {
                Ok(row.get::<_, Option<String>>(2)?.unwrap_or_else(|| "(expression)".into()))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        result.push(IndexMetadata {
            name,
            columns,
            unique,
        });
    }
    Ok(result)
}

fn sqlite_foreign_keys(
    connection: &RusqliteConnection,
    table: &str,
) -> AppResult<Vec<ForeignKeyMetadata>> {
    let escaped = table.replace('"', "\"\"");
    let mut statement = connection.prepare(&format!("PRAGMA foreign_key_list(\"{escaped}\")"))?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, Option<String>>(4)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    let mut grouped: Vec<(i64, ForeignKeyMetadata)> = Vec::new();
    for (id, referenced_table, column, referenced_column) in rows {
        let referenced_column = referenced_column.unwrap_or_default();
        match grouped.iter_mut().find(|(key, _)| *key == id) {
            Some((_, key)) => {
                key.columns.push(column);
                key.referenced_columns.push(referenced_column);
            }
            None => grouped.push((
                id,
                ForeignKeyMetadata {
                    name: format!("fk_{table}_{id}"),
                    columns: vec![column],
                    referenced_table,
                    referenced_columns: vec![referenced_column],
                },
            )),
        }
    }
    Ok(grouped.into_iter().map(|(_, key)| key).collect())
}

#[cfg(test)]
mod query_export_tests {
    use super::*;
    use crate::models::ErrorClass;
    use std::fs;
    use std::path::PathBuf;

    struct Fixture {
        driver: SqliteDriver,
        directory: PathBuf,
    }

    impl Fixture {
        fn new() -> Self {
            let mut fixture = Self {
                driver: SqliteDriver::new("query-export-test".into()),
                directory: std::env::temp_dir()
                    .join(format!("luxbase-query-export-{}", uuid::Uuid::new_v4())),
            };
            fs::create_dir(&fixture.directory).unwrap();
            let database = fixture.directory.join("test.sqlite3");
            let connection = RusqliteConnection::open(&database).unwrap();
            connection
                .execute_batch(
                    "CREATE TABLE export_rows (id INTEGER PRIMARY KEY, label TEXT NOT NULL);
                     WITH RECURSIVE numbers(id) AS (
                         SELECT 1 UNION ALL SELECT id + 1 FROM numbers WHERE id < 1200
                     )
                     INSERT INTO export_rows SELECT id, 'row-' || id FROM numbers;",
                )
                .unwrap();
            drop(connection);
            fixture.driver.pool = Some(
                r2d2::Builder::new()
                    .max_size(1)
                    .build(SqliteConnectionManager::file(database))
                    .unwrap(),
            );
            fixture
        }

        fn request(&self, sql: &str, params: Vec<ParamValue>, format: &str) -> SqlQueryExportRequest {
            SqlQueryExportRequest {
                request: SqlRequest {
                    connection_id: self.driver.connection_id.clone(),
                    sql: sql.into(),
                    params,
                    operation_id: uuid::Uuid::new_v4().to_string(),
                    page_size: Some(5),
                    offset: Some(700),
                    schema: Some("main".into()),
                },
                format: format.into(),
                path: self
                    .directory
                    .join(format!("result.{format}"))
                    .to_string_lossy()
                    .into_owned(),
            }
        }

        fn assert_released(&self) {
            assert!(self.driver.cancellations.lock().is_empty());
            let connection = self.driver.pool.as_ref().unwrap().get().unwrap();
            assert!(connection.is_autocommit());
            let count: i64 = connection
                .query_row("SELECT count(*) FROM export_rows", [], |row| row.get(0))
                .unwrap();
            assert_eq!(count, 1200);
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            self.driver.pool.take();
            if self.directory.exists() {
                fs::remove_dir_all(&self.directory).unwrap();
            }
        }
    }

    #[tokio::test]
    async fn csv_streams_all_bound_rows_and_ignores_ui_pagination() {
        let fixture = Fixture::new();
        let request = fixture.request(
            "SELECT id, label FROM export_rows WHERE id >= ?1 AND label LIKE ?2 ORDER BY id",
            vec![ParamValue::I64(101), ParamValue::Text("row-%".into())],
            "csv",
        );
        let result = fixture.driver.export_query(request).await.unwrap();
        assert_eq!(result.rows_exported, 1100);
        assert_eq!(result.tables_exported, 1);
        assert!(result.snapshot_isolated);
        let output = fs::read_to_string(&result.output_path).unwrap();
        let lines = output.lines().collect::<Vec<_>>();
        assert_eq!(lines.len(), 1101);
        assert_eq!(lines[0], "id,label");
        assert_eq!(lines[1], "101,row-101");
        assert_eq!(lines[1100], "1200,row-1200");
        fixture.assert_released();

        // Limits explicitly written in SQL are still part of the query itself.
        let request = fixture.request(
            "SELECT id FROM export_rows ORDER BY id LIMIT ?1 OFFSET ?2",
            vec![ParamValue::I64(3), ParamValue::I64(2)],
            "csv",
        );
        let result = fixture.driver.export_query(request).await.unwrap();
        assert_eq!(result.rows_exported, 3);
        assert_eq!(
            fs::read_to_string(&result.output_path).unwrap(),
            "id\n3\n4\n5\n"
        );
        fixture.assert_released();
    }

    #[tokio::test]
    async fn empty_csv_retains_prepared_column_headers() {
        let fixture = Fixture::new();
        let request = fixture.request(
            "SELECT id AS \"identifier,quoted\", label FROM export_rows WHERE id > ?1",
            vec![ParamValue::I64(1200)],
            "csv",
        );
        let result = fixture.driver.export_query(request).await.unwrap();
        assert_eq!(result.rows_exported, 0);
        assert_eq!(result.tables_exported, 1);
        assert!(result.snapshot_isolated);
        assert_eq!(
            fs::read_to_string(&result.output_path).unwrap(),
            "\"identifier,quoted\",label\n"
        );
        fixture.assert_released();
    }

    #[tokio::test]
    async fn rejects_writes_including_returning_without_modifying_the_database() {
        let fixture = Fixture::new();
        for sql in [
            "UPDATE export_rows SET label = 'changed' WHERE id = 1 RETURNING id",
            "DELETE FROM export_rows WHERE id = 1 RETURNING id",
            "INSERT INTO export_rows VALUES (1201, 'new') RETURNING id",
            "CREATE TABLE forbidden (id INTEGER)",
        ] {
            let request = fixture.request(sql, Vec::new(), "csv");
            let path = request.path.clone();
            let error = fixture.driver.export_query(request).await.unwrap_err();
            assert_eq!(error.class, ErrorClass::Validation);
            assert!(!Path::new(&path).exists());
            fixture.assert_released();
        }
        let connection = fixture.driver.pool.as_ref().unwrap().get().unwrap();
        let label: String = connection
            .query_row("SELECT label FROM export_rows WHERE id = 1", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(label, "row-1");
        let forbidden: i64 = connection
            .query_row(
                "SELECT count(*) FROM sqlite_schema WHERE name = 'forbidden'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(forbidden, 0);
    }

    #[tokio::test]
    async fn jsonl_streams_objects_without_database_scope_metadata() {
        let fixture = Fixture::new();
        let request = fixture.request(
            "SELECT id, label, NULL AS missing, X'0001' AS bytes
             FROM export_rows WHERE id > ?1 ORDER BY id",
            vec![ParamValue::I64(600)],
            "jsonl",
        );
        let result = fixture.driver.export_query(request).await.unwrap();
        assert_eq!(result.rows_exported, 600);
        assert_eq!(result.tables_exported, 1);
        assert!(result.snapshot_isolated);
        let output = fs::read_to_string(&result.output_path).unwrap();
        let rows = output
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(rows.len(), 600);
        assert_eq!(
            rows[0],
            json!({
                "id": 601,
                "label": "row-601",
                "missing": null,
                "bytes": {"bytesBase64": "AAE="}
            })
        );
        assert_eq!(rows.last().unwrap()["id"], 1200);
        assert!(output.ends_with('\n'));
        fixture.assert_released();
    }

    #[tokio::test]
    async fn rejects_non_query_formats_before_opening_output_or_registering_operations() {
        let fixture = Fixture::new();
        for format in ["sql", "CSV", "json", ""] {
            let request = fixture.request("SELECT id FROM export_rows", Vec::new(), format);
            let path = request.path.clone();
            let error = fixture.driver.export_query(request).await.unwrap_err();
            assert_eq!(error.class, ErrorClass::Validation);
            assert!(!Path::new(&path).exists());
            fixture.assert_released();
        }
    }

    const SLOW_QUERY: &str =
        "WITH RECURSIVE numbers(id) AS (
             SELECT 1 UNION ALL SELECT id + 1 FROM numbers WHERE id < 100000000
         ) SELECT sum(id) AS total FROM numbers";

    #[tokio::test]
    async fn timeout_rolls_back_and_releases_the_operation_and_progress_handler() {
        let mut fixture = Fixture::new();
        fixture.driver.query_timeout = Duration::from_millis(20);
        let request = fixture.request(SLOW_QUERY, Vec::new(), "csv");
        let error = fixture.driver.export_query(request).await.unwrap_err();
        assert_eq!(error.class, ErrorClass::Timeout);
        fixture.assert_released();
    }

    #[tokio::test]
    async fn cancellation_rolls_back_before_releasing_the_operation() {
        let fixture = Fixture::new();
        let request = fixture.request(SLOW_QUERY, Vec::new(), "csv");
        let operation_id = request.request.operation_id.clone();
        let output_path = request.path.clone();
        fs::write(&output_path, "existing export must survive").unwrap();
        let cancel = async {
            tokio::time::timeout(Duration::from_secs(5), async {
                while !fixture.driver.cancellations.lock().contains_key(&operation_id) {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("The export worker should register its cancellable operation.");
            fixture.driver.cancel(&operation_id).await.unwrap();
        };
        let (result, ()) = tokio::join!(fixture.driver.export_query(request), cancel);
        assert_eq!(result.unwrap_err().class, ErrorClass::Cancelled);
        assert_eq!(fs::read_to_string(output_path).unwrap(), "existing export must survive");
        fixture.assert_released();
    }
}

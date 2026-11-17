use crate::app_state::AppState;
use crate::error::{AppError, AppResult};
use crate::models::{
    AppBootstrap, AuditEntry, ConnectionConfig, ConnectionTestResult, DriverFamily,
    EsAlias, EsClusterInfo, EsDocument, EsIndex, EsRestRequest, EsRestResult, EsSearchRequest,
    EsSearchResult, ExecuteResult, LegacyImportReport, MetadataRequest, MetadataSnapshot,
    PoolMetrics, Protocol, ProtocolDescriptor, QueryDraft, QueryHistoryEntry, QueryResult,
    RedisCommandRequest, RedisCommandResult, RedisInfo, RedisScanRequest, RedisScanResult,
    RedisValue, SavedConnection, SqlBatchRequest, SqlExportRequest, SqlImportRequest, SqlRequest,
    SqlScriptRequest, SqlScriptResult,
};
use base64::{engine::general_purpose::STANDARD as B64, Engine};
use serde::{Deserialize, Serialize};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tauri::{AppHandle, Manager, State};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SaveConnectionPayload {
    pub config: ConnectionConfig,
    pub password: Option<String>,
    pub ssh_passphrase: Option<String>,
    #[serde(default)]
    pub ssh_password: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TestConnectionPayload {
    pub config: ConnectionConfig,
    pub password: Option<String>,
    pub ssh_passphrase: Option<String>,
    #[serde(default)]
    pub ssh_password: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IdentitySnapshot {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<serde_json::Value>>,
    pub note: Option<String>,
}

pub(crate) fn state_ref(state: &State<'_, Arc<AppState>>) -> Arc<AppState> {
    Arc::clone(state.inner())
}

fn password_for(state: &AppState, config: &ConnectionConfig, supplied: Option<String>) -> AppResult<Option<String>> {
    if supplied.is_some() {
        return Ok(supplied);
    }
    state.connection_password(config)
}

/// Binary Redis keys cannot round-trip through a lossy UTF-8 display string; when the scanner
/// supplies the exact bytes as base64 we address the key with those instead.
fn redis_raw_key(display: &str, key_base64: Option<&str>) -> AppResult<Vec<u8>> {
    match key_base64 {
        Some(b64) => B64
            .decode(b64)
            .map_err(|error| AppError::validation(format!("Invalid base64 Redis key: {error}"))),
        None => Ok(display.as_bytes().to_vec()),
    }
}

#[tauri::command]
pub async fn app_bootstrap(state: State<'_, Arc<AppState>>) -> AppResult<AppBootstrap> {
    let state = state_ref(&state);
    let connections = state.store.list_connections()?;
    let recent_history = state.store.recent_history(25)?;
    let recent_audit = state.store.recent_audit(50)?;
    let security = state.store.security_state();
    Ok(AppBootstrap {
        connections,
        recent_history,
        recent_audit,
        protocols: vec![
            ProtocolDescriptor {
                id: Protocol::Mysql,
                family: DriverFamily::Sql,
                label: "MySQL".into(),
                supports: vec![
                    "query".into(),
                    "transaction".into(),
                    "metadata".into(),
                    "schema_compare".into(),
                    "users_permissions".into(),
                    "ssh".into(),
                    "tls".into(),
                ],
                unsupported: Vec::new(),
            },
            ProtocolDescriptor {
                id: Protocol::Postgres,
                family: DriverFamily::Sql,
                label: "PostgreSQL".into(),
                supports: vec![
                    "query".into(),
                    "transaction".into(),
                    "metadata".into(),
                    "schema_compare".into(),
                    "users_permissions".into(),
                    "ssh".into(),
                    "tls".into(),
                ],
                unsupported: Vec::new(),
            },
            ProtocolDescriptor {
                id: Protocol::Sqlite,
                family: DriverFamily::Sql,
                label: "SQLite".into(),
                supports: vec![
                    "query".into(),
                    "transaction".into(),
                    "metadata".into(),
                    "schema_compare".into(),
                ],
                unsupported: vec![
                    "SSH tunnel does not apply to a local file".into(),
                    "Server-level users and roles do not apply".into(),
                ],
            },
            ProtocolDescriptor {
                id: Protocol::Redis,
                family: DriverFamily::Kv,
                label: "Redis".into(),
                supports: vec![
                    "scan".into(),
                    "typed_editors".into(),
                    "raw_console".into(),
                    "sentinel".into(),
                    "cluster".into(),
                    "tls".into(),
                    "acl_view".into(),
                ],
                unsupported: vec![
                    "SQL grids and transactions do not apply".into(),
                    "Schema comparison, table design and foreign keys do not apply".into(),
                ],
            },
            ProtocolDescriptor {
                id: Protocol::Elasticsearch,
                family: DriverFamily::Search,
                label: "Elasticsearch".into(),
                supports: vec![
                    "es_indices".into(),
                    "es_documents".into(),
                    "es_query_dsl".into(),
                    "es_rest_console".into(),
                    "es_mappings".into(),
                    "es_aliases".into(),
                    "tls".into(),
                ],
                unsupported: vec![
                    "SQL grids, transactions and schema comparison do not apply".into(),
                    "Table design and foreign keys do not apply".into(),
                ],
            },
        ],
        security,
    })
}

#[tauri::command]
pub async fn save_connection(
    state: State<'_, Arc<AppState>>,
    payload: SaveConnectionPayload,
) -> AppResult<SavedConnection> {
    let state = state_ref(&state);
    let saved = state.store.save_connection(
        &payload.config,
        payload.password.as_deref(),
        payload.ssh_passphrase.as_deref(),
        payload.ssh_password.as_deref(),
    )?;
    // An edited connection must not keep serving requests from a pool built with old settings.
    state.forget_connection(&saved.id);
    let _ = state.sql.disconnect(&saved.id).await;
    let _ = state.kv.disconnect(&saved.id).await;
    let _ = state.search.disconnect(&saved.id).await;
    Ok(saved)
}

#[tauri::command]
pub async fn get_connection_config(
    state: State<'_, Arc<AppState>>,
    connection_id: String,
) -> AppResult<ConnectionConfig> {
    let state = state_ref(&state);
    state.store.load_connection_config(&connection_id)
}

#[tauri::command]
pub async fn test_connection(
    state: State<'_, Arc<AppState>>,
    payload: TestConnectionPayload,
) -> AppResult<ConnectionTestResult> {
    let state = state_ref(&state);
    let mut config = payload.config;
    config.ssh_passphrase = payload
        .ssh_passphrase
        .or_else(|| state.store.ssh_passphrase(&config.id).ok().flatten());
    config.ssh_password = payload
        .ssh_password
        .or_else(|| state.store.ssh_password(&config.id).ok().flatten());
    let password = payload
        .password
        .or_else(|| state.store.connection_password(&config.id).ok().flatten());
    let result = match config.protocol.family() {
        DriverFamily::Sql => state.sql.test(&config, password).await,
        DriverFamily::Kv => state.kv.test(&config, password).await,
        DriverFamily::Search => state.search.test(&config, password).await,
    };
    let _ = state.store.append_audit(
        Some(&config.id),
        Some(&config.name),
        Some(config.protocol.as_str()),
        "connection_test",
        &format!("Test {} connection", config.protocol.as_str()),
        None,
        if result.ok { "success" } else { "failure" },
        result.message.as_deref(),
    );
    Ok(result)
}

#[tauri::command]
pub async fn save_query_draft(
    state: State<'_, Arc<AppState>>,
    connection_id: String,
    name: String,
    sql_text: String,
    draft_id: Option<i64>,
) -> AppResult<QueryDraft> {
    let state = state_ref(&state);
    let config = state.connection(&connection_id)?;
    if config.protocol.family() != DriverFamily::Sql {
        return Err(AppError::unsupported(
            "Query drafts are only available for SQL connections.",
        ));
    }
    let draft = state.store.save_query_draft(&connection_id, &name, &sql_text, draft_id)?;
    state.store.append_audit(
        Some(&connection_id),
        Some(&config.name),
        Some(config.protocol.as_str()),
        "query_draft_saved",
        &format!("Saved query draft '{}'", draft.name),
        None,
        "success",
        None,
    )?;
    Ok(draft)
}

#[tauri::command]
pub async fn rename_query_draft(
    state: State<'_, Arc<AppState>>, connection_id: String, draft_id: i64, name: String,
) -> AppResult<QueryDraft> {
    let state = state_ref(&state);
    let config = state.connection(&connection_id)?;
    let draft = state.store.rename_query_draft(&connection_id, draft_id, &name)?;
    state.store.append_audit(Some(&connection_id), Some(&config.name), Some(config.protocol.as_str()),
        "query_draft_renamed", "Renamed saved query", None, "success", None)?;
    Ok(draft)
}

#[tauri::command]
pub async fn delete_query_draft(
    state: State<'_, Arc<AppState>>, connection_id: String, draft_id: i64,
) -> AppResult<()> {
    let state = state_ref(&state);
    let config = state.connection(&connection_id)?;
    state.store.delete_query_draft(&connection_id, draft_id)?;
    state.store.append_audit(Some(&connection_id), Some(&config.name), Some(config.protocol.as_str()),
        "query_draft_deleted", "Deleted saved query from local workspace", None, "success", None)?;
    Ok(())
}

#[tauri::command]
pub async fn list_query_drafts(
    state: State<'_, Arc<AppState>>,
    connection_id: String,
) -> AppResult<Vec<QueryDraft>> {
    let state = state_ref(&state);
    state.store.list_query_drafts(&connection_id)
}

#[tauri::command]
pub async fn sql_execute(
    state: State<'_, Arc<AppState>>,
    connection_id: String,
    request: SqlRequest,
) -> AppResult<ExecuteResult> {
    let state = state_ref(&state);
    state.ensure_running()?;
    if sql_statement_count(&request.sql) > 1 {
        return Err(AppError::validation(
            "Multiple SQL statements must use the transaction batch command; partial commits are disabled.",
        ));
    }
    reject_session_state_statement(&request.sql)?;
    let config = state.connection(&connection_id)?;
    let password = state.connection_password(&config)?;
    let result = state
        .sql
        .execute(&config, password.clone(), request.clone())
        .await;
    let summary = summarize_sql(&request.sql);
    match &result {
        Ok(execution) => {
            state.store.record_query(
                &connection_id,
                &redact_sql_password_literals(&request.sql),
                "success",
                Some(execution.duration_ms),
                Some(execution.rows_affected),
                None,
            )?;
            state.store.append_audit(
                Some(&connection_id),
                Some(&config.name),
                Some(config.protocol.as_str()),
                audit_kind_for_sql(&request.sql),
                &summary,
                Some(execution.rows_affected as i64),
                "success",
                None,
            )?;
        }
        Err(error) => {
            state.store.record_query(
                &connection_id,
                &redact_sql_password_literals(&request.sql),
                "failure",
                None,
                None,
                Some(&format!("{:?}", error.class)),
            )?;
            state.store.append_audit(
                Some(&connection_id),
                Some(&config.name),
                Some(config.protocol.as_str()),
                audit_kind_for_sql(&request.sql),
                &summary,
                None,
                "failure",
                Some(&error.message),
            )?;
        }
    }
    let _ = password;
    result
}

#[tauri::command]
pub async fn sql_execute_batch(
    state: State<'_, Arc<AppState>>,
    connection_id: String,
    request: SqlBatchRequest,
) -> AppResult<Vec<ExecuteResult>> {
    let state = state_ref(&state);
    state.ensure_running()?;
    if request.statements.is_empty() {
        return Err(AppError::validation("The transaction batch contains no statements."));
    }
    if !request.wrap_in_transaction {
        return Err(AppError::validation(
            "SQL batches must be wrapped in a transaction. Partial commits are disabled.",
        ));
    }
    if let Some(statement) = request
        .statements
        .iter()
        .find(|statement| sql_statement_count(&statement.sql) != 1)
    {
        return Err(AppError::validation(format!(
            "Transaction statement '{}' must contain exactly one SQL statement.",
            statement.operation_id
        )));
    }
    for statement in &request.statements {
        reject_session_state_statement(&statement.sql)?;
    }
    let config = state.connection(&connection_id)?;
    if config.protocol == Protocol::Mysql
        && request
            .statements
            .iter()
            .any(|statement| audit_kind_for_sql(&statement.sql) == "ddl")
    {
        return Err(AppError::validation(
            "MySQL DDL causes an implicit commit and cannot be atomic. Use the reviewed DDL execution path, which reports that earlier DDL steps may already be committed if a later step fails.",
        ));
    }
    let password = state.connection_password(&config)?;
    let statement_count = request.statements.len();
    let contains_ddl = request
        .statements
        .iter()
        .any(|statement| audit_kind_for_sql(&statement.sql) == "ddl");
    let result = state
        .sql
        .execute_batch(&config, password, request.clone())
        .await;
    state.store.append_audit(
        Some(&connection_id),
        Some(&config.name),
        Some(config.protocol.as_str()),
        if contains_ddl { "ddl_batch" } else { "sql_batch" },
        &format!("Transaction batch with {statement_count} statements"),
        result.as_ref().ok().map(|values| {
            values
                .iter()
                .map(|value| value.rows_affected as i64)
                .sum()
        }),
        if result.is_ok() { "success" } else { "failure" },
        result.as_ref().err().map(|error| error.message.as_str()),
    )?;
    result
}

pub(crate) fn is_read_statement(sql: &str) -> bool {
    matches!(
        sql_leading_keyword(sql).as_str(),
        "SELECT" | "SHOW" | "DESCRIBE" | "DESC" | "EXPLAIN" | "VALUES" | "TABLE" | "PRAGMA"
    )
}

/// Runs a multi-statement script on one session and returns one result per statement. Scripts
/// that write run inside a single transaction (rolled back on the first failure); MySQL DDL
/// commits implicitly, so such scripts run statement by statement only with explicit consent.
#[tauri::command]
pub async fn sql_run_script(
    state: State<'_, Arc<AppState>>,
    connection_id: String,
    request: SqlScriptRequest,
) -> AppResult<SqlScriptResult> {
    let state = state_ref(&state);
    state.ensure_running()?;
    if request.statements.is_empty() {
        return Err(AppError::validation("The script contains no statements."));
    }
    if request.statements.len() > 500 {
        return Err(AppError::validation("A script is limited to 500 statements per run."));
    }
    for (index, statement) in request.statements.iter().enumerate() {
        if sql_statement_count(&statement.sql) != 1 {
            return Err(AppError::validation(format!(
                "Script statement {} must contain exactly one SQL statement.",
                index + 1
            )));
        }
        reject_session_state_statement(&statement.sql).map_err(|error| {
            AppError::new(error.class, format!("Statement {}: {}", index + 1, error.message))
        })?;
    }
    let config = state.connection(&connection_id)?;
    if config.protocol.family() != DriverFamily::Sql {
        return Err(AppError::unsupported("SQL scripts are not available for the Redis KV protocol."));
    }
    let has_write = request.statements.iter().any(|statement| !is_read_statement(&statement.sql));
    let has_ddl = request
        .statements
        .iter()
        .any(|statement| audit_kind_for_sql(&statement.sql) == "ddl");
    let implicit_commit = config.protocol == Protocol::Mysql && has_ddl;
    if implicit_commit && request.statements.len() > 1 && !request.allow_partial_commit {
        return Err(AppError::validation(
            "MySQL DDL commits implicitly, so this script cannot run atomically. Confirm statement-by-statement execution to continue.",
        ));
    }
    let transactional = has_write && !implicit_commit;
    let password = state.connection_password(&config)?;
    let statements = request.statements.clone();
    let result = state
        .sql
        .run_script(&config, password, request, transactional)
        .await;
    match &result {
        Ok(script) => {
            for (statement, outcome) in statements.iter().zip(script.results.iter()) {
                let committed = !transactional || script.committed;
                state.store.record_query(
                    &connection_id,
                    &redact_sql_password_literals(&statement.sql),
                    if committed { "success" } else { "rolled_back" },
                    Some(outcome.duration_ms),
                    Some(if outcome.returns_rows { outcome.rows.len() as u64 } else { outcome.rows_affected }),
                    None,
                )?;
            }
            if let Some(failure) = &script.failure {
                if let Some(statement) = statements.get(failure.index) {
                    state.store.record_query(
                        &connection_id,
                        &statement.sql,
                        "failure",
                        None,
                        None,
                        Some(&format!("{:?}", failure.class)),
                    )?;
                }
            }
            state.store.append_audit(
                Some(&connection_id),
                Some(&config.name),
                Some(config.protocol.as_str()),
                if has_ddl { "ddl_script" } else if has_write { "sql_script" } else { "query_script" },
                &format!(
                    "Script with {} statements ({})",
                    statements.len(),
                    if transactional { "single transaction" } else { "statement by statement" }
                ),
                Some(
                    script
                        .results
                        .iter()
                        .map(|outcome| outcome.rows_affected as i64)
                        .sum(),
                ),
                if script.failure.is_none() { "success" } else { "failure" },
                script.failure.as_ref().map(|failure| failure.message.as_str()),
            )?;
        }
        Err(error) => {
            state.store.append_audit(
                Some(&connection_id),
                Some(&config.name),
                Some(config.protocol.as_str()),
                "sql_script",
                &format!("Script with {} statements", statements.len()),
                None,
                "failure",
                Some(&error.message),
            )?;
        }
    }
    result
}

#[tauri::command]
pub async fn sql_apply_ddl(
    state: State<'_, Arc<AppState>>,
    connection_id: String,
    statements: Vec<SqlRequest>,
    operation_id: String,
) -> AppResult<Vec<ExecuteResult>> {
    let state = state_ref(&state);
    state.ensure_running()?;
    if statements.is_empty() {
        return Err(AppError::validation("No reviewed DDL statements were supplied."));
    }
    if let Some(statement) = statements
        .iter()
        .find(|statement| sql_statement_count(&statement.sql) != 1)
    {
        return Err(AppError::validation(format!(
            "DDL statement '{}' must contain exactly one SQL statement.",
            statement.operation_id
        )));
    }
    for statement in &statements {
        reject_session_state_statement(&statement.sql)?;
        if audit_kind_for_sql(&statement.sql) != "ddl" {
            return Err(AppError::validation(
                "The reviewed DDL path only accepts DDL statements.",
            ));
        }
    }
    let config = state.connection(&connection_id)?;
    let password = state.connection_password(&config)?;
    let results = if config.protocol == Protocol::Mysql {
        let mut applied = Vec::with_capacity(statements.len());
        for (index, statement) in statements.into_iter().enumerate() {
            let summary = summarize_sql(&statement.sql);
            match state
                .sql
                .execute(&config, password.clone(), statement)
                .await
            {
                Ok(result) => {
                    state.store.append_audit(
                        Some(&connection_id),
                        Some(&config.name),
                        Some(config.protocol.as_str()),
                        "ddl",
                        &summary,
                        Some(result.rows_affected as i64),
                        "success",
                        Some(&format!("MySQL implicit-commit DDL step {}", index + 1)),
                    )?;
                    applied.push(result);
                }
                Err(error) => {
                    state.store.append_audit(
                        Some(&connection_id),
                        Some(&config.name),
                        Some(config.protocol.as_str()),
                        "ddl",
                        &summary,
                        None,
                        "failure",
                        Some(&error.message),
                    )?;
                    return Err(AppError::new(
                        error.class,
                        format!(
                            "MySQL DDL step {} failed after {} earlier DDL step(s) may have committed: {}",
                            index + 1,
                            index,
                            error.message
                        ),
                    ));
                }
            }
        }
        applied
    } else {
        state
            .sql
            .execute_batch(
                &config,
                password,
                SqlBatchRequest {
                    connection_id: connection_id.clone(),
                    statements,
                    operation_id,
                    wrap_in_transaction: true,
                },
            )
            .await?
    };
    Ok(results)
}

#[tauri::command]
pub async fn sql_export_query(
    state: State<'_, Arc<AppState>>, connection_id: String,
    request: crate::models::SqlQueryExportRequest,
) -> AppResult<crate::models::SqlExportResult> {
    let state = state_ref(&state);
    state.ensure_running()?;
    if !matches!(request.format.as_str(), "csv" | "jsonl") || sql_statement_count(&request.request.sql) != 1
        || !matches!(sql_leading_keyword(&request.request.sql).as_str(), "SELECT" | "WITH" | "SHOW" | "DESCRIBE" | "DESC" | "EXPLAIN" | "VALUES" | "TABLE" | "PRAGMA")
    {
        return Err(AppError::validation("Query export accepts one result-producing statement in CSV or JSONL format."));
    }
    reject_session_state_statement(&request.request.sql)?;
    let config = state.connection(&connection_id)?;
    let password = state.connection_password(&config)?;
    let result = state.sql.export_query(&config, password, request).await?;
    state.store.append_audit(Some(&connection_id), Some(&config.name), Some(config.protocol.as_str()),
        "query_export", "Export complete query result", Some(result.rows_exported as i64), "success", None)?;
    Ok(result)
}

#[tauri::command]
pub async fn sql_export_result(
    state: State<'_, Arc<AppState>>, connection_id: String, format: String, path: String,
    columns: Vec<crate::models::ResultColumn>, rows: Vec<Vec<serde_json::Value>>,
) -> AppResult<crate::models::SqlExportResult> {
    let state = state_ref(&state);
    state.ensure_running()?;
    if !matches!(format.as_str(), "csv" | "jsonl") || rows.len() > 10_000 || columns.is_empty() {
        return Err(AppError::validation("Select up to 10000 result rows and choose CSV or JSONL."));
    }
    let config = state.connection(&connection_id)?;
    let protocol = config.protocol;
    let result = tokio::task::spawn_blocking(move || {
        crate::drivers::sql::SqlConnectionManager::export_result(protocol, &format, &path, columns, rows)
    }).await.map_err(|error| AppError::new(crate::models::ErrorClass::Internal, error.to_string()))??;
    state.store.append_audit(Some(&connection_id), Some(&config.name), Some(config.protocol.as_str()),
        "query_export", "Export displayed query rows", Some(result.rows_exported as i64), "success", None)?;
    Ok(result)
}

#[tauri::command]
pub async fn sql_fetch(
    state: State<'_, Arc<AppState>>,
    connection_id: String,
    request: SqlRequest,
) -> AppResult<QueryResult> {
    let state = state_ref(&state);
    state.ensure_running()?;
    if sql_statement_count(&request.sql) != 1 {
        return Err(AppError::validation(
            "Fetch requires exactly one SQL statement. Use the transaction batch command for scripts.",
        ));
    }
    reject_session_state_statement(&request.sql)?;
    let config = state.connection(&connection_id)?;
    let password = state.connection_password(&config)?;
    let result = state
        .sql
        .fetch(&config, password, request.clone())
        .await;
    let summary = summarize_sql(&request.sql);
    match &result {
        Ok(query) => {
            state.store.record_query(
                &connection_id,
                &redact_sql_password_literals(&request.sql),
                "success",
                Some(query.duration_ms),
                Some(query.rows.len() as u64),
                None,
            )?;
            state.store.append_audit(
                Some(&connection_id),
                Some(&config.name),
                Some(config.protocol.as_str()),
                "query_fetch",
                &summary,
                Some(query.rows.len() as i64),
                "success",
                None,
            )?;
        }
        Err(error) => {
            state.store.record_query(
                &connection_id,
                &redact_sql_password_literals(&request.sql),
                "failure",
                None,
                None,
                Some(&format!("{:?}", error.class)),
            )?;
            state.store.append_audit(
                Some(&connection_id),
                Some(&config.name),
                Some(config.protocol.as_str()),
                "query_fetch",
                &summary,
                None,
                "failure",
                Some(&error.message),
            )?;
        }
    }
    result
}

fn audited_commit(audit: AppResult<i64>, operation: &str) -> AppResult<()> {
    audit.map(|_| ()).map_err(|error| {
        AppError::new(
            crate::models::ErrorClass::Internal,
            format!(
                "The {operation} committed on the server, but the local audit write failed: {}. Refresh before retrying.",
                error.message
            ),
        )
    })
}

async fn writable_table(
    state: &AppState,
    config: &ConnectionConfig,
    password: Option<String>,
    schema: &str,
    table: &str,
) -> AppResult<crate::models::TableMetadata> {
    let snapshot = state.sql.metadata(config, password, MetadataRequest {
        connection_id: config.id.clone(), schema: Some(schema.into()), table: Some(table.into()),
        include_tables: false, include_columns: false,
    }).await?;
    let metadata = snapshot.databases.into_iter().flat_map(|database| database.tables)
        .find(|item| item.name == table)
        .ok_or_else(|| AppError::validation("The selected table no longer exists."))?;
    if metadata.table_type.to_ascii_uppercase().contains("VIEW") {
        return Err(AppError::validation("Grid writes are only available for base tables."));
    }
    Ok(metadata)
}

fn validate_row_key(metadata: &crate::models::TableMetadata, columns: &[String], values: &[serde_json::Value]) -> AppResult<()> {
    let primary = metadata.columns.iter().filter(|column| column.is_primary_key).map(|column| &column.name).collect::<Vec<_>>();
    if primary.is_empty() || columns.len() != primary.len() || values.len() != columns.len()
        || primary.iter().any(|name| columns.iter().filter(|column| column.as_str() == name.as_str()).count() != 1)
        || values.iter().any(serde_json::Value::is_null)
    {
        return Err(AppError::validation("Every primary-key column requires one non-null value to identify a row safely."));
    }
    Ok(())
}

fn sql_placeholder(protocol: &Protocol, index: usize) -> String {
    if *protocol == Protocol::Postgres { format!("${index}") } else { "?".into() }
}

fn row_predicate(protocol: &Protocol, columns: &[String], first: usize) -> String {
    columns.iter().enumerate().map(|(index, column)| {
        format!("{} = {}", quote_identifier(protocol, column), sql_placeholder(protocol, first + index))
    }).collect::<Vec<_>>().join(" AND ")
}

#[tauri::command]
pub async fn sql_update_cell(
    state: State<'_, Arc<AppState>>, connection_id: String, schema: String, table: String,
    key_columns: Vec<String>, key_values: Vec<serde_json::Value>, column: String,
    value: serde_json::Value, operation_id: String,
) -> AppResult<ExecuteResult> {
    let state = state_ref(&state);
    state.ensure_running()?;
    let config = state.connection(&connection_id)?;
    let password = state.connection_password(&config)?;
    let metadata = writable_table(&state, &config, password.clone(), &schema, &table).await?;
    validate_row_key(&metadata, &key_columns, &key_values)?;
    if !metadata.columns.iter().any(|item| item.name == column) {
        return Err(AppError::validation("The selected column no longer exists."));
    }
    let qualified = qualified_table(&config.protocol, &schema, &table)?;
    let sql = format!("UPDATE {qualified} SET {} = {} WHERE {}", quote_identifier(&config.protocol, &column),
        sql_placeholder(&config.protocol, 1), row_predicate(&config.protocol, &key_columns, 2));
    let key_params = key_values.into_iter().map(json_to_param).collect::<AppResult<Vec<_>>>()?;
    let mut params = vec![json_to_param(value)?];
    params.extend(key_params.clone());
    let verify_operation_id = format!("{operation_id}-verify");
    let mut result = state.sql.execute(&config, password.clone(), SqlRequest {
        connection_id: connection_id.clone(), sql, params, operation_id, page_size: None, offset: None,
        schema: Some(schema.clone()),
    }).await?;
    if result.rows_affected == 0 {
        let exists = state.sql.fetch(&config, password, SqlRequest {
            connection_id: connection_id.clone(),
            sql: format!("SELECT 1 FROM {qualified} WHERE {}", row_predicate(&config.protocol, &key_columns, 1)),
            params: key_params,
            operation_id: verify_operation_id,
            page_size: Some(1),
            offset: Some(0),
            schema: Some(schema.clone()),
        }).await?;
        if exists.rows.is_empty() {
            return Err(AppError::new(crate::models::ErrorClass::Conflict,
                "The row changed or was removed. Refresh the table before editing again."));
        }
        result.rows_affected = 1;
    } else if result.rows_affected != 1 {
        return Err(AppError::new(crate::models::ErrorClass::Conflict,
            "The update matched more than one row. Refresh the table before editing again."));
    }
    audited_commit(
        state.store.append_audit(Some(&connection_id), Some(&config.name), Some(config.protocol.as_str()),
            "data_update", &format!("UPDATE {schema}.{table} SET {column} by primary key"), Some(1), "success", None),
        "row update",
    )?;
    Ok(result)
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RowEditChange {
    pub column: String,
    pub value: serde_json::Value,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RowEdit {
    pub key_columns: Vec<String>,
    pub key_values: Vec<serde_json::Value>,
    pub changes: Vec<RowEditChange>,
}
#[tauri::command]
pub async fn sql_apply_row_edits(
    state: State<'_, Arc<AppState>>, connection_id: String, schema: String, table: String,
    rows: Vec<RowEdit>, operation_id: String,
) -> AppResult<ExecuteResult> {
    let state = state_ref(&state);
    apply_row_edits(&state, &connection_id, &schema, &table, &rows, &operation_id).await
}

pub(crate) async fn apply_row_edits(
    state: &Arc<AppState>, connection_id: &str, schema: &str, table: &str,
    rows: &[RowEdit], operation_id: &str,
) -> AppResult<ExecuteResult> {
    state.ensure_running()?;
    if rows.is_empty() || rows.len() > 500 {
        return Err(AppError::validation("Apply between 1 and 500 edited rows per batch."));
    }
    let config = state.connection(connection_id)?;
    let password = state.connection_password(&config)?;
    let metadata = writable_table(state, &config, password.clone(), schema, table).await?;
    let qualified = qualified_table(&config.protocol, schema, table)?;
    let mut statements = Vec::with_capacity(rows.len());
    for (row_index, row) in rows.iter().enumerate() {
        validate_row_key(&metadata, &row.key_columns, &row.key_values)?;
        if row.changes.is_empty() {
            return Err(AppError::validation("Each edited row needs at least one changed column."));
        }
        let mut params = Vec::new();
        let mut assignments = Vec::new();
        for change in &row.changes {
            if !metadata.columns.iter().any(|item| item.name == change.column) {
                return Err(AppError::validation(format!("Column '{}' no longer exists.", change.column)));
            }
            assignments.push(format!("{} = {}", quote_identifier(&config.protocol, &change.column), sql_placeholder(&config.protocol, params.len() + 1)));
            params.push(json_to_param(change.value.clone())?);
        }
        let predicate = row_predicate(&config.protocol, &row.key_columns, params.len() + 1);
        for value in &row.key_values {
            params.push(json_to_param(value.clone())?);
        }
        statements.push(SqlRequest {
            connection_id: connection_id.to_string(),
            sql: format!("UPDATE {qualified} SET {} WHERE {}", assignments.join(", "), predicate),
            params,
            operation_id: format!("{operation_id}-row-{row_index}"),
            page_size: None,
            offset: None,
            schema: Some(schema.to_string()),
        });
    }
    let row_count = statements.len();
    let results = state.sql.execute_batch(&config, password, crate::models::SqlBatchRequest {
        connection_id: connection_id.to_string(), statements, operation_id: operation_id.to_string(), wrap_in_transaction: true,
    }).await?;
    let affected = results.iter().map(|result| result.rows_affected).sum::<u64>();
    let duration_ms = results.iter().map(|result| result.duration_ms).sum::<u64>();
    audited_commit(
        state.store.append_audit(Some(connection_id), Some(&config.name), Some(config.protocol.as_str()),
            "data_update", &format!("UPDATE {schema}.{table} staged edits for {row_count} rows"), Some(affected as i64), "success", None),
        "row edits",
    )?;
    Ok(ExecuteResult { operation_id: operation_id.to_string(), rows_affected: affected, last_insert_id: None, duration_ms, committed: true })
}

#[tauri::command]
pub async fn sql_delete_rows(
    state: State<'_, Arc<AppState>>, connection_id: String, schema: String, table: String,
    key_columns: Vec<String>, key_values: Vec<Vec<serde_json::Value>>, operation_id: String,
) -> AppResult<ExecuteResult> {
    let state = state_ref(&state);
    state.ensure_running()?;
    if key_values.is_empty() || key_values.len() > 500 {
        return Err(AppError::validation("Select between 1 and 500 rows for a confirmed delete."));
    }
    let config = state.connection(&connection_id)?;
    let password = state.connection_password(&config)?;
    let metadata = writable_table(&state, &config, password.clone(), &schema, &table).await?;
    for values in &key_values { validate_row_key(&metadata, &key_columns, values)?; }
    let qualified = qualified_table(&config.protocol, &schema, &table)?;
    let predicates = key_values.iter().enumerate().map(|(index, _)| {
        format!("({})", row_predicate(&config.protocol, &key_columns, index * key_columns.len() + 1))
    }).collect::<Vec<_>>().join(" OR ");
    let params = key_values.into_iter().flatten().map(json_to_param).collect::<AppResult<Vec<_>>>()?;
    let result = state.sql.execute(&config, password, SqlRequest {
        connection_id: connection_id.clone(), sql: format!("DELETE FROM {qualified} WHERE {predicates}"), params,
        operation_id, page_size: None, offset: None, schema: Some(schema.clone()),
    }).await?;
    audited_commit(
        state.store.append_audit(Some(&connection_id), Some(&config.name), Some(config.protocol.as_str()),
            "data_delete", &format!("DELETE {schema}.{table} selected primary keys"),
            Some(result.rows_affected as i64), "success", None),
        "row deletion",
    )?;
    Ok(result)
}

#[tauri::command]
pub async fn sql_insert_row(
    state: State<'_, Arc<AppState>>, connection_id: String, schema: String, table: String,
    columns: Vec<String>, values: Vec<serde_json::Value>, operation_id: String,
) -> AppResult<ExecuteResult> {
    let state = state_ref(&state);
    state.ensure_running()?;
    let config = state.connection(&connection_id)?;
    let password = state.connection_password(&config)?;
    let metadata = writable_table(&state, &config, password.clone(), &schema, &table).await?;
    if columns.len() != values.len() || columns.iter().enumerate().any(|(index, name)|
        columns[..index].contains(name) || !metadata.columns.iter().any(|column| column.name == *name))
    {
        return Err(AppError::validation("Provide one value per distinct table column."));
    }
    let qualified = qualified_table(&config.protocol, &schema, &table)?;
    let sql = if columns.is_empty() {
        if config.protocol == Protocol::Mysql { format!("INSERT INTO {qualified} () VALUES ()") }
        else { format!("INSERT INTO {qualified} DEFAULT VALUES") }
    } else {
        format!("INSERT INTO {qualified} ({}) VALUES ({})", columns.iter().map(|name| quote_identifier(&config.protocol, name)).collect::<Vec<_>>().join(", "),
            (1..=values.len()).map(|index| sql_placeholder(&config.protocol, index)).collect::<Vec<_>>().join(", "))
    };
    let result = state.sql.execute(&config, password, SqlRequest {
        connection_id: connection_id.clone(), sql,
        params: values.into_iter().map(json_to_param).collect::<AppResult<Vec<_>>>()?, operation_id,
        page_size: None, offset: None, schema: Some(schema.clone()),
    }).await?;
    audited_commit(
        state.store.append_audit(Some(&connection_id), Some(&config.name), Some(config.protocol.as_str()),
            "data_insert", &format!("INSERT {schema}.{table}"), Some(result.rows_affected as i64), "success", None),
        "row insertion",
    )?;
    Ok(result)
}

#[tauri::command]
pub async fn sql_table_preview(
    state: State<'_, Arc<AppState>>,
    connection_id: String,
    schema: String,
    table: String,
    limit: Option<u32>,
    offset: Option<u64>,
    operation_id: Option<String>,
    filter: Option<String>,
    order_by: Option<String>,
) -> AppResult<QueryResult> {
    let state = state_ref(&state);
    state.ensure_running()?;
    let config = state.connection(&connection_id)?;
    let password = state.connection_password(&config)?;
    let qualified = match config.protocol {
        Protocol::Mysql => format!(
            "`{}`.`{}`",
            schema.replace('`', "``"),
            table.replace('`', "``")
        ),
        Protocol::Postgres => format!(
            "\"{}\".\"{}\"",
            schema.replace('"', "\"\""),
            table.replace('"', "\"\"")
        ),
        Protocol::Sqlite => format!("\"{}\"", table.replace('"', "\"\"")),
        Protocol::Redis | Protocol::Elasticsearch => {
            return Err(AppError::unsupported(
                "Table preview is not available for the Redis KV or Elasticsearch search protocol.",
            ))
        }
    };
    let page_size = limit.unwrap_or(500).clamp(1, 10_000);
    let offset = offset.unwrap_or(0);
    // The filter and sort are user-authored SQL fragments (the same trust level as the query
    // editor); the composed statement must still be a single, session-neutral statement.
    let mut preview_sql = format!("SELECT * FROM {qualified}");
    if let Some(filter) = filter.as_deref().map(str::trim).filter(|value| !value.is_empty()) {
        preview_sql.push_str(&format!(" WHERE ({filter})"));
    }
    if let Some(order_by) = order_by.as_deref().map(str::trim).filter(|value| !value.is_empty()) {
        preview_sql.push_str(&format!(" ORDER BY {order_by}"));
    }
    if sql_statement_count(&preview_sql) != 1 {
        return Err(AppError::validation("The filter or sort must not contain additional statements."));
    }
    reject_session_state_statement(&preview_sql)?;
    let request = SqlRequest {
        connection_id: connection_id.clone(),
        sql: preview_sql,
        params: Vec::new(),
        operation_id: operation_id
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| format!("preview-{}", uuid::Uuid::new_v4())),
        page_size: Some(page_size),
        offset: Some(offset),
        schema: None,
    };
    let result = state.sql.fetch(&config, password, request).await?;
    state.store.append_audit(
        Some(&connection_id),
        Some(&config.name),
        Some(config.protocol.as_str()),
        "table_preview",
        &format!("Preview {schema}.{table}"),
        Some(result.rows.len() as i64),
        "success",
        None,
    )?;
    Ok(result)
}

#[tauri::command]
pub async fn sql_table_ddl(
    state: State<'_, Arc<AppState>>,
    connection_id: String,
    schema: String,
    table: String,
) -> AppResult<String> {
    let state = state_ref(&state);
    state.ensure_running()?;
    let config = state.connection(&connection_id)?;
    if config.protocol.family() != DriverFamily::Sql {
        return Err(AppError::unsupported("Table DDL is not available for the Redis KV protocol."));
    }
    let password = state.connection_password(&config)?;
    state.sql.ddl(&config, password, &schema, &table).await
}

#[tauri::command]
pub async fn sql_object_stats(
    state: State<'_, Arc<AppState>>,
    connection_id: String,
    schema: String,
) -> AppResult<Vec<crate::models::ObjectStat>> {
    let state = state_ref(&state);
    state.ensure_running()?;
    let config = state.connection(&connection_id)?;
    if config.protocol.family() != DriverFamily::Sql {
        return Err(AppError::unsupported("The object list is only available for SQL connections."));
    }
    let password = state.connection_password(&config)?;
    state.sql.object_stats(&config, password, &schema).await
}

#[tauri::command]
pub async fn sql_drop_table(
    state: State<'_, Arc<AppState>>,
    connection_id: String,
    schema: String,
    table: String,
    operation_id: String,
) -> AppResult<Vec<ExecuteResult>> {
    let state = state_ref(&state);
    state.ensure_running()?;
    if table.trim().is_empty() {
        return Err(AppError::validation("A table name is required for DROP TABLE."));
    }
    let config = state.connection(&connection_id)?;
    let password = state.connection_password(&config)?;
    let qualified = qualified_table(&config.protocol, &schema, &table)?;
    let result = state
        .sql
        .execute_batch(
            &config,
            password,
            SqlBatchRequest {
                connection_id: connection_id.clone(),
                operation_id: operation_id.clone(),
                wrap_in_transaction: true,
                statements: vec![SqlRequest {
                    connection_id: connection_id.clone(),
                    sql: format!("DROP TABLE {qualified}"),
                    params: Vec::new(),
                    operation_id: format!("{operation_id}-1"),
                    page_size: None,
                    offset: None,
                    schema: None,
                }],
            },
        )
        .await;
    state.store.append_audit(
        Some(&connection_id),
        Some(&config.name),
        Some(config.protocol.as_str()),
        "ddl_drop",
        &format!("DROP TABLE {schema}.{table}"),
        result
            .as_ref()
            .ok()
            .map(|values| values.iter().map(|value| value.rows_affected as i64).sum()),
        if result.is_ok() { "success" } else { "failure" },
        result.as_ref().err().map(|error| error.message.as_str()),
    )?;
    result
}

#[tauri::command]
pub async fn sql_cancel(
    state: State<'_, Arc<AppState>>,
    connection_id: String,
    operation_id: String,
) -> AppResult<()> {
    let state = state_ref(&state);
    state.sql.cancel(&connection_id, &operation_id).await?;
    let config = state.connection(&connection_id).ok();
    state.store.append_audit(
        Some(&connection_id),
        config.as_ref().map(|value| value.name.as_str()),
        config.as_ref().map(|value| value.protocol.as_str()),
        "query_cancel",
        &format!("Cancelled operation {operation_id}"),
        None,
        "cancelled",
        None,
    )?;
    Ok(())
}

#[tauri::command]
pub async fn sql_metadata(
    state: State<'_, Arc<AppState>>,
    connection_id: String,
    request: MetadataRequest,
) -> AppResult<MetadataSnapshot> {
    let state = state_ref(&state);
    let config = state.connection(&connection_id)?;
    let password = state.connection_password(&config)?;
    state.sql.metadata(&config, password, request).await
}

#[tauri::command]
pub async fn list_identities(
    state: State<'_, Arc<AppState>>,
    connection_id: String,
) -> AppResult<IdentitySnapshot> {
    let state = state_ref(&state);
    let config = state.connection(&connection_id)?;
    match config.protocol {
        Protocol::Sqlite => Err(AppError::unsupported(
            "SQLite is an embedded file database and has no server-level users, roles or grants.",
        )),
        Protocol::Mysql | Protocol::Postgres => {
            let password = state.connection_password(&config)?;
            let sql = if config.protocol == Protocol::Mysql {
                "SELECT User, Host, account_locked FROM mysql.user ORDER BY User, Host"
            } else {
                "SELECT rolname, rolsuper, rolcanlogin, rolvaliduntil FROM pg_roles ORDER BY rolname"
            };
            let result = state
                .sql
                .fetch(
                    &config,
                    password,
                    SqlRequest {
                        connection_id: connection_id.clone(),
                        sql: sql.into(),
                        params: Vec::new(),
                        operation_id: format!("identities-{}", uuid::Uuid::new_v4()),
                        page_size: Some(1_000),
                        offset: Some(0),
                        schema: None,
                    },
                )
                .await?;
            Ok(IdentitySnapshot {
                columns: result.columns.into_iter().map(|column| column.name).collect(),
                rows: result.rows,
                note: Some("身份列表来自当前服务器；创建、删除、密码轮换和 GRANT/REVOKE 均需二次确认并写入只追加审计，密码字面量会从审计摘要中隐藏。".into()),
            })
        }
        Protocol::Redis => {
            let password = state.connection_password(&config)?;
            let result = state
                .kv
                .execute_raw(
                    &config,
                    password,
                    RedisCommandRequest {
                        connection_id: connection_id.clone(),
                        command: "ACL".into(),
                        args: vec!["LIST".into()],
                        operation_id: format!("acl-{}", uuid::Uuid::new_v4()),
                        confirmed_dangerous: false,
                    },
                )
                .await?;
            let rows = match result.value {
                serde_json::Value::Array(items) => items
                    .into_iter()
                    .map(|item| vec![item])
                    .collect::<Vec<_>>(),
                value => vec![vec![value]],
            };
            Ok(IdentitySnapshot {
                columns: vec!["ACL user".into()],
                rows,
                note: Some("Redis 展示 ACL LIST；ACL SETUSER/DELUSER 支持创建、启用、禁用、轮换密码和删除用户，密码参数会从审计摘要中隐藏。".into()),
            })
        }
        Protocol::Elasticsearch => Err(AppError::unsupported(
            "Elasticsearch security users/roles are managed via the _security API; use the REST console in this version.",
        )),
    }
}

#[tauri::command]
pub async fn export_data(
    state: State<'_, Arc<AppState>>,
    connection_id: String,
    format: String,
    path: String,
    schema: Option<String>,
    table: Option<String>,
    pattern: Option<String>,
) -> AppResult<u64> {
    let state = state_ref(&state);
    state.ensure_running()?;
    let config = state.connection(&connection_id)?;
    let password = state.connection_password(&config)?;
    let mut exported = 0u64;
    let audit_scope;
    match config.protocol.family() {
        DriverFamily::Kv => {
            if format != "jsonl" {
                return Err(AppError::validation(
                    "Redis export supports JSON Lines so each type is preserved without flattening.",
                ));
            }
            if let Some(parent) = Path::new(&path).parent() {
                std::fs::create_dir_all(parent)?;
            }
            let file = std::fs::File::create(&path)?;
            let mut writer = BufWriter::new(file);
            let mut cursor = 0u64;
            let scan_pattern = pattern.unwrap_or_else(|| "*".into());
            loop {
                let scan = state
                    .kv
                    .scan(
                        &config,
                        password.clone(),
                        RedisScanRequest {
                            connection_id: connection_id.clone(),
                            cursor,
                            pattern: scan_pattern.clone(),
                            count: 500,
                            type_filter: None,
                            db: config.redis_db,
                            operation_id: format!("export-scan-{}", uuid::Uuid::new_v4()),
                        },
                    )
                    .await?;
                for key in scan.keys {
                    let raw = redis_raw_key(&key.key, key.key_base64.as_deref())?;
                    let value = state
                        .kv
                        .get(
                            &config,
                            password.clone(),
                            &raw,
                            config.redis_db,
                            &format!("export-key-{}", uuid::Uuid::new_v4()),
                        )
                        .await?;
                    serde_json::to_writer(&mut writer, &value)?;
                    writer.write_all(b"\n")?;
                    exported += 1;
                }
                cursor = scan.cursor;
                if cursor == 0 {
                    break;
                }
            }
            writer.flush()?;
            audit_scope = format!("pattern {scan_pattern}");
        }
        DriverFamily::Sql => {
            let schema = schema
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| AppError::validation("SQL export requires a schema."))?;
            let table = table.filter(|value| !value.trim().is_empty());
            audit_scope = table
                .as_deref()
                .map(|value| format!("table {schema}.{value}"))
                .unwrap_or_else(|| format!("database {schema}"));
            let result = state
                .sql
                .export_snapshot(
                    &config,
                    password,
                    SqlExportRequest {
                        connection_id: connection_id.clone(),
                        schema,
                        format: format.clone(),
                        path: path.clone(),
                        table,
                        operation_id: format!("export-snapshot-{}", uuid::Uuid::new_v4()),
                    },
                )
                .await?;
            exported = result.rows_exported;
        }
        DriverFamily::Search => {
            return Err(AppError::unsupported(
                "Elasticsearch export is not available in this version. Use the REST console with _search / scroll instead.",
            ));
        }
    }
    state.store.append_audit(
        Some(&connection_id),
        Some(&config.name),
        Some(config.protocol.as_str()),
        "data_export",
        &format!(
            "Consistent streaming export ({audit_scope}, {exported} rows, {format}) to {}",
            Path::new(&path)
                .file_name()
                .and_then(|value| value.to_str())
                .unwrap_or("output")
        ),
        Some(exported as i64),
        "success",
        None,
    )?;
    Ok(exported)
}

#[tauri::command]
pub async fn import_data(
    state: State<'_, Arc<AppState>>,
    connection_id: String,
    format: String,
    path: String,
    schema: Option<String>,
    table: Option<String>,
    batch_size: Option<u32>,
) -> AppResult<u64> {
    let state = state_ref(&state);
    state.ensure_running()?;
    let config = state.connection(&connection_id)?;
    let password = state.connection_password(&config)?;
    let mut imported = 0u64;
    match config.protocol.family() {
        DriverFamily::Kv => {
            if format != "jsonl" {
                return Err(AppError::validation(
                    "Redis import supports the JSON Lines files produced by Redis export.",
                ));
            }
            let reader = BufReader::new(std::fs::File::open(&path)?);
            for line in reader.lines() {
                let line = line?;
                if line.trim().is_empty() {
                    continue;
                }
                let value: RedisValue = serde_json::from_str(&line)?;
                let key = value.key.clone();
                state
                    .kv
                    .set(
                        &config,
                        password.clone(),
                        key.as_bytes(),
                        config.redis_db,
                        value,
                        &format!("import-{}", uuid::Uuid::new_v4()),
                    )
                    .await
                    .map_err(|error| {
                        AppError::new(
                            error.class,
                            format!("Redis import stopped after {imported} keys at '{key}': {}", error.message),
                        )
                    })?;
                imported += 1;
            }
        }
        DriverFamily::Sql => {
            let table = table.ok_or_else(|| AppError::validation("SQL import requires a table."))?;
            imported = state
                .sql
                .stream_import(
                    &config,
                    password,
                    SqlImportRequest {
                        connection_id: connection_id.clone(),
                        format,
                        path: path.clone(),
                        schema,
                        table,
                        operation_id: format!("import-{}", uuid::Uuid::new_v4()),
                        batch_size: batch_size.unwrap_or(2_000),
                    },
                )
                .await?;
        }
        DriverFamily::Search => {
            return Err(AppError::unsupported(
                "Elasticsearch import is not available in this version. Use the REST console with the _bulk API instead.",
            ));
        }
    }
    state.store.append_audit(
        Some(&connection_id),
        Some(&config.name),
        Some(config.protocol.as_str()),
        "data_import",
        &format!("Imported {} rows/keys from {}", imported, Path::new(&path).file_name().and_then(|value| value.to_str()).unwrap_or("file")),
        Some(imported as i64),
        "success",
        None,
    )?;
    Ok(imported)
}

#[tauri::command]
pub async fn sql_pool_metrics(
    state: State<'_, Arc<AppState>>,
    connection_id: String,
) -> AppResult<PoolMetrics> {
    let state = state_ref(&state);
    state.sql.metrics(&connection_id).await
}

#[tauri::command]
pub async fn redis_scan(
    state: State<'_, Arc<AppState>>,
    connection_id: String,
    request: RedisScanRequest,
) -> AppResult<RedisScanResult> {
    let state = state_ref(&state);
    state.ensure_running()?;
    let config = state.connection(&connection_id)?;
    let password = state.connection_password(&config)?;
    let result = state.kv.scan(&config, password, request.clone()).await;
    state.store.append_audit(
        Some(&connection_id),
        Some(&config.name),
        Some("redis"),
        "redis_scan",
        &format!(
            "SCAN {} MATCH {} COUNT {}",
            request.cursor, request.pattern, request.count
        ),
        result.as_ref().ok().map(|value| value.scanned as i64),
        if result.is_ok() { "success" } else { "failure" },
        result.as_ref().err().map(|error| error.message.as_str()),
    )?;
    result
}

#[tauri::command]
pub async fn redis_get(
    state: State<'_, Arc<AppState>>,
    connection_id: String,
    key: String,
    key_base64: Option<String>,
    db: u32,
    operation_id: String,
) -> AppResult<RedisValue> {
    let state = state_ref(&state);
    state.ensure_running()?;
    let config = state.connection(&connection_id)?;
    let password = state.connection_password(&config)?;
    let raw = redis_raw_key(&key, key_base64.as_deref())?;
    state
        .kv
        .get(&config, password, &raw, db, &operation_id)
        .await
}

#[tauri::command]
pub async fn redis_create(
    state: State<'_, Arc<AppState>>, connection_id: String, db: u32, value: RedisValue, operation_id: String,
) -> AppResult<()> {
    let state = state_ref(&state);
    state.ensure_running()?;
    let config = state.connection(&connection_id)?;
    let password = state.connection_password(&config)?;
    let key = value.key.clone();
    state.kv.create(&config, password, db, value, &operation_id).await?;
    audited_commit(
        state.store.append_audit(Some(&connection_id), Some(&config.name), Some(config.protocol.as_str()),
            "redis_create", &format!("Create key {key} in DB {db}"), Some(1), "success", None),
        "Redis key creation",
    )?;
    Ok(())
}

#[tauri::command]
pub async fn redis_set(
    state: State<'_, Arc<AppState>>,
    connection_id: String,
    db: u32,
    value: RedisValue,
    key_base64: Option<String>,
    operation_id: String,
) -> AppResult<()> {
    let state = state_ref(&state);
    state.ensure_running()?;
    let config = state.connection(&connection_id)?;
    let password = state.connection_password(&config)?;
    let summary = format!("Atomic replace {} ({}) in DB {db}", value.key, value.key_type);
    let raw = redis_raw_key(&value.key, key_base64.as_deref())?;
    let result = state
        .kv
        .set(&config, password, &raw, db, value, &operation_id)
        .await;
    state.store.append_audit(
        Some(&connection_id),
        Some(&config.name),
        Some("redis"),
        "redis_set",
        &summary,
        result.as_ref().ok().map(|_| 1),
        if result.is_ok() { "success" } else { "failure" },
        result.as_ref().err().map(|error| error.message.as_str()),
    )?;
    result
}

#[tauri::command]
pub async fn redis_delete(
    state: State<'_, Arc<AppState>>,
    connection_id: String,
    keys: Vec<String>,
    keys_base64: Option<Vec<String>>,
    db: u32,
    confirmed_dangerous: bool,
    operation_id: String,
) -> AppResult<u64> {
    let state = state_ref(&state);
    state.ensure_running()?;
    let config = state.connection(&connection_id)?;
    let password = state.connection_password(&config)?;
    let raw_keys: Vec<Vec<u8>> = match keys_base64 {
        Some(b64s) => b64s
            .iter()
            .map(|b64| B64.decode(b64).map_err(|e| AppError::validation(format!("Invalid base64 key: {e}"))))
            .collect::<AppResult<Vec<_>>>()?,
        None => keys.iter().map(|key| key.as_bytes().to_vec()).collect(),
    };
    let result = state
        .kv
        .del(
            &config,
            password,
            &raw_keys,
            db,
            confirmed_dangerous,
            &operation_id,
        )
        .await;
    state.store.append_audit(
        Some(&connection_id),
        Some(&config.name),
        Some("redis"),
        "redis_danger",
        &format!("DEL {} (DB {db})", keys.join(" ")),
        result.as_ref().ok().map(|value| *value as i64),
        if result.is_ok() { "confirmed" } else { "refused" },
        result.as_ref().err().map(|error| error.message.as_str()),
    )?;
    result
}

#[tauri::command]
pub async fn redis_info(
    state: State<'_, Arc<AppState>>,
    connection_id: String,
    operation_id: String,
) -> AppResult<RedisInfo> {
    let state = state_ref(&state);
    state.ensure_running()?;
    let config = state.connection(&connection_id)?;
    let password = state.connection_password(&config)?;
    state.kv.info(&config, password, &operation_id).await
}

#[tauri::command]
pub async fn redis_dbsize(
    state: State<'_, Arc<AppState>>,
    connection_id: String,
    db: u32,
    operation_id: String,
) -> AppResult<u64> {
    let state = state_ref(&state);
    state.ensure_running()?;
    let config = state.connection(&connection_id)?;
    let password = state.connection_password(&config)?;
    state.kv.dbsize(&config, password, db, &operation_id).await
}

#[tauri::command]
pub async fn redis_command(
    state: State<'_, Arc<AppState>>,
    connection_id: String,
    request: RedisCommandRequest,
) -> AppResult<RedisCommandResult> {
    let state = state_ref(&state);
    let config = state.connection(&connection_id)?;
    let password = state.connection_password(&config)?;
    let summary = summarize_redis_command(&request.command, &request.args);
    let result = state.kv.execute_raw(&config, password, request).await;
    let kind = if summary.starts_with("FLUSH")
        || summary.starts_with("CONFIG SET")
        || summary.starts_with("SHUTDOWN")
        || summary.starts_with("DEL ")
        || summary.starts_with("UNLINK ")
    {
        "redis_danger"
    } else {
        "redis_command"
    };
    state.store.append_audit(
        Some(&connection_id),
        Some(&config.name),
        Some("redis"),
        kind,
        &summary,
        None,
        if result.is_ok() { "success" } else { "failure" },
        result.as_ref().err().map(|error| error.message.as_str()),
    )?;
    result
}

#[tauri::command]
pub async fn redis_cancel(
    state: State<'_, Arc<AppState>>,
    connection_id: String,
    operation_id: String,
) -> AppResult<()> {
    let state = state_ref(&state);
    state.kv.cancel(&connection_id, &operation_id).await?;
    let config = state.connection(&connection_id).ok();
    state.store.append_audit(
        Some(&connection_id),
        config.as_ref().map(|value| value.name.as_str()),
        Some("redis"),
        "redis_cancel",
        &format!("Cancelled Redis operation {operation_id}"),
        None,
        "cancelled",
        None,
    )?;
    Ok(())
}

#[tauri::command]
pub async fn es_info(
    state: State<'_, Arc<AppState>>,
    connection_id: String,
    operation_id: String,
) -> AppResult<EsClusterInfo> {
    let state = state_ref(&state);
    state.ensure_running()?;
    let config = state.connection(&connection_id)?;
    let password = state.connection_password(&config)?;
    state.search.info(&config, password, &operation_id).await
}

#[tauri::command]
pub async fn es_indices(
    state: State<'_, Arc<AppState>>,
    connection_id: String,
    operation_id: String,
) -> AppResult<Vec<EsIndex>> {
    let state = state_ref(&state);
    state.ensure_running()?;
    let config = state.connection(&connection_id)?;
    let password = state.connection_password(&config)?;
    state.search.list_indices(&config, password, &operation_id).await
}

#[tauri::command]
pub async fn es_create_index(
    state: State<'_, Arc<AppState>>,
    connection_id: String,
    name: String,
    body: serde_json::Value,
    operation_id: String,
) -> AppResult<()> {
    let state = state_ref(&state);
    state.ensure_running()?;
    let config = state.connection(&connection_id)?;
    let password = state.connection_password(&config)?;
    let result = state.search.create_index(&config, password, &name, body, &operation_id).await;
    state.store.append_audit(
        Some(&connection_id), Some(&config.name), Some("elasticsearch"), "es_ddl",
        &format!("CREATE INDEX {name}"),
        result.as_ref().ok().map(|_| 1),
        if result.is_ok() { "success" } else { "failure" },
        result.as_ref().err().map(|error| error.message.as_str()),
    )?;
    result
}

#[tauri::command]
pub async fn es_delete_index(
    state: State<'_, Arc<AppState>>,
    connection_id: String,
    name: String,
    confirmed_dangerous: bool,
    operation_id: String,
) -> AppResult<()> {
    let state = state_ref(&state);
    state.ensure_running()?;
    let config = state.connection(&connection_id)?;
    let password = state.connection_password(&config)?;
    let result = state.search.delete_index(&config, password, &name, confirmed_dangerous, &operation_id).await;
    state.store.append_audit(
        Some(&connection_id), Some(&config.name), Some("elasticsearch"), "es_danger",
        &format!("DELETE INDEX {name}"),
        result.as_ref().ok().map(|_| 1),
        if result.is_ok() { "confirmed" } else { "refused" },
        result.as_ref().err().map(|error| error.message.as_str()),
    )?;
    result
}

#[tauri::command]
pub async fn es_mapping(
    state: State<'_, Arc<AppState>>,
    connection_id: String,
    name: String,
    operation_id: String,
) -> AppResult<serde_json::Value> {
    let state = state_ref(&state);
    state.ensure_running()?;
    let config = state.connection(&connection_id)?;
    let password = state.connection_password(&config)?;
    state.search.get_mapping(&config, password, &name, &operation_id).await
}

#[tauri::command]
pub async fn es_aliases(
    state: State<'_, Arc<AppState>>,
    connection_id: String,
    operation_id: String,
) -> AppResult<Vec<EsAlias>> {
    let state = state_ref(&state);
    state.ensure_running()?;
    let config = state.connection(&connection_id)?;
    let password = state.connection_password(&config)?;
    state.search.list_aliases(&config, password, &operation_id).await
}

#[tauri::command]
pub async fn es_alias_modify(
    state: State<'_, Arc<AppState>>,
    connection_id: String,
    actions: serde_json::Value,
    confirmed_dangerous: bool,
    operation_id: String,
) -> AppResult<()> {
    let state = state_ref(&state);
    state.ensure_running()?;
    let config = state.connection(&connection_id)?;
    let password = state.connection_password(&config)?;
    let result = state.search.modify_aliases(&config, password, actions.clone(), confirmed_dangerous, &operation_id).await;
    state.store.append_audit(
        Some(&connection_id), Some(&config.name), Some("elasticsearch"), "es_danger",
        &format!("ALIAS ACTIONS {}", actions),
        result.as_ref().ok().map(|_| 1),
        if result.is_ok() { "confirmed" } else { "refused" },
        result.as_ref().err().map(|error| error.message.as_str()),
    )?;
    result
}

#[tauri::command]
pub async fn es_search(
    state: State<'_, Arc<AppState>>,
    connection_id: String,
    request: EsSearchRequest,
) -> AppResult<EsSearchResult> {
    let state = state_ref(&state);
    state.ensure_running()?;
    let config = state.connection(&connection_id)?;
    let password = state.connection_password(&config)?;
    let index = request.index.clone();
    let result = state.search.search(&config, password, request).await;
    state.store.append_audit(
        Some(&connection_id), Some(&config.name), Some("elasticsearch"), "es_search",
        &format!("SEARCH {index}"),
        result.as_ref().ok().map(|value| value.hits.len() as i64),
        if result.is_ok() { "success" } else { "failure" },
        result.as_ref().err().map(|error| error.message.as_str()),
    )?;
    result
}

#[tauri::command]
pub async fn es_get_document(
    state: State<'_, Arc<AppState>>,
    connection_id: String,
    index: String,
    id: String,
    operation_id: String,
) -> AppResult<EsDocument> {
    let state = state_ref(&state);
    state.ensure_running()?;
    let config = state.connection(&connection_id)?;
    let password = state.connection_password(&config)?;
    state.search.get_document(&config, password, &index, &id, &operation_id).await
}

#[tauri::command]
pub async fn es_put_document(
    state: State<'_, Arc<AppState>>,
    connection_id: String,
    index: String,
    id: String,
    source: serde_json::Value,
    operation_id: String,
) -> AppResult<EsDocument> {
    let state = state_ref(&state);
    state.ensure_running()?;
    let config = state.connection(&connection_id)?;
    let password = state.connection_password(&config)?;
    let result = state.search.put_document(&config, password, &index, &id, source, &operation_id).await;
    state.store.append_audit(
        Some(&connection_id), Some(&config.name), Some("elasticsearch"), "es_write",
        &format!("INDEX DOCUMENT {index}/{}", if id.is_empty() { "<auto-id>" } else { &id }),
        result.as_ref().ok().map(|_| 1),
        if result.is_ok() { "success" } else { "failure" },
        result.as_ref().err().map(|error| error.message.as_str()),
    )?;
    result
}

#[tauri::command]
pub async fn es_delete_document(
    state: State<'_, Arc<AppState>>,
    connection_id: String,
    index: String,
    id: String,
    confirmed_dangerous: bool,
    operation_id: String,
) -> AppResult<()> {
    let state = state_ref(&state);
    state.ensure_running()?;
    let config = state.connection(&connection_id)?;
    let password = state.connection_password(&config)?;
    let result = state.search.delete_document(&config, password, &index, &id, confirmed_dangerous, &operation_id).await;
    state.store.append_audit(
        Some(&connection_id), Some(&config.name), Some("elasticsearch"), "es_danger",
        &format!("DELETE DOCUMENT {index}/{id}"),
        result.as_ref().ok().map(|_| 1),
        if result.is_ok() { "confirmed" } else { "refused" },
        result.as_ref().err().map(|error| error.message.as_str()),
    )?;
    result
}

#[tauri::command]
pub async fn es_rest(
    state: State<'_, Arc<AppState>>,
    connection_id: String,
    request: EsRestRequest,
) -> AppResult<EsRestResult> {
    let state = state_ref(&state);
    state.ensure_running()?;
    let config = state.connection(&connection_id)?;
    let password = state.connection_password(&config)?;
    let summary = format!("{} {}", request.method.to_ascii_uppercase(), request.path);
    let kind = if request.method.eq_ignore_ascii_case("DELETE") { "es_danger" } else { "es_rest" };
    let result = state.search.execute_rest(&config, password, request).await;
    state.store.append_audit(
        Some(&connection_id), Some(&config.name), Some("elasticsearch"), kind,
        &summary,
        None,
        if result.is_ok() { "success" } else { "failure" },
        result.as_ref().err().map(|error| error.message.as_str()),
    )?;
    result
}

#[tauri::command]
pub async fn es_cancel(
    state: State<'_, Arc<AppState>>,
    connection_id: String,
    operation_id: String,
) -> AppResult<()> {
    let state = state_ref(&state);
    state.search.cancel(&connection_id, &operation_id).await
}

#[tauri::command]
pub async fn recent_audit(
    state: State<'_, Arc<AppState>>,
    limit: Option<usize>,
) -> AppResult<Vec<AuditEntry>> {
    let state = state_ref(&state);
    state.store.recent_audit(limit.unwrap_or(100).min(1_000))
}

#[tauri::command]
pub async fn recent_history(
    state: State<'_, Arc<AppState>>,
    limit: Option<usize>,
) -> AppResult<Vec<QueryHistoryEntry>> {
    let state = state_ref(&state);
    state.store.recent_history(limit.unwrap_or(100).min(1_000))
}

#[tauri::command]
pub async fn verify_audit_chain(state: State<'_, Arc<AppState>>) -> AppResult<bool> {
    let state = state_ref(&state);
    state.store.verify_audit_chain()
}

/// One-time recovery of credentials an older LuxBase Studio build left in the OS keychain.
///
/// The app never reads the keychain by itself; this runs only when the user presses the recovery
/// button in the security center, and the read can block while macOS asks for permission.
#[tauri::command]
pub async fn import_legacy_keychain_secrets(
    state: State<'_, Arc<AppState>>,
) -> AppResult<LegacyImportReport> {
    let state = state_ref(&state);
    tokio::task::spawn_blocking(move || state.store.import_legacy_keychain_secrets())
        .await
        .map_err(|error| AppError::new(crate::models::ErrorClass::Internal, error.to_string()))?
}

#[tauri::command]
pub fn save_diagnostics_report(
    state: State<'_, Arc<AppState>>,
    path: String,
    report: serde_json::Value,
) -> AppResult<String> {
    const MAX_DIAGNOSTICS_BYTES: usize = 2 * 1024 * 1024;
    let mut path = PathBuf::from(path.trim());
    if path.as_os_str().is_empty() {
        return Err(AppError::validation("Choose a valid diagnostics report path."));
    }
    if path.extension().is_none() {
        path.set_extension("json");
    } else if !path
        .extension()
        .and_then(|value| value.to_str())
        .is_some_and(|value| value.eq_ignore_ascii_case("json"))
    {
        return Err(AppError::validation(
            "Diagnostics reports must be saved as a .json file.",
        ));
    }
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    if !parent.is_dir() {
        return Err(AppError::validation(
            "The selected diagnostics folder does not exist.",
        ));
    }
    let contents = serde_json::to_vec_pretty(&report)?;
    if contents.len() > MAX_DIAGNOSTICS_BYTES {
        return Err(AppError::validation(
            "The diagnostics report exceeds the 2 MB safety limit.",
        ));
    }
    let temporary = parent.join(format!(
        ".luxbase-diagnostics-{}.tmp",
        uuid::Uuid::new_v4()
    ));
    let write_result = (|| -> std::io::Result<()> {
        std::fs::write(&temporary, &contents)?;
        #[cfg(windows)]
        if path.exists() {
            std::fs::remove_file(&path)?;
        }
        std::fs::rename(&temporary, &path)
    })();
    if let Err(error) = write_result {
        let _ = std::fs::remove_file(&temporary);
        return Err(error.into());
    }
    let protocol = report
        .pointer("/activeDriver/protocol")
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned);
    state.store.append_audit(
        None,
        None,
        protocol.as_deref(),
        "diagnostics_export",
        "Exported redacted runtime diagnostics",
        None,
        "success",
        Some(r#"{"format":"json","redacted":true}"#),
    )?;
    Ok(path.to_string_lossy().into_owned())
}

#[tauri::command]
pub async fn test_saved_connection(
    state: State<'_, Arc<AppState>>,
    connection_id: String,
) -> AppResult<ConnectionTestResult> {
    let state = state_ref(&state);
    let config = state.connection(&connection_id)?;
    let password = state.connection_password(&config)?;
    let result = match config.protocol.family() {
        DriverFamily::Sql => state.sql.test(&config, password).await,
        DriverFamily::Kv => state.kv.test(&config, password).await,
        DriverFamily::Search => state.search.test(&config, password).await,
    };
    state.store.append_audit(
        Some(&connection_id),
        Some(&config.name),
        Some(config.protocol.as_str()),
        "connection_test",
        &format!("Test saved {} connection", config.protocol.as_str()),
        None,
        if result.ok { "success" } else { "failure" },
        result.message.as_deref(),
    )?;
    Ok(result)
}

#[tauri::command]
pub async fn delete_connection(
    state: State<'_, Arc<AppState>>,
    connection_id: String,
) -> AppResult<()> {
    let state = state_ref(&state);
    let config = state.connection(&connection_id)?;
    match config.protocol.family() {
        DriverFamily::Sql => state.sql.disconnect(&connection_id).await?,
        DriverFamily::Kv => state.kv.disconnect(&connection_id).await?,
        DriverFamily::Search => state.search.disconnect(&connection_id).await?,
    }
    state.store.delete_connection(&connection_id)?;
    state.forget_connection(&connection_id);
    state.store.append_audit(
        Some(&connection_id),
        Some(&config.name),
        Some(config.protocol.as_str()),
        "connection_deleted",
        "Deleted saved connection and local vault credentials",
        None,
        "success",
        None,
    )?;
    Ok(())
}

fn read_connection_export(path: &str) -> AppResult<crate::models::ConnectionExportFile> {
    let text = std::fs::read_to_string(path)
        .map_err(|error| AppError::validation(format!("无法读取导出文件：{error}")))?;
    let file: crate::models::ConnectionExportFile = serde_json::from_str(&text)
        .map_err(|error| AppError::validation(format!("导出文件格式无效：{error}")))?;
    if file.format != "luxbase-connections" {
        return Err(AppError::validation("不是 LuxBase 连接导出文件。"));
    }
    Ok(file)
}

fn export_passphrase(passphrase: &Option<String>, required: bool) -> AppResult<String> {
    let value = passphrase.as_deref().unwrap_or("").to_string();
    if required && value.is_empty() {
        return Err(AppError::validation("该操作涉及加密凭据，请输入口令。"));
    }
    Ok(value)
}

#[tauri::command]
pub async fn export_connections(
    state: State<'_, Arc<AppState>>,
    ids: Vec<String>,
    include_secrets: bool,
    passphrase: Option<String>,
    path: String,
) -> AppResult<usize> {
    let state = state_ref(&state);
    let passphrase = export_passphrase(&passphrase, include_secrets)?;
    let mut entries = Vec::new();
    for id in &ids {
        let config = state.connection(id)?;
        let secrets = if include_secrets {
            let plain = serde_json::to_vec(&serde_json::json!({
                "password": state.store.connection_password(id)?,
                "sshPassphrase": state.store.ssh_passphrase(id)?,
                "sshPassword": state.store.ssh_password(id)?,
            }))?;
            Some(crate::security::encrypt_with_passphrase(&plain, &passphrase)?)
        } else {
            None
        };
        entries.push(crate::models::ConnectionExportEntry { config, secrets });
    }
    let file = crate::models::ConnectionExportFile {
        format: "luxbase-connections".into(),
        version: 1,
        exported_at: chrono::Utc::now().to_rfc3339(),
        includes_secrets: include_secrets,
        connections: entries,
    };
    if let Some(parent) = Path::new(&path).parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&path, serde_json::to_string_pretty(&file)?)?;
    state.store.append_audit(
        None, None, None, "connections_export",
        &format!("Exported {} connections (secrets={include_secrets})", ids.len()),
        Some(ids.len() as i64), "success", None,
    )?;
    Ok(ids.len())
}

#[tauri::command]
pub async fn import_connections_preview(
    state: State<'_, Arc<AppState>>,
    path: String,
    passphrase: Option<String>,
) -> AppResult<crate::models::ConnectionImportPreview> {
    let state = state_ref(&state);
    let file = read_connection_export(&path)?;
    let passphrase = export_passphrase(&passphrase, file.includes_secrets)?;
    let existing: std::collections::HashSet<String> =
        state.store.list_connections()?.into_iter().map(|c| c.id).collect();
    let mut entries = Vec::new();
    for entry in &file.connections {
        if let Some(secrets) = &entry.secrets {
            crate::security::decrypt_with_passphrase(secrets, &passphrase)?;
        }
        entries.push(crate::models::ConnectionImportPreviewEntry {
            id: entry.config.id.clone(),
            name: entry.config.name.clone(),
            protocol: entry.config.protocol,
            host_summary: format!("{}:{}", entry.config.host, entry.config.port),
            has_secrets: entry.secrets.is_some(),
            exists: existing.contains(&entry.config.id),
        });
    }
    Ok(crate::models::ConnectionImportPreview { includes_secrets: file.includes_secrets, entries })
}

#[tauri::command]
pub async fn import_connections_apply(
    state: State<'_, Arc<AppState>>,
    path: String,
    passphrase: Option<String>,
    ids: Vec<String>,
    overwrite: bool,
) -> AppResult<crate::models::ConnectionImportResult> {
    let state = state_ref(&state);
    let file = read_connection_export(&path)?;
    let passphrase = export_passphrase(&passphrase, file.includes_secrets)?;
    let existing: std::collections::HashSet<String> =
        state.store.list_connections()?.into_iter().map(|c| c.id).collect();
    let mut imported = 0usize;
    let mut skipped = 0usize;
    let mut failed = Vec::new();
    for entry in file.connections {
        if !ids.contains(&entry.config.id) {
            continue;
        }
        if existing.contains(&entry.config.id) && !overwrite {
            skipped += 1;
            continue;
        }
        let (password, ssh_passphrase, ssh_password) = match &entry.secrets {
            Some(secrets) => {
                let plain = crate::security::decrypt_with_passphrase(secrets, &passphrase)?;
                let value: serde_json::Value = serde_json::from_slice(&plain)?;
                let get = |key: &str| value.get(key).and_then(|v| v.as_str()).map(String::from);
                (get("password"), get("sshPassphrase"), get("sshPassword"))
            }
            None => (None, None, None),
        };
        match state.store.save_connection(&entry.config, password.as_deref(), ssh_passphrase.as_deref(), ssh_password.as_deref()) {
            Ok(saved) => {
                state.forget_connection(&saved.id);
                imported += 1;
            }
            Err(error) => failed.push(format!("{}: {}", entry.config.name, error.message)),
        }
    }
    state.store.append_audit(
        None, None, None, "connections_import",
        &format!("Imported {imported} connections (skipped {skipped}, failed {})", failed.len()),
        Some(imported as i64), "success", None,
    )?;
    Ok(crate::models::ConnectionImportResult { imported, skipped, failed })
}

#[tauri::command]
pub async fn disconnect_connection(
    state: State<'_, Arc<AppState>>,
    connection_id: String,
) -> AppResult<()> {
    let state = state_ref(&state);
    let config = state.connection(&connection_id)?;
    match config.protocol.family() {
        DriverFamily::Sql => state.sql.disconnect(&connection_id).await?,
        DriverFamily::Kv => state.kv.disconnect(&connection_id).await?,
        DriverFamily::Search => state.search.disconnect(&connection_id).await?,
    }
    state.store.append_audit(
        Some(&connection_id),
        Some(&config.name),
        Some(config.protocol.as_str()),
        "connection_disconnect",
        "Disconnected and released the driver connection resources",
        None,
        "success",
        None,
    )?;
    Ok(())
}

#[tauri::command]
pub async fn shutdown_connections(state: State<'_, Arc<AppState>>) -> AppResult<()> {
    let state = state_ref(&state);
    state.shutdown().await;
    Ok(())
}

#[tauri::command]
pub async fn minimize_window(app: AppHandle) -> AppResult<()> {
    if let Some(window) = app.get_webview_window("main") {
        window.minimize()?;
    }
    Ok(())
}

#[tauri::command]
pub async fn toggle_maximize_window(app: AppHandle) -> AppResult<()> {
    if let Some(window) = app.get_webview_window("main") {
        let maximized = window.is_maximized()?;
        if maximized {
            window.unmaximize()?;
        } else {
            window.maximize()?;
        }
    }
    Ok(())
}

#[tauri::command]
pub async fn close_window(app: AppHandle, state: State<'_, Arc<AppState>>) -> AppResult<()> {
    let state = state_ref(&state);
    state.shutdown().await;
    if let Some(window) = app.get_webview_window("main") {
        window.close()?;
    }
    Ok(())
}

/// Mirrors the renderer's appearance choice onto the native window so the title bar, traffic
/// lights and the macOS menu bar change together with the webview. `system` clears the override,
/// which is how AppKit keeps following the OS setting.
#[tauri::command]
pub async fn set_window_appearance(app: AppHandle, mode: String) -> AppResult<()> {
    let theme = match mode.as_str() {
        "light" => Some(tauri::Theme::Light),
        "dark" => Some(tauri::Theme::Dark),
        _ => None,
    };
    if let Some(window) = app.get_webview_window("main") {
        window.set_theme(theme)?;
    }
    Ok(())
}

#[tauri::command]
pub fn screenshot_target() -> Option<String> {
    crate::screenshot_path().map(|path| path.to_string_lossy().to_string())
}

#[tauri::command]
pub fn save_screenshot(path: String, png_base64: String) -> AppResult<()> {
    let path = PathBuf::from(path);
    let extension_ok = path
        .extension()
        .and_then(|extension| extension.to_str())
        .map(|extension| extension.eq_ignore_ascii_case("png"))
        .unwrap_or(false);
    if !extension_ok {
        return Err(AppError::validation("Screenshot path must end in .png."));
    }
    let bytes = B64
        .decode(png_base64.as_bytes())
        .map_err(|error| AppError::validation(format!("Invalid PNG data: {error}")))?;
    if bytes.len() < 8 || &bytes[..8] != b"\x89PNG\r\n\x1a\n" {
        return Err(AppError::validation("Screenshot payload is not a valid PNG."));
    }
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, bytes)?;
    Ok(())
}

#[tauri::command]
pub fn exit_after_screenshot(app: AppHandle) {
    app.exit(0);
}

fn quote_identifier(protocol: &Protocol, identifier: &str) -> String {
    match protocol {
        Protocol::Mysql => format!("`{}`", identifier.replace('`', "``")),
        Protocol::Postgres | Protocol::Sqlite => {
            format!("\"{}\"", identifier.replace('"', "\"\""))
        }
        Protocol::Redis | Protocol::Elasticsearch => identifier.to_string(),
    }
}

pub(crate) fn qualified_table(protocol: &Protocol, schema: &str, table: &str) -> AppResult<String> {
    match protocol {
        Protocol::Mysql => Ok(format!(
            "`{}`.`{}`",
            schema.replace('`', "``"),
            table.replace('`', "``")
        )),
        Protocol::Postgres => Ok(format!(
            "\"{}\".\"{}\"",
            schema.replace('"', "\"\""),
            table.replace('"', "\"\"")
        )),
        Protocol::Sqlite => Ok(format!("\"{}\"", table.replace('"', "\"\""))),
        Protocol::Redis | Protocol::Elasticsearch => Err(AppError::unsupported(
            "SQL table operations are not available for Redis or Elasticsearch.",
        )),
    }
}

pub(crate) fn json_to_param(value: serde_json::Value) -> AppResult<crate::models::ParamValue> {
    use crate::models::ParamValue;
    Ok(match value {
        serde_json::Value::Null => ParamValue::Null,
        serde_json::Value::Bool(value) => ParamValue::Bool(value),
        serde_json::Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                ParamValue::I64(value)
            } else if let Some(value) = value.as_u64() {
                ParamValue::U64(value)
            } else if let Some(value) = value.as_f64() {
                ParamValue::F64(value)
            } else {
                return Err(AppError::validation("Numeric cell value is out of range."));
            }
        }
        serde_json::Value::String(value) => ParamValue::Text(value),
        serde_json::Value::Object(value) if value.len() == 1 && value.contains_key("bytesBase64") => {
            let encoded = value.get("bytesBase64").and_then(serde_json::Value::as_str)
                .ok_or_else(|| AppError::validation("Binary cells require a base64 string."))?;
            ParamValue::Bytes(B64.decode(encoded).map_err(|_| AppError::validation("Invalid binary cell encoding."))?)
        }
        value => ParamValue::Json(value),
    })
}

pub(crate) fn sql_statement_count(sql: &str) -> usize {
    let bytes = sql.as_bytes();
    let mut index = 0;
    let mut quote = None;
    let mut dollar_tag: Option<Vec<u8>> = None;
    let mut line_comment = false;
    let mut block_comment = false;
    let mut has_content = false;
    let mut count = 0;
    while index < bytes.len() {
        let current = bytes[index];
        let next = bytes.get(index + 1).copied();
        if let Some(tag) = dollar_tag.as_ref() {
            if bytes[index..].starts_with(tag) {
                index += tag.len();
                dollar_tag = None;
            } else {
                index += 1;
            }
            continue;
        }
        if line_comment {
            if current == b'\n' {
                line_comment = false;
            }
            index += 1;
            continue;
        }
        if block_comment {
            if current == b'*' && next == Some(b'/') {
                block_comment = false;
                index += 2;
            } else {
                index += 1;
            }
            continue;
        }
        if let Some(delimiter) = quote {
            if current == b'\\' {
                index += 2;
                continue;
            }
            if current == delimiter {
                if next == Some(delimiter) {
                    index += 2;
                } else {
                    quote = None;
                    index += 1;
                }
            } else {
                index += 1;
            }
            continue;
        }
        if (current == b'-' && next == Some(b'-')) || current == b'#' {
            line_comment = true;
            index += if current == b'#' { 1 } else { 2 };
            continue;
        }
        if current == b'/' && next == Some(b'*') {
            block_comment = true;
            index += 2;
            continue;
        }
        if current == b'$' {
            if let Some(length) = dollar_quote_tag_length(&bytes[index..]) {
                dollar_tag = Some(bytes[index..index + length].to_vec());
                has_content = true;
                index += length;
                continue;
            }
        }
        if matches!(current, b'\'' | b'"' | b'`') {
            quote = Some(current);
            has_content = true;
            index += 1;
            continue;
        }
        if current == b';' {
            if has_content {
                count += 1;
                has_content = false;
            }
            index += 1;
            continue;
        }
        if !current.is_ascii_whitespace() {
            has_content = true;
        }
        index += 1;
    }
    if has_content {
        count += 1;
    }
    count
}

fn audit_kind_for_sql(sql: &str) -> &'static str {
    match sql_leading_keyword(sql).as_str() {
        "CREATE" | "ALTER" | "DROP" | "TRUNCATE" | "RENAME" | "COMMENT" | "GRANT"
        | "REVOKE" => "ddl",
        _ => "query_execute",
    }
}

pub(crate) fn reject_session_state_statement(sql: &str) -> AppResult<()> {
    let keyword = sql_leading_keyword(sql);
    if matches!(
        keyword.as_str(),
        "BEGIN"
            | "START"
            | "COMMIT"
            | "ROLLBACK"
            | "SAVEPOINT"
            | "RELEASE"
            | "END"
            | "ABORT"
            | "SET"
            | "RESET"
            | "USE"
            | "LOCK"
            | "PREPARE"
            | "EXECUTE"
            | "DEALLOCATE"
            | "LISTEN"
            | "UNLISTEN"
            | "NOTIFY"
            | "ATTACH"
            | "DETACH"
    ) {
        return Err(AppError::validation(
            "This statement changes session or transaction state and cannot run through a pooled single-statement entrypoint. Use a transaction batch or a dedicated connection control.",
        ));
    }
    Ok(())
}

fn sql_leading_keyword(sql: &str) -> String {
    let bytes = sql.as_bytes();
    let mut index = 0;
    loop {
        while index < bytes.len() && bytes[index].is_ascii_whitespace() {
            index += 1;
        }
        if bytes.get(index..index + 2) == Some(b"--".as_slice()) {
            index += 2;
            while index < bytes.len() && bytes[index] != b'\n' {
                index += 1;
            }
            continue;
        }
        if bytes.get(index).copied() == Some(b'#') {
            index += 1;
            while index < bytes.len() && bytes[index] != b'\n' {
                index += 1;
            }
            continue;
        }
        if bytes.get(index..index + 2) == Some(b"/*".as_slice()) {
            index += 2;
            while index + 1 < bytes.len()
                && !(bytes[index] == b'*' && bytes[index + 1] == b'/')
            {
                index += 1;
            }
            if index + 1 < bytes.len() {
                index += 2;
            }
            continue;
        }
        break;
    }
    let start = index;
    while index < bytes.len() && bytes[index].is_ascii_alphabetic() {
        index += 1;
    }
    String::from_utf8_lossy(&bytes[start..index]).to_ascii_uppercase()
}

fn dollar_quote_tag_length(input: &[u8]) -> Option<usize> {
    if input.first().copied() != Some(b'$') {
        return None;
    }
    let mut index = 1;
    if input.get(index).copied() == Some(b'$') {
        return Some(2);
    }
    if !input
        .get(index)
        .copied()
        .map(|value| value.is_ascii_alphabetic() || value == b'_')
        .unwrap_or(false)
    {
        return None;
    }
    index += 1;
    while input
        .get(index)
        .copied()
        .map(|value| value.is_ascii_alphanumeric() || value == b'_')
        .unwrap_or(false)
    {
        index += 1;
    }
    if input.get(index).copied() == Some(b'$') {
        Some(index + 1)
    } else {
        None
    }
}

fn summarize_redis_command(command: &str, args: &[String]) -> String {
    let upper = command.trim().to_ascii_uppercase();
    let redact_config_value = upper == "CONFIG"
        && args
            .first()
            .map(|value| value.eq_ignore_ascii_case("SET"))
            .unwrap_or(false);
    let mut rendered = Vec::new();
    for (index, argument) in args.iter().take(8).enumerate() {
        if redact_config_value && index >= 2 {
            rendered.push("<redacted>".to_string());
        } else if upper == "ACL"
            && argument
                .chars()
                .next()
                .is_some_and(|prefix| matches!(prefix, '>' | '<' | '#' | '!'))
        {
            rendered.push(format!("{}<redacted>", &argument[..1]));
        } else {
            rendered.push(argument.clone());
        }
    }
    format!("{} {}", command.trim().to_ascii_uppercase(), rendered.join(" "))
        .trim()
        .to_string()
}

fn redact_sql_password_literals(sql: &str) -> String {
    let chars = sql.chars().collect::<Vec<_>>();
    let mut output = String::with_capacity(sql.len());
    let mut words = Vec::<String>::new();
    let mut index = 0;
    while index < chars.len() {
        let current = chars[index];
        let dollar_end = (current == '$')
            .then(|| dollar_quote_end(&chars, index))
            .flatten();
        if current == '\'' || dollar_end.is_some() {
            let end = match dollar_end {
                Some(end) => end,
                None => {
                    let mut end = index + 1;
                    while end < chars.len() {
                        if chars[end] == '\\' {
                            end = (end + 2).min(chars.len());
                            continue;
                        }
                        if chars[end] == '\'' {
                            if chars.get(end + 1).copied() == Some('\'') {
                                end += 2;
                                continue;
                            }
                            end += 1;
                            break;
                        }
                        end += 1;
                    }
                    end
                }
            };
            // PostgreSQL E'...' / N'...' prefixes are lexed as a word glued to the quote.
            let prefixed = current == '\''
                && index > 0
                && matches!(chars[index - 1], 'E' | 'e' | 'N' | 'n')
                && words.last().is_some_and(|word| word == "E" || word == "N");
            let effective_length = words.len() - usize::from(prefixed);
            let after_password = effective_length > 0
                && words.get(effective_length - 1).map(|word| word == "PASSWORD").unwrap_or(false);
            let after_identified_by = effective_length > 1
                && words
                    .get(effective_length - 1)
                    .map(|word| word == "BY")
                    .unwrap_or(false)
                && words
                    .iter()
                    .take(effective_length.saturating_sub(1))
                    .rev()
                    .take(4)
                    .any(|word| word == "IDENTIFIED");
            if after_password || after_identified_by {
                output.push_str("'<redacted>'");
            } else {
                output.extend(chars[index..end].iter());
            }
            words.clear();
            index = end;
            continue;
        }
        if current.is_alphanumeric() || current == '_' || current == '$' {
            let start = index;
            while index < chars.len()
                && (chars[index].is_alphanumeric() || chars[index] == '_' || chars[index] == '$')
            {
                index += 1;
            }
            let word = chars[start..index].iter().collect::<String>();
            output.push_str(&word);
            words.push(word.to_ascii_uppercase());
            if words.len() > 8 {
                words.remove(0);
            }
            continue;
        }
        if current == ';' {
            words.clear();
        }
        output.push(current);
        index += 1;
    }
    output
}

/// End index (exclusive) of a PostgreSQL dollar-quoted literal starting at `start`, if any.
/// `$1` placeholders are not literals because no closing `$` follows the tag.
fn dollar_quote_end(chars: &[char], start: usize) -> Option<usize> {
    let mut tag_end = start + 1;
    while tag_end < chars.len() && (chars[tag_end].is_alphanumeric() || chars[tag_end] == '_') {
        tag_end += 1;
    }
    if chars.get(tag_end) != Some(&'$') || chars.get(start + 1).is_some_and(|c| c.is_ascii_digit()) {
        return None;
    }
    let tag = &chars[start..=tag_end];
    let mut search = tag_end + 1;
    while search + tag.len() <= chars.len() {
        if &chars[search..search + tag.len()] == tag {
            return Some(search + tag.len());
        }
        search += 1;
    }
    Some(chars.len())
}

fn summarize_sql(sql: &str) -> String {
    let redacted = redact_sql_password_literals(sql);
    let compact = redacted.split_whitespace().collect::<Vec<_>>().join(" ");
    if compact.chars().count() <= 160 {
        compact
    } else {
        format!("{}…", compact.chars().take(160).collect::<String>())
    }
}

#[cfg(test)]
mod tests {
    use super::summarize_sql;

    #[test]
    fn redacts_mysql_identity_passwords() {
        let summary = summarize_sql(
            "CREATE USER 'reporter'@'%' IDENTIFIED BY 's3cret\\'quote'",
        );
        assert!(summary.contains("<redacted>"));
        assert!(!summary.contains("s3cret"));
        assert!(summary.contains("'reporter'@'%'"));
    }

    #[test]
    fn redacts_postgres_identity_passwords() {
        let summary = summarize_sql("ALTER ROLE \"reporter\" WITH PASSWORD E'pa''ss'");
        assert!(summary.contains("<redacted>"));
        assert!(!summary.contains("pa''ss"));
        assert!(summary.contains("\"reporter\""));
    }

    #[test]
    fn redacts_dollar_quoted_passwords_but_not_placeholders() {
        let summary = summarize_sql("CREATE ROLE app LOGIN PASSWORD $pw$s3cr3t$pw$; SELECT $1, $2");
        assert!(summary.contains("<redacted>"));
        assert!(!summary.contains("s3cr3t"));
        assert!(summary.contains("SELECT $1, $2"));
    }

    #[test]
    fn leaves_non_password_literals_visible() {
        let summary = summarize_sql("GRANT SELECT ON app.orders TO 'reporter'@'%'");
        assert!(!summary.contains("<redacted>"));
        assert!(summary.contains("'reporter'@'%'"));
    }
}

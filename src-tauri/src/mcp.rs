//! Local MCP (Model Context Protocol) server over stdio, so AI agents can use the connections
//! saved in LuxBase without ever seeing their passwords. Read-only by default; write statements
//! require the explicit `--allow-writes` flag and every statement is appended to the audit log.
//!
//! The agent launches `luxbase-mcp` and speaks newline-delimited JSON-RPC 2.0 on stdin/stdout.
//! Diagnostics go to stderr only, because stdout is the protocol channel.

use crate::app_state::AppState;
use crate::commands::{is_read_statement, reject_session_state_statement, sql_statement_count};
use crate::error::{AppError, AppResult};
use crate::models::{
    ConnectionConfig, EsSearchRequest, MetadataRequest, Protocol, RedisScanRequest, SqlRequest,
    VectorSearchRequest,
};
use crate::security::SecretStore;
use crate::storage::LocalStore;
use serde_json::{json, Value};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

const MAX_OUTPUT_BYTES: usize = 400_000;

#[derive(Debug, Default, Clone)]
pub struct McpOptions {
    pub data_dir: Option<PathBuf>,
    pub allow_writes: bool,
}

fn resolve_data_dir(options: &McpOptions) -> PathBuf {
    if let Some(dir) = &options.data_dir {
        return dir.clone();
    }
    if let Ok(dir) = std::env::var("LUXBASE_DATA_DIR") {
        if !dir.trim().is_empty() {
            return PathBuf::from(dir);
        }
    }
    dirs::data_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("com.luxbase.app")
}

fn operation_id() -> String {
    format!("mcp-{}", uuid::Uuid::new_v4())
}

fn clamp(value: Option<u64>, default: u64, max: u64) -> u64 {
    value.unwrap_or(default).clamp(1, max)
}

fn ensure_read_only_sql(sql: &str) -> AppResult<()> {
    if sql_statement_count(sql) != 1 {
        return Err(AppError::validation("Provide exactly one SQL statement per query call."));
    }
    reject_session_state_statement(sql)?;
    if !is_read_statement(sql) {
        return Err(AppError::validation(
            "This server is read-only: only SELECT / SHOW / DESCRIBE / EXPLAIN are allowed. Writes need the luxbase-mcp --allow-writes flag and the execute tool.",
        ));
    }
    Ok(())
}

fn arg_str<'a>(args: &'a Value, key: &str) -> AppResult<&'a str> {
    args.get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| AppError::validation(format!("Missing required string argument '{key}'.")))
}

fn arg_opt_str<'a>(args: &'a Value, key: &str) -> Option<&'a str> {
    args.get(key).and_then(Value::as_str).filter(|value| !value.is_empty())
}

fn arg_u64(args: &Value, key: &str) -> Option<u64> {
    args.get(key).and_then(Value::as_u64)
}

fn audit(
    state: &Arc<AppState>,
    config: Option<&ConnectionConfig>,
    kind: &str,
    detail: &str,
    rows: Option<i64>,
    outcome: &str,
    error: Option<&str>,
) {
    let _ = state.store.append_audit(
        config.map(|config| config.id.as_str()),
        config.map(|config| config.name.as_str()),
        config.map(|config| config.protocol.as_str()),
        kind,
        detail,
        rows,
        outcome,
        error,
    );
}

fn sql_connection(state: &Arc<AppState>, args: &Value) -> AppResult<(ConnectionConfig, Option<String>)> {
    let config = state.connection(arg_str(args, "connectionId")?)?;
    if !matches!(config.protocol, Protocol::Mysql | Protocol::Postgres | Protocol::Sqlite) {
        return Err(AppError::validation(format!(
            "Connection '{}' is {}; use the redis_* or es_* tools for it.",
            config.name,
            config.protocol.as_str()
        )));
    }
    let password = state.connection_password(&config)?;
    Ok((config, password))
}

async fn call_tool(
    state: &Arc<AppState>,
    allow_writes: bool,
    name: &str,
    args: &Value,
) -> AppResult<Value> {
    match name {
        "list_connections" => {
            let connections = state.store.list_connections()?;
            Ok(json!({ "connections": connections }))
        }
        "list_tables" => {
            let (config, password) = sql_connection(state, args)?;
            let schema = arg_opt_str(args, "schema").or(if config.database.is_empty() { None } else { Some(config.database.as_str()) });
            let metadata = state.sql.metadata(&config, password, MetadataRequest {
                connection_id: config.id.clone(),
                schema: schema.map(str::to_string),
                table: None,
                include_tables: true,
                include_columns: false,
            }).await?;
            let tables = metadata.databases.iter().flat_map(|db| {
                db.tables.iter().map(|table| json!({
                    "schema": table.schema, "name": table.name, "type": table.table_type,
                }))
            }).collect::<Vec<_>>();
            Ok(json!({ "connection": config.name, "tables": tables }))
        }
        "describe_table" => {
            let (config, password) = sql_connection(state, args)?;
            let table = arg_str(args, "table")?;
            let schema = arg_opt_str(args, "schema").or(if config.database.is_empty() { None } else { Some(config.database.as_str()) });
            let metadata = state.sql.metadata(&config, password, MetadataRequest {
                connection_id: config.id.clone(),
                schema: schema.map(str::to_string),
                table: Some(table.to_string()),
                include_tables: true,
                include_columns: true,
            }).await?;
            let table_meta = metadata.databases.iter()
                .flat_map(|db| db.tables.iter())
                .find(|item| item.name == table)
                .ok_or_else(|| AppError::validation(format!("Table '{table}' was not found.")))?;
            Ok(json!({
                "table": table_meta.name,
                "schema": table_meta.schema,
                "columns": table_meta.columns,
                "indexes": table_meta.indexes,
                "primaryKeys": table_meta.columns.iter().filter(|column| column.is_primary_key).map(|column| column.name.clone()).collect::<Vec<_>>(),
            }))
        }
        "query" => {
            let (config, password) = sql_connection(state, args)?;
            let sql = arg_str(args, "sql")?;
            ensure_read_only_sql(sql)?;
            let limit = clamp(arg_u64(args, "limit"), 100, 1000);
            let op = operation_id();
            let result = state.sql.fetch(&config, password, SqlRequest {
                connection_id: config.id.clone(),
                sql: sql.to_string(),
                params: Vec::new(),
                operation_id: op,
                page_size: Some(limit as u32),
                offset: Some(0),
                schema: arg_opt_str(args, "schema").map(str::to_string),
            }).await;
            match result {
                Ok(result) => {
                    audit(state, Some(&config), "agent_query", &format!("MCP read-only query: {sql}"), Some(result.rows.len() as i64), "success", None);
                    Ok(json!({
                        "columns": result.columns,
                        "rows": result.rows,
                        "rowCount": result.rows.len(),
                        "truncated": result.page.has_more,
                        "rowLimit": limit,
                        "durationMs": result.duration_ms,
                    }))
                }
                Err(error) => {
                    audit(state, Some(&config), "agent_query", &format!("MCP read-only query: {sql}"), None, "failure", Some(&error.message));
                    Err(error)
                }
            }
        }
        "execute" => {
            if !allow_writes {
                return Err(AppError::validation("Writes are disabled. Restart luxbase-mcp with --allow-writes to enable the execute tool."));
            }
            let (config, password) = sql_connection(state, args)?;
            let sql = arg_str(args, "sql")?;
            if sql_statement_count(sql) != 1 {
                return Err(AppError::validation("Provide exactly one SQL statement per execute call."));
            }
            reject_session_state_statement(sql)?;
            let result = state.sql.execute(&config, password, SqlRequest {
                connection_id: config.id.clone(),
                sql: sql.to_string(),
                params: Vec::new(),
                operation_id: operation_id(),
                page_size: None,
                offset: None,
                schema: arg_opt_str(args, "schema").map(str::to_string),
            }).await;
            match result {
                Ok(result) => {
                    audit(state, Some(&config), "agent_execute", &format!("MCP write statement: {sql}"), Some(result.rows_affected as i64), "success", None);
                    Ok(json!({ "rowsAffected": result.rows_affected, "lastInsertId": result.last_insert_id, "durationMs": result.duration_ms }))
                }
                Err(error) => {
                    audit(state, Some(&config), "agent_execute", &format!("MCP write statement: {sql}"), None, "failure", Some(&error.message));
                    Err(error)
                }
            }
        }
        "redis_scan" => {
            let config = state.connection(arg_str(args, "connectionId")?)?;
            if config.protocol != Protocol::Redis {
                return Err(AppError::validation("redis_scan requires a Redis connection."));
            }
            let password = state.connection_password(&config)?;
            let db = arg_u64(args, "db").unwrap_or(config.redis_db as u64) as u32;
            let count = clamp(arg_u64(args, "count"), 100, 500) as u32;
            let result = state.kv.scan(&config, password, RedisScanRequest {
                connection_id: config.id.clone(),
                cursor: arg_u64(args, "cursor").unwrap_or(0),
                pattern: arg_opt_str(args, "pattern").unwrap_or("*").to_string(),
                count,
                type_filter: None,
                db,
                operation_id: operation_id(),
            }).await?;
            audit(state, Some(&config), "agent_query", "MCP redis scan", Some(result.keys.len() as i64), "success", None);
            Ok(json!({ "cursor": result.cursor, "keys": result.keys, "db": db, "done": result.cursor == 0 }))
        }
        "redis_get" => {
            let config = state.connection(arg_str(args, "connectionId")?)?;
            if config.protocol != Protocol::Redis {
                return Err(AppError::validation("redis_get requires a Redis connection."));
            }
            let password = state.connection_password(&config)?;
            let key = arg_str(args, "key")?;
            let db = arg_u64(args, "db").unwrap_or(config.redis_db as u64) as u32;
            let value = state.kv.get(&config, password, key.as_bytes(), db, &operation_id()).await?;
            audit(state, Some(&config), "agent_query", &format!("MCP redis get {key}"), None, "success", None);
            Ok(json!({ "value": value }))
        }
        "es_search" => {
            let config = state.connection(arg_str(args, "connectionId")?)?;
            if config.protocol != Protocol::Elasticsearch {
                return Err(AppError::validation("es_search requires an Elasticsearch connection."));
            }
            let password = state.connection_password(&config)?;
            let size = clamp(arg_u64(args, "size"), 20, 100) as u32;
            let body = args.get("body").cloned().unwrap_or_else(|| json!({ "query": { "match_all": {} } }));
            let result = state.search.search(&config, password, EsSearchRequest {
                connection_id: config.id.clone(),
                index: arg_str(args, "index")?.to_string(),
                body,
                from: arg_u64(args, "from").unwrap_or(0),
                size,
                operation_id: operation_id(),
            }).await?;
            audit(state, Some(&config), "agent_query", "MCP elasticsearch search", Some(result.hits.len() as i64), "success", None);
            Ok(json!({
                "total": result.total,
                "totalRelation": result.total_relation,
                "hits": result.hits,
                "aggregations": result.aggregations,
                "tookMs": result.took_ms,
            }))
        }
        "vector_search" => {
            let config = state.connection(arg_str(args, "connectionId")?)?;
            if config.protocol.family() != crate::models::DriverFamily::Vector {
                return Err(AppError::validation("vector_search requires a Milvus or Qdrant connection."));
            }
            let password = state.connection_password(&config)?;
            let vector = args.get("vector").and_then(Value::as_array)
                .map(|items| items.iter().filter_map(Value::as_f64).collect::<Vec<f64>>())
                .unwrap_or_default();
            if vector.is_empty() {
                return Err(AppError::validation("vector_search requires a non-empty numeric `vector` array."));
            }
            let top_k = clamp(arg_u64(args, "topK"), 10, 100) as u32;
            let output_fields = args.get("outputFields").and_then(Value::as_array)
                .map(|items| items.iter().filter_map(|v| v.as_str().map(str::to_string)).collect::<Vec<String>>())
                .unwrap_or_default();
            let result = state.vector.search(&config, password, VectorSearchRequest {
                connection_id: config.id.clone(),
                collection: arg_str(args, "collection")?.to_string(),
                vector_field: args.get("vectorField").and_then(Value::as_str).map(str::to_string),
                vector,
                top_k,
                filter: args.get("filter").cloned().unwrap_or(Value::Null),
                output_fields,
                params: Value::Null,
                operation_id: operation_id(),
            }).await?;
            audit(state, Some(&config), "agent_query", "MCP vector search", Some(result.hits.len() as i64), "success", None);
            Ok(json!({ "hits": result.hits, "durationMs": result.duration_ms }))
        }
        _ => Err(AppError::validation(format!("Unknown tool '{name}'."))),
    }
}

fn tool_definitions(allow_writes: bool) -> Vec<Value> {
    let connection_arg = json!({ "connectionId": { "type": "string", "description": "Saved LuxBase connection id (see list_connections). Passwords stay inside the LuxBase vault." } });
    let schema_arg = json!({ "type": "string", "description": "Optional database/schema override; defaults to the connection's database." });
    let mut tools = vec![
        json!({
            "name": "list_connections",
            "description": "List the database connections saved in LuxBase (id, name, protocol, host). No credentials are returned.",
            "inputSchema": { "type": "object", "properties": {}, "additionalProperties": false }
        }),
        json!({
            "name": "list_tables",
            "description": "List tables and views of a MySQL/PostgreSQL/SQLite connection.",
            "inputSchema": { "type": "object", "properties": { "connectionId": connection_arg["connectionId"].clone(), "schema": schema_arg }, "required": ["connectionId"] }
        }),
        json!({
            "name": "describe_table",
            "description": "Describe a table: columns with types/nullability, primary keys and indexes.",
            "inputSchema": { "type": "object", "properties": { "connectionId": connection_arg["connectionId"].clone(), "schema": schema_arg, "table": { "type": "string" } }, "required": ["connectionId", "table"] }
        }),
        json!({
            "name": "query",
            "description": "Run ONE read-only SQL statement (SELECT / SHOW / DESCRIBE / EXPLAIN) on a MySQL/PostgreSQL/SQLite connection. Rows are capped by limit (default 100, max 1000).",
            "inputSchema": { "type": "object", "properties": {
                "connectionId": connection_arg["connectionId"].clone(),
                "schema": schema_arg,
                "sql": { "type": "string", "description": "Single read-only SQL statement." },
                "limit": { "type": "integer", "minimum": 1, "maximum": 1000, "default": 100 }
            }, "required": ["connectionId", "sql"] }
        }),
        json!({
            "name": "redis_scan",
            "description": "Scan Redis keys with MATCH/COUNT semantics. Iterate with the returned cursor until it is 0.",
            "inputSchema": { "type": "object", "properties": {
                "connectionId": connection_arg["connectionId"].clone(),
                "db": { "type": "integer", "minimum": 0 },
                "pattern": { "type": "string", "default": "*" },
                "cursor": { "type": "integer", "default": 0 },
                "count": { "type": "integer", "minimum": 1, "maximum": 500, "default": 100 }
            }, "required": ["connectionId"] }
        }),
        json!({
            "name": "redis_get",
            "description": "Read one Redis key's value, type and TTL (read-only).",
            "inputSchema": { "type": "object", "properties": {
                "connectionId": connection_arg["connectionId"].clone(),
                "key": { "type": "string" },
                "db": { "type": "integer", "minimum": 0 }
            }, "required": ["connectionId", "key"] }
        }),
        json!({
            "name": "es_search",
            "description": "Run an Elasticsearch query DSL body against one index (read-only, size max 100). Defaults to match_all.",
            "inputSchema": { "type": "object", "properties": {
                "connectionId": connection_arg["connectionId"].clone(),
                "index": { "type": "string" },
                "body": { "type": "object", "description": "Query DSL body (query/aggs/sort/_source)." },
                "from": { "type": "integer", "default": 0 },
                "size": { "type": "integer", "minimum": 1, "maximum": 100, "default": 20 }
            }, "required": ["connectionId", "index"] }
        }),
        json!({
            "name": "vector_search",
            "description": "Run a vector similarity search on a Milvus or Qdrant collection (read-only, topK max 100). Provide the query `vector`; `filter` is a Milvus boolean-expression string or a Qdrant filter object.",
            "inputSchema": { "type": "object", "properties": {
                "connectionId": connection_arg["connectionId"].clone(),
                "collection": { "type": "string" },
                "vector": { "type": "array", "items": { "type": "number" }, "description": "Query embedding." },
                "vectorField": { "type": "string", "description": "Named vector field; omit for the default/only vector." },
                "filter": { "description": "Milvus expr string or Qdrant filter object." },
                "outputFields": { "type": "array", "items": { "type": "string" } },
                "topK": { "type": "integer", "minimum": 1, "maximum": 100, "default": 10 }
            }, "required": ["connectionId", "collection", "vector"] }
        }),
    ];
    if allow_writes {
        tools.push(json!({
            "name": "execute",
            "description": "Run ONE write SQL statement (INSERT / UPDATE / DELETE / DDL). Only available because the server was started with --allow-writes. Every statement is audited.",
            "inputSchema": { "type": "object", "properties": {
                "connectionId": connection_arg["connectionId"].clone(),
                "schema": schema_arg,
                "sql": { "type": "string" }
            }, "required": ["connectionId", "sql"] }
        }));
    }
    tools
}

fn tool_text(value: &Value) -> String {
    let mut text = serde_json::to_string_pretty(value).unwrap_or_else(|_| "{}".into());
    if text.len() > MAX_OUTPUT_BYTES {
        text.truncate(MAX_OUTPUT_BYTES);
        text.push_str("\n… output truncated by luxbase-mcp; narrow the query or lower the limit.");
    }
    text
}

async fn handle_request(
    state: &Arc<AppState>,
    allow_writes: bool,
    method: &str,
    params: &Value,
) -> Result<Value, (i64, String)> {
    match method {
        "initialize" => Ok(json!({
            "protocolVersion": params.get("protocolVersion").and_then(Value::as_str).unwrap_or("2024-11-05"),
            "capabilities": { "tools": { "listChanged": false } },
            "serverInfo": { "name": "luxbase-mcp", "version": env!("CARGO_PKG_VERSION") },
            "instructions": format!(
                "LuxBase MCP bridge over the user's saved connections. Read-only tools: list_connections, list_tables, describe_table, query, redis_scan, redis_get, es_search, vector_search. {}Passwords never leave the LuxBase vault. Statements are appended to the LuxBase audit log.",
                if allow_writes { "Write tool enabled: execute (single statement). " } else { "Writes are disabled; start luxbase-mcp with --allow-writes to enable execute. " }
            ),
        })),
        "ping" => Ok(json!({})),
        "tools/list" => Ok(json!({ "tools": tool_definitions(allow_writes) })),
        "tools/call" => {
            let name = params.get("name").and_then(Value::as_str).unwrap_or("");
            let args = params.get("arguments").cloned().unwrap_or_else(|| json!({}));
            match call_tool(state, allow_writes, name, &args).await {
                Ok(value) => Ok(json!({ "content": [{ "type": "text", "text": tool_text(&value) }], "isError": false })),
                Err(error) => Ok(json!({ "content": [{ "type": "text", "text": format!("Error: {}", error.message) }], "isError": true })),
            }
        }
        _ => Err((-32601, format!("Method not found: {method}"))),
    }
}

pub async fn run(options: McpOptions) -> AppResult<()> {
    let data_dir = resolve_data_dir(&options);
    let store = LocalStore::open(data_dir.join("luxbase.sqlite3"), SecretStore::new(data_dir.join("vault.key")))?;
    let state = Arc::new(AppState::new(store));
    eprintln!(
        "luxbase-mcp: data dir {} · {} · protocol stdio (newline-delimited JSON-RPC 2.0)",
        data_dir.display(),
        if options.allow_writes { "writes ENABLED via --allow-writes" } else { "read-only" }
    );
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    let mut stdout = tokio::io::stdout();
    while let Some(line) = lines.next_line().await.map_err(|error| AppError::connection(format!("MCP stdin read failed: {error}")))? {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let message: Value = match serde_json::from_str(line) {
            Ok(message) => message,
            Err(error) => {
                let response = json!({ "jsonrpc": "2.0", "id": Value::Null, "error": { "code": -32700, "message": format!("Parse error: {error}") } });
                stdout.write_all(format!("{response}\n").as_bytes()).await.ok();
                stdout.flush().await.ok();
                continue;
            }
        };
        let method = message.get("method").and_then(Value::as_str).unwrap_or("");
        let id = message.get("id").cloned();
        let params = message.get("params").cloned().unwrap_or_else(|| json!({}));
        if method == "notifications/initialized" || method == "notifications/cancelled" {
            continue;
        }
        let response = match handle_request(&state, options.allow_writes, method, &params).await {
            Ok(result) => id.map(|id| json!({ "jsonrpc": "2.0", "id": id, "result": result })),
            Err((code, message)) => id.map(|id| json!({ "jsonrpc": "2.0", "id": id, "error": { "code": code, "message": message } })),
        };
        if let Some(response) = response {
            stdout
                .write_all(format!("{response}\n").as_bytes())
                .await
                .map_err(|error| AppError::connection(format!("MCP stdout write failed: {error}")))?;
            stdout.flush().await.ok();
        }
    }
    state.shutdown().await;
    Ok(())
}

#[cfg(test)]
mod mcp_tests {
    use super::*;

    #[test]
    fn read_only_rejects_writes_and_multi_statements() {
        assert!(ensure_read_only_sql("SELECT * FROM users WHERE id = 1").is_ok());
        assert!(ensure_read_only_sql("EXPLAIN SELECT 1").is_ok());
        assert!(ensure_read_only_sql("DROP TABLE users").is_err());
        assert!(ensure_read_only_sql("SELECT 1; SELECT 2").is_err());
        assert!(ensure_read_only_sql("SET FOREIGN_KEY_CHECKS=0").is_err());
    }

    #[test]
    fn write_tool_is_hidden_unless_enabled() {
        let read_only = tool_definitions(false);
        assert!(read_only.iter().any(|tool| tool["name"] == "query"));
        assert!(!read_only.iter().any(|tool| tool["name"] == "execute"));
        assert!(tool_definitions(true).iter().any(|tool| tool["name"] == "execute"));
    }

    #[test]
    fn limits_are_clamped() {
        assert_eq!(clamp(None, 100, 1000), 100);
        assert_eq!(clamp(Some(0), 100, 1000), 1);
        assert_eq!(clamp(Some(5000), 100, 1000), 1000);
    }

    #[test]
    fn output_is_truncated_for_agents() {
        let big = json!({ "text": "x".repeat(MAX_OUTPUT_BYTES + 100) });
        assert!(tool_text(&big).len() < MAX_OUTPUT_BYTES + 200);
    }

    #[tokio::test]
    async fn initialize_describes_the_server() {
        let dir = std::env::temp_dir().join(format!("luxbase-mcp-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let store = LocalStore::open(dir.join("luxbase.sqlite3"), SecretStore::new(dir.join("vault.key"))).unwrap();
        let state = Arc::new(AppState::new(store));
        let result = handle_request(&state, false, "initialize", &json!({ "protocolVersion": "2024-11-05" })).await.unwrap();
        assert_eq!(result["serverInfo"]["name"], "luxbase-mcp");
        assert!(result["instructions"].as_str().unwrap().contains("Writes are disabled"));
        let list = handle_request(&state, false, "tools/list", &json!({})).await.unwrap();
        assert!(list["tools"].as_array().unwrap().len() >= 7);
        let missing = handle_request(&state, false, "tools/call", &json!({ "name": "query", "arguments": { "connectionId": "missing", "sql": "SELECT 1" } })).await.unwrap();
        assert_eq!(missing["isError"], true);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn live_mysql_tools() {
        let Some(target) = crate::live_tests::target("LUXBASE_LIVE_MYSQL") else { return };
        // Kept on purpose: the stdio smoke test reuses this disposable store.
        let dir = PathBuf::from("/tmp/luxbase-mcp-qa-store");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let store = LocalStore::open(dir.join("luxbase.sqlite3"), SecretStore::new(dir.join("vault.key"))).unwrap();
        let state = Arc::new(AppState::new(store));
        let config = crate::live_tests::config("mcp-live-mysql", "mysql", &target, json!({ "tlsMode": "disabled" }));
        state.store.save_connection(&config, target.password.as_deref(), None, None).expect("save mcp live connection");
        let connection_id = config.id.clone();
        crate::live_tests::live_execute(&state, &config, &target.password, "DROP TABLE IF EXISTS luxbase_qa_mcp").await;
        crate::live_tests::live_execute(&state, &config, &target.password, "CREATE TABLE luxbase_qa_mcp (id INT PRIMARY KEY, name VARCHAR(64))").await;
        crate::live_tests::live_execute(&state, &config, &target.password, "INSERT INTO luxbase_qa_mcp VALUES (1, 'alpha'), (2, 'beta')").await;

        let connections = call_tool(&state, false, "list_connections", &json!({})).await.unwrap();
        assert_eq!(connections["connections"][0]["id"], connection_id);
        assert!(connections["connections"][0].get("password").is_none(), "credentials must never be listed");
        let tables = call_tool(&state, false, "list_tables", &json!({ "connectionId": connection_id })).await.unwrap();
        assert!(tables["tables"].as_array().unwrap().iter().any(|table| table["name"] == "luxbase_qa_mcp"));
        let described = call_tool(&state, false, "describe_table", &json!({ "connectionId": connection_id, "table": "luxbase_qa_mcp" })).await.unwrap();
        assert_eq!(described["primaryKeys"][0], "id");
        let queried = call_tool(&state, false, "query", &json!({ "connectionId": connection_id, "sql": "SELECT id, name FROM luxbase_qa_mcp ORDER BY id", "limit": 10 })).await.unwrap();
        assert_eq!(queried["rowCount"], 2);
        assert_eq!(queried["rows"][1][1], "beta");
        assert!(call_tool(&state, false, "query", &json!({ "connectionId": connection_id, "sql": "DELETE FROM luxbase_qa_mcp" })).await.is_err(), "read-only query rejects DELETE");
        assert!(call_tool(&state, false, "execute", &json!({ "connectionId": connection_id, "sql": "DELETE FROM luxbase_qa_mcp" })).await.is_err(), "execute requires --allow-writes");
        let inserted = call_tool(&state, true, "execute", &json!({ "connectionId": connection_id, "sql": "INSERT INTO luxbase_qa_mcp VALUES (3, 'gamma')" })).await.unwrap();
        assert_eq!(inserted["rowsAffected"], 1);
        let audited = state.store.recent_audit(20).unwrap();
        assert!(audited.iter().any(|entry| entry.event_kind == "agent_query"), "agent queries are audited");
        assert!(audited.iter().any(|entry| entry.event_kind == "agent_execute"), "agent writes are audited");
        crate::live_tests::live_execute(&state, &config, &target.password, "DELETE FROM luxbase_qa_mcp WHERE id = 3").await;
    }
}

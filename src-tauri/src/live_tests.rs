//! Live driver checks against real servers. Each suite runs only when its environment
//! variable is set, so a plain `cargo test` stays hermetic:
//!
//! LUXBASE_LIVE_MYSQL="host|port|user|password|database"          (any auth plugin, TLS preferred)
//! LUXBASE_LIVE_MYSQL_NATIVE="host|port|user|password|database"   (account using mysql_native_password)
//! LUXBASE_LIVE_POSTGRES="host|port|user|password|database"
//! LUXBASE_LIVE_REDIS="host|port|password"
//! LUXBASE_LIVE_ES="host|port|user"  (password read from LUXBASE_LIVE_ES_PASSWORD_FILE, never argv)
//! SQLite always runs against a temporary file.

use crate::drivers::kv::KvConnectionManager;
use crate::drivers::search::SearchConnectionManager;
use crate::drivers::sql::SqlConnectionManager;
use crate::models::{
    ConnectionConfig, ErrorClass, EsSearchRequest, MetadataRequest, ParamValue, Protocol,
    QueryResult, RedisCommandRequest, RedisScanRequest, RedisValue, RedisValueBody,
    SqlBatchRequest, SqlQueryExportRequest, SqlRequest, SqlScriptRequest,
};
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::{Duration, Instant};

pub(crate) struct Target {
    pub(crate) host: String,
    pub(crate) port: u16,
    pub(crate) user: String,
    pub(crate) password: Option<String>,
    pub(crate) database: String,
}

pub(crate) fn target(name: &str) -> Option<Target> {
    let raw = std::env::var(name).ok()?;
    let parts = raw.split('|').collect::<Vec<_>>();
    Some(Target {
        host: parts.first()?.to_string(),
        port: parts.get(1)?.parse().ok()?,
        user: parts.get(2).unwrap_or(&"").to_string(),
        password: parts.get(3).filter(|value| !value.is_empty()).map(|value| value.to_string()),
        database: parts.get(4).unwrap_or(&"").to_string(),
    })
}

pub(crate) fn config(id: &str, protocol: &str, target: &Target, extra: Value) -> ConnectionConfig {
    let mut base = json!({
        "id": id,
        "name": id,
        "protocol": protocol,
        "host": target.host,
        "port": target.port,
        "username": target.user,
        "database": target.database,
        "pool": {
            "minIdle": 1,
            "maxSize": 4,
            "connectionTimeoutMs": 5000,
            "idleTimeoutMs": 60000,
            "maxLifetimeMs": 600000,
            "healthCheckTimeoutMs": 3000
        },
        "queryTimeoutMs": 20000
    });
    if let (Some(base), Some(extra)) = (base.as_object_mut(), extra.as_object()) {
        for (key, value) in extra {
            base.insert(key.clone(), value.clone());
        }
    }
    serde_json::from_value(base).expect("valid live-test connection config")
}

pub(crate) fn request(config: &ConnectionConfig, operation: &str, sql: &str, params: Vec<ParamValue>) -> SqlRequest {
    SqlRequest {
        connection_id: config.id.clone(),
        sql: sql.to_string(),
        params,
        operation_id: format!("{operation}-{}", uuid::Uuid::new_v4()),
        page_size: None,
        offset: None,
        schema: None,
    }
}

/// A throw-away vault and local store, so the transfer and row-sync checks run through the same
/// `AppState` plumbing as the app while never seeing a real saved connection.
pub(crate) fn live_state(tag: &str) -> (Arc<crate::app_state::AppState>, std::path::PathBuf) {
    let dir = std::env::temp_dir().join(format!("luxbase-live-{tag}-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).expect("create live-test state dir");
    let store = crate::storage::LocalStore::open(
        dir.join("store.sqlite3"),
        crate::security::SecretStore::new(dir.join("vault.key")),
    )
    .expect("open live-test local store");
    (Arc::new(crate::app_state::AppState::new(store)), dir)
}

pub(crate) async fn live_execute(
    state: &crate::app_state::AppState,
    config: &ConnectionConfig,
    password: &Option<String>,
    sql: &str,
) {
    state
        .sql
        .execute(config, password.clone(), request(config, "seed", sql, Vec::new()))
        .await
        .unwrap_or_else(|error| panic!("{sql} failed: {error:?}"));
}

pub(crate) async fn live_count(
    state: &crate::app_state::AppState,
    config: &ConnectionConfig,
    password: &Option<String>,
    table: &str,
) -> i64 {
    let result = state
        .sql
        .fetch(
            config,
            password.clone(),
            request(config, "count", &format!("SELECT COUNT(*) AS n FROM {table}"), Vec::new()),
        )
        .await
        .unwrap_or_else(|error| panic!("counting {table} failed: {error:?}"));
    result.rows[0][0].as_i64().expect("COUNT(*) is an integer")
}

/// One-row `SELECT <expr>` used to compare server-side aggregates between two tables.
/// Decimals arrive as text on some engines, so the value is normalised to f64.
pub(crate) async fn live_number(
    state: &crate::app_state::AppState,
    config: &ConnectionConfig,
    password: &Option<String>,
    sql: &str,
) -> f64 {
    let result = state
        .sql
        .fetch(config, password.clone(), request(config, "scalar", sql, Vec::new()))
        .await
        .unwrap_or_else(|error| panic!("{sql} failed: {error:?}"));
    match result.rows[0][0].clone() {
        Value::Number(number) => number.as_f64().expect("numeric aggregate"),
        Value::String(text) => text.parse::<f64>().unwrap_or(f64::NAN),
        Value::Bool(flag) => f64::from(flag),
        Value::Null => f64::NAN,
        other => panic!("{other} is not a number"),
    }
}

struct Dialect {
    protocol: Protocol,
    schema: Option<String>,
    placeholder: fn(usize) -> String,
    create: &'static str,
    bytes_expr: &'static str,
    long_query: &'static str,
    timeout_query: Option<&'static str>,
    active_timeout_queries: Option<&'static str>,
}

fn question(_: usize) -> String {
    "?".into()
}

fn dollar(index: usize) -> String {
    format!("${index}")
}

fn text(value: &str) -> ParamValue {
    ParamValue::Text(value.into())
}

async fn fetch(manager: &SqlConnectionManager, config: &ConnectionConfig, password: &Option<String>, sql: &str) -> QueryResult {
    manager
        .fetch(config, password.clone(), request(config, "fetch", sql, Vec::new()))
        .await
        .unwrap_or_else(|error| panic!("{sql} failed: {error:?}"))
}

async fn count_rows(manager: &SqlConnectionManager, config: &ConnectionConfig, password: &Option<String>) -> i64 {
    let result = fetch(manager, config, password, "SELECT COUNT(*) AS n FROM luxbase_live").await;
    result.rows[0][0].as_i64().expect("COUNT(*) is an integer")
}

async fn run_sql_suite(config: ConnectionConfig, password: Option<String>, dialect: Dialect) {
    let manager = Arc::new(SqlConnectionManager::default());
    let p = dialect.placeholder;

    let tested = manager.test(&config, password.clone()).await;
    assert!(tested.ok, "connection test failed: {tested:?}");
    assert!(tested.server_version.as_deref().is_some_and(|value| !value.is_empty()), "{tested:?}");
    if dialect.protocol != Protocol::Sqlite {
        let wrong = manager.test(&config, Some("definitely-wrong-password".into())).await;
        assert!(!wrong.ok);
        assert_eq!(wrong.error_class, Some(ErrorClass::PermissionDenied), "wrong password: {wrong:?}");
    }

    let execute = |sql: String, params: Vec<ParamValue>| {
        let manager = Arc::clone(&manager);
        let config = config.clone();
        let password = password.clone();
        async move {
            manager
                .execute(&config, password, request(&config, "execute", &sql, params))
                .await
        }
    };
    execute("DROP TABLE IF EXISTS luxbase_live".into(), Vec::new()).await.expect("drop");
    execute(dialect.create.into(), Vec::new()).await.expect("create");

    let insert = format!(
        "INSERT INTO luxbase_live (id, name, amount, created_at, payload, data, flag, ratio) \
         VALUES ({}, {}, {}, {}, {}, {}, {}, {})",
        p(1), p(2), p(3), p(4), p(5), dialect.bytes_expr, p(6), p(7)
    );
    for (id, name) in [(1, "中文😀 O'Brien"), (2, "second"), (3, "third")] {
        let done = execute(
            insert.clone(),
            vec![
                ParamValue::I64(id),
                text(name),
                text("12.50"),
                text("2026-01-02 03:04:05"),
                text(r#"{"k":[1,2]}"#),
                ParamValue::Bool(id == 1),
                ParamValue::F64(0.25),
            ],
        )
        .await
        .unwrap_or_else(|error| panic!("insert {id}: {error:?}"));
        assert_eq!(done.rows_affected, 1);
    }
    execute(
        format!("INSERT INTO luxbase_live (id, name) VALUES ({}, {})", p(1), p(2)),
        vec![ParamValue::I64(4), text("nulls")],
    )
    .await
    .expect("insert row with NULLs");

    let select = "SELECT id, name, amount, created_at, payload, data, flag, ratio FROM luxbase_live ORDER BY id";
    let mut first_page = request(&config, "page", select, Vec::new());
    first_page.page_size = Some(2);
    let page = manager.fetch(&config, password.clone(), first_page).await.expect("page 1");
    assert_eq!(page.rows.len(), 2);
    assert!(page.page.has_more);
    assert_eq!(page.columns.len(), 8);
    let row = &page.rows[0];
    assert_eq!(row[0], json!(1));
    assert_eq!(row[1], json!("中文😀 O'Brien"));
    match dialect.protocol {
        Protocol::Sqlite => assert_eq!(row[2].as_f64(), Some(12.5)),
        _ => assert_eq!(row[2], json!("12.50")),
    }
    assert_eq!(row[3], json!("2026-01-02 03:04:05"));
    let payload: Value = serde_json::from_str(row[4].as_str().expect("JSON shown as text")).unwrap();
    assert_eq!(payload, json!({"k": [1, 2]}));
    assert_eq!(row[5], json!({"bytesBase64": "3q2+7w=="}));
    assert!(row[6] == json!(true) || row[6] == json!(1), "flag: {}", row[6]);
    assert_eq!(row[7].as_f64(), Some(0.25));

    let mut second_page = request(&config, "page", select, Vec::new());
    second_page.page_size = Some(2);
    second_page.offset = Some(2);
    let page = manager.fetch(&config, password.clone(), second_page).await.expect("page 2");
    assert_eq!(page.rows.len(), 2);
    assert!(!page.page.has_more);
    assert_eq!(page.rows[1][2], Value::Null);
    assert_eq!(page.rows[1][5], Value::Null);

    let filtered = format!("SELECT id FROM luxbase_live WHERE created_at >= {} AND amount > {} ORDER BY id", p(1), p(2));
    let result = manager
        .fetch(&config, password.clone(), request(&config, "filter", &filtered, vec![text("2026-01-01"), ParamValue::U64(10)]))
        .await
        .expect("parameterised filter on timestamp and numeric columns");
    assert_eq!(result.rows.len(), 3);

    let batch = |statements: Vec<(String, Vec<ParamValue>)>, operation: &str| SqlBatchRequest {
        connection_id: config.id.clone(),
        statements: statements
            .into_iter()
            .map(|(sql, params)| request(&config, operation, &sql, params))
            .collect(),
        operation_id: format!("{operation}-{}", uuid::Uuid::new_v4()),
        wrap_in_transaction: true,
    };
    let failing = batch(
        vec![
            (format!("INSERT INTO luxbase_live (id, name) VALUES ({}, {})", p(1), p(2)), vec![ParamValue::I64(10), text("ok")]),
            (format!("INSERT INTO luxbase_live (id, name) VALUES ({}, {})", p(1), p(2)), vec![ParamValue::I64(1), text("duplicate")]),
        ],
        "batch-fail",
    );
    let error = manager.execute_batch(&config, password.clone(), failing).await.expect_err("duplicate key must fail");
    assert_eq!(error.class, ErrorClass::Conflict, "{error:?}");
    assert_eq!(count_rows(&manager, &config, &password).await, 4, "failed batch must roll back");

    let passing = batch(
        vec![
            (format!("INSERT INTO luxbase_live (id, name) VALUES ({}, {})", p(1), p(2)), vec![ParamValue::I64(11), text("batch")]),
            (format!("UPDATE luxbase_live SET name = {} WHERE id = {}", p(1), p(2)), vec![text("batch-updated"), ParamValue::I64(11)]),
        ],
        "batch-ok",
    );
    let results = manager.execute_batch(&config, password.clone(), passing).await.expect("batch commit");
    assert!(results.iter().all(|result| result.committed));
    assert_eq!(count_rows(&manager, &config, &password).await, 5);

    let syntax = manager
        .fetch(&config, password.clone(), request(&config, "syntax", "SELEC broken FROM", Vec::new()))
        .await
        .expect_err("syntax error");
    assert_eq!(syntax.class, ErrorClass::SyntaxError, "{syntax:?}");

    let metadata = manager
        .metadata(
            &config,
            password.clone(),
            MetadataRequest {
                connection_id: config.id.clone(),
                schema: dialect.schema.clone(),
                table: Some("luxbase_live".into()),
                include_tables: true,
                include_columns: false,
            },
        )
        .await
        .expect("metadata");
    let table = metadata
        .databases
        .iter()
        .flat_map(|database| database.tables.iter())
        .find(|table| table.name == "luxbase_live")
        .expect("luxbase_live listed in metadata");
    let id_column = table.columns.iter().find(|column| column.name == "id").expect("id column");
    assert!(id_column.is_primary_key);

    let cancel_request = request(&config, "cancel", dialect.long_query, Vec::new());
    let cancel_id = cancel_request.operation_id.clone();
    let started = Instant::now();
    let running = {
        let manager = Arc::clone(&manager);
        let config = config.clone();
        let password = password.clone();
        tokio::spawn(async move { manager.fetch(&config, password, cancel_request).await })
    };
    tokio::time::sleep(Duration::from_millis(500)).await;
    manager.cancel(&config.id, &cancel_id).await.expect("cancel accepted");
    let cancelled = running.await.unwrap().expect_err("cancelled query must fail");
    assert_eq!(cancelled.class, ErrorClass::Cancelled, "{cancelled:?}");
    assert!(started.elapsed() < Duration::from_secs(4), "cancel took {:?}", started.elapsed());
    assert_eq!(fetch(&manager, &config, &password, "SELECT 1").await.rows[0][0], json!(1));

    let tx_request = batch(
        vec![
            (format!("INSERT INTO luxbase_live (id, name) VALUES ({}, {})", p(1), p(2)), vec![ParamValue::I64(20), text("in-flight")]),
            (dialect.long_query.to_string(), Vec::new()),
        ],
        "tx-cancel",
    );
    let tx_id = tx_request.operation_id.clone();
    let running = {
        let manager = Arc::clone(&manager);
        let config = config.clone();
        let password = password.clone();
        tokio::spawn(async move { manager.execute_batch(&config, password, tx_request).await })
    };
    tokio::time::sleep(Duration::from_millis(500)).await;
    manager.cancel(&config.id, &tx_id).await.expect("transaction cancel accepted");
    let cancelled = running.await.unwrap().expect_err("cancelled transaction must fail");
    assert_eq!(cancelled.class, ErrorClass::Cancelled);
    assert_eq!(count_rows(&manager, &config, &password).await, 5, "cancelled transaction must not commit");
    // Re-inserting the same key proves the cancelled transaction released its row locks.
    let reinsert = batch(
        vec![(format!("INSERT INTO luxbase_live (id, name) VALUES ({}, {})", p(1), p(2)), vec![ParamValue::I64(20), text("after-cancel")])],
        "reinsert",
    );
    tokio::time::timeout(Duration::from_secs(8), manager.execute_batch(&config, password.clone(), reinsert))
        .await
        .expect("row lock from the cancelled transaction was released")
        .expect("reinsert after cancel");

    let refreshed = manager
        .metadata(
            &config,
            password.clone(),
            MetadataRequest {
                connection_id: config.id.clone(),
                schema: dialect.schema.clone(),
                table: None,
                include_tables: true,
                include_columns: false,
            },
        )
        .await;
    assert!(refreshed.is_ok(), "metadata after cancellation: {refreshed:?}");

    if let Some(timeout_query) = dialect.timeout_query {
        let mut short = config.clone();
        short.id = format!("{}-timeout", config.id);
        short.query_timeout_ms = 800;
        let started = Instant::now();
        let timed_out = manager
            .fetch(&short, password.clone(), request(&short, "timeout", timeout_query, Vec::new()))
            .await
            .expect_err("query must time out");
        assert_eq!(timed_out.class, ErrorClass::Timeout, "{timed_out:?}");
        assert!(started.elapsed() < Duration::from_millis(2500), "timeout took {:?}", started.elapsed());
        if let Some(active) = dialect.active_timeout_queries {
            tokio::time::sleep(Duration::from_millis(400)).await;
            let still_running = fetch(&manager, &config, &password, active).await.rows[0][0].as_i64();
            assert_eq!(still_running, Some(0), "timed-out query must be cancelled on the server");
        }
        manager.disconnect(&short.id).await.unwrap();
    }

    let ddl_schema = dialect.schema.clone().unwrap_or_default();
    let ddl = manager
        .ddl(&config, password.clone(), &ddl_schema, "luxbase_live")
        .await
        .expect("table DDL");
    assert!(ddl.to_ascii_lowercase().contains("create table") && ddl.contains("luxbase_live"), "{ddl}");

    let mut ordered = request(&config, "ordered", "SELECT id FROM luxbase_live ORDER BY id DESC", Vec::new());
    ordered.page_size = Some(2);
    let page = manager.fetch(&config, password.clone(), ordered).await.expect("ordered page");
    assert_eq!(page.rows.iter().map(|row| row[0].clone()).collect::<Vec<_>>(), vec![json!(20), json!(11)]);

    if let Some(schema) = dialect.schema.clone() {
        let probe = match dialect.protocol {
            Protocol::Mysql => "SELECT DATABASE()",
            _ => "SELECT current_schema()",
        };
        let mut scoped = request(&config, "scoped", probe, Vec::new());
        scoped.schema = Some(schema.clone());
        let result = manager.fetch(&config, password.clone(), scoped).await.expect("schema-scoped query");
        assert_eq!(result.rows[0][0], json!(schema));
    }

    let script = |sqls: &[&str]| SqlScriptRequest {
        connection_id: config.id.clone(),
        statements: sqls.iter().map(|sql| {
            let mut statement = request(&config, "script", sql, Vec::new());
            statement.schema = dialect.schema.clone();
            statement
        }).collect(),
        operation_id: format!("script-{}", uuid::Uuid::new_v4()),
        page_size: Some(2),
        allow_partial_commit: false,
    };
    let mixed = manager
        .run_script(
            &config,
            password.clone(),
            script(&[
                "SELECT id, name FROM luxbase_live ORDER BY id",
                "UPDATE luxbase_live SET name = 'scripted' WHERE id = 11",
                "SELECT name FROM luxbase_live WHERE id = 11",
            ]),
            true,
        )
        .await
        .expect("mixed script");
    assert!(mixed.failure.is_none() && mixed.committed, "{mixed:?}");
    assert_eq!(mixed.results.len(), 3);
    assert!(mixed.results[0].returns_rows && mixed.results[0].rows.len() == 2 && mixed.results[0].has_more);
    assert!(!mixed.results[1].returns_rows && mixed.results[1].rows_affected == 1);
    assert_eq!(mixed.results[2].rows[0][0], json!("scripted"));

    let failing = manager
        .run_script(
            &config,
            password.clone(),
            script(&[
                "UPDATE luxbase_live SET name = 'should-roll-back' WHERE id = 11",
                "INSERT INTO luxbase_live (id, name) VALUES (1, 'duplicate')",
                "SELECT 1",
            ]),
            true,
        )
        .await
        .expect("failing script still returns a result");
    let failure = failing.failure.as_ref().expect("duplicate key reported");
    assert_eq!((failure.index, failure.class.clone()), (1, ErrorClass::Conflict), "{failing:?}");
    assert!(!failing.committed && failing.results.len() == 1);
    let name = fetch(&manager, &config, &password, "SELECT name FROM luxbase_live WHERE id = 11").await;
    assert_eq!(name.rows[0][0], json!("scripted"), "failed script must roll back");

    // A cached `SELECT *` statement must survive the table gaining a column.
    let star = "SELECT * FROM luxbase_live ORDER BY id";
    let before = fetch(&manager, &config, &password, star).await.columns.len();
    execute("ALTER TABLE luxbase_live ADD COLUMN added_later INT".into(), Vec::new()).await.expect("alter");
    for _ in 0..4 {
        let after = fetch(&manager, &config, &password, star).await;
        assert_eq!(after.columns.len(), before + 1, "stale cached statement");
        assert_eq!(after.columns.last().map(|column| column.name.as_str()), Some("added_later"));
    }

    let schema_detail = manager
        .metadata(
            &config,
            password.clone(),
            MetadataRequest {
                connection_id: config.id.clone(),
                schema: Some(dialect.schema.clone().unwrap_or_else(|| "main".into())),
                table: None,
                include_tables: true,
                include_columns: true,
            },
        )
        .await
        .expect("bulk schema metadata");
    let live = schema_detail.databases[0]
        .tables
        .iter()
        .find(|table| table.name == "luxbase_live")
        .expect("luxbase_live in bulk metadata");
    assert!(live.columns.len() >= 9 && live.columns.iter().any(|column| column.is_primary_key), "{live:?}");
    assert!(dialect.protocol == Protocol::Sqlite || !live.indexes.is_empty(), "{live:?}");

    if dialect.protocol == Protocol::Mysql {
        let routine = manager
            .run_script(
                &config,
                password.clone(),
                script(&[
                    "DROP PROCEDURE IF EXISTS luxbase_live_proc",
                    "CREATE PROCEDURE luxbase_live_proc() SELECT 1",
                    "DROP PROCEDURE luxbase_live_proc",
                ]),
                false,
            )
            .await
            .expect("statements outside the prepared protocol");
        assert!(routine.failure.is_none(), "{routine:?}");
    }

    let stats_schema = dialect.schema.clone().unwrap_or_else(|| "main".into());
    let stats = manager
        .object_stats(&config, password.clone(), &stats_schema)
        .await
        .expect("object stats");
    let live_stat = stats.iter().find(|stat| stat.name == "luxbase_live").expect("luxbase_live in object stats");
    assert!(!live_stat.object_type.contains("VIEW"), "{live_stat:?}");
    if dialect.protocol != Protocol::Sqlite {
        assert!(live_stat.data_bytes.is_some_and(|bytes| bytes > 0), "{live_stat:?}");
    }

    execute("DROP TABLE IF EXISTS luxbase_live_composite".into(), Vec::new()).await.expect("drop composite");
    execute(
        "CREATE TABLE luxbase_live_composite (tenant_id INT NOT NULL, item_id INT NOT NULL, value TEXT NOT NULL, big_value BIGINT NOT NULL, PRIMARY KEY (tenant_id, item_id))".into(),
        Vec::new(),
    )
    .await
    .expect("create composite");
    let composite_insert = format!(
        "INSERT INTO luxbase_live_composite (tenant_id, item_id, value, big_value) VALUES ({}, {}, {}, {})",
        p(1), p(2), p(3), p(4)
    );
    for (tenant, item, value, big) in [
        (7, 1, "first", 9_007_199_254_740_993_i64),
        (7, 2, "second", 9_007_199_254_740_994_i64),
    ] {
        let inserted = execute(
            composite_insert.clone(),
            vec![ParamValue::I64(tenant), ParamValue::I64(item), text(value), ParamValue::I64(big)],
        )
        .await
        .expect("insert composite row");
        assert_eq!(inserted.rows_affected, 1);
    }
    let updated = execute(
        format!("UPDATE luxbase_live_composite SET value = {} WHERE tenant_id = {} AND item_id = {}", p(1), p(2), p(3)),
        vec![text("updated"), ParamValue::I64(7), ParamValue::I64(1)],
    )
    .await
    .expect("update by composite primary key");
    assert_eq!(updated.rows_affected, 1);
    let composite = fetch(
        &manager,
        &config,
        &password,
        "SELECT tenant_id, item_id, value, big_value FROM luxbase_live_composite ORDER BY item_id",
    )
    .await;
    assert_eq!(composite.rows.len(), 2);
    assert_eq!(composite.rows[0][2], json!("updated"));
    assert_eq!(composite.rows[0][3], json!("9007199254740993"));

    let export_path = std::env::temp_dir().join(format!("luxbase-query-export-{}.csv", uuid::Uuid::new_v4()));
    let mut export_request = request(
        &config,
        "query-export",
        "SELECT tenant_id, item_id, value, big_value FROM luxbase_live_composite ORDER BY item_id",
        Vec::new(),
    );
    export_request.page_size = Some(1);
    export_request.offset = Some(1);
    export_request.schema = dialect.schema.clone();
    let exported = manager
        .export_query(
            &config,
            password.clone(),
            SqlQueryExportRequest {
                request: export_request,
                format: "csv".into(),
                path: export_path.to_string_lossy().into_owned(),
            },
        )
        .await
        .expect("stream complete query export");
    assert_eq!(exported.rows_exported, 2, "UI pagination must not limit complete export");
    let csv = std::fs::read_to_string(&export_path).expect("read exported CSV");
    assert!(csv.starts_with("tenant_id,item_id,value,big_value\n"), "{csv}");
    assert!(csv.contains("7,1,updated,9007199254740993") && csv.contains("7,2,second,9007199254740994"), "{csv}");
    let _ = std::fs::remove_file(export_path);

    let deleted = execute(
        format!("DELETE FROM luxbase_live_composite WHERE (tenant_id = {} AND item_id = {}) OR (tenant_id = {} AND item_id = {})", p(1), p(2), p(3), p(4)),
        vec![ParamValue::I64(7), ParamValue::I64(1), ParamValue::I64(7), ParamValue::I64(2)],
    )
    .await
    .expect("delete by composite primary keys");
    assert_eq!(deleted.rows_affected, 2);

    let metrics = manager.metrics(&config.id).await.expect("metrics");
    assert!(metrics.total >= 1 && metrics.max_size == 4, "{metrics:?}");
    execute("DROP TABLE luxbase_live_composite".into(), Vec::new()).await.expect("cleanup composite");
    execute("DROP TABLE luxbase_live".into(), Vec::new()).await.expect("cleanup");
    manager.close_all().await;
}

fn mysql_dialect() -> Dialect {
    Dialect {
        protocol: Protocol::Mysql,
        schema: None,
        placeholder: question,
        create: "CREATE TABLE luxbase_live (id INT PRIMARY KEY, name VARCHAR(64) NOT NULL, \
                 amount DECIMAL(12,2), created_at DATETIME, payload JSON, data VARBINARY(16), \
                 flag BOOLEAN, ratio FLOAT) DEFAULT CHARSET=utf8mb4",
        bytes_expr: "UNHEX('DEADBEEF')",
        long_query: "SELECT SLEEP(6)",
        timeout_query: Some("SELECT SLEEP(3)"),
        active_timeout_queries: Some(
            "SELECT COUNT(*) FROM information_schema.processlist WHERE info LIKE 'SELECT SLEEP(3)%'",
        ),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn live_mysql() {
    let Some(target) = target("LUXBASE_LIVE_MYSQL") else { return };
    let mut dialect = mysql_dialect();
    dialect.schema = Some(target.database.clone());
    run_sql_suite(config("live-mysql", "mysql", &target, json!({})), target.password.clone(), dialect).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn live_mysql_native_password() {
    let Some(target) = target("LUXBASE_LIVE_MYSQL_NATIVE") else { return };
    let config = config("live-mysql-native", "mysql", &target, json!({ "tlsMode": "disabled" }));
    let manager = SqlConnectionManager::default();
    let tested = manager.test(&config, target.password.clone()).await;
    assert!(tested.ok, "mysql_native_password login failed: {tested:?}");
    let wrong = manager.test(&config, Some("nope".into())).await;
    assert_eq!(wrong.error_class, Some(ErrorClass::PermissionDenied), "{wrong:?}");
    let result = manager
        .fetch(&config, target.password.clone(), request(&config, "whoami", "SELECT CURRENT_USER() AS who, @@version AS version", Vec::new()))
        .await
        .expect("query through mysql_native_password session");
    assert!(result.rows[0][0].as_str().unwrap().starts_with(&format!("{}@", target.user)));
    let mut dialect = mysql_dialect();
    dialect.schema = Some(target.database.clone());
    dialect.active_timeout_queries = None;
    run_sql_suite(config, target.password.clone(), dialect).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn live_postgres() {
    let Some(target) = target("LUXBASE_LIVE_POSTGRES") else { return };
    let dialect = Dialect {
        protocol: Protocol::Postgres,
        schema: Some("public".into()),
        placeholder: dollar,
        create: "CREATE TABLE luxbase_live (id INT PRIMARY KEY, name VARCHAR(64) NOT NULL, \
                 amount NUMERIC(12,2), created_at TIMESTAMP, payload JSONB, data BYTEA, \
                 flag BOOLEAN, ratio REAL)",
        bytes_expr: "decode('deadbeef', 'hex')",
        long_query: "SELECT pg_sleep(6)",
        timeout_query: Some("SELECT pg_sleep(3)"),
        active_timeout_queries: Some(
            "SELECT COUNT(*) FROM pg_stat_activity WHERE state = 'active' AND query LIKE 'SELECT pg_sleep(3)%'",
        ),
    };
    run_sql_suite(config("live-postgres", "postgres", &target, json!({ "tlsMode": "disabled" })), target.password.clone(), dialect).await;

    let config = config("live-postgres-numeric", "postgres", &target, json!({ "tlsMode": "disabled" }));
    let manager = SqlConnectionManager::default();
    let result = fetch(
        &manager,
        &config,
        &target.password,
        "SELECT 12.50::numeric(12,2), (-0.0042)::numeric, 123456789.000100::numeric, 'NaN'::numeric, 0::numeric(5,3), NULL::numeric",
    )
    .await;
    assert_eq!(
        result.rows[0],
        vec![json!("12.50"), json!("-0.0042"), json!("123456789.000100"), json!("NaN"), json!("0.000"), Value::Null],
        "NUMERIC must render exactly as psql does"
    );
    manager.close_all().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn live_sqlite() {
    let path = std::env::temp_dir().join(format!("luxbase-live-{}.sqlite3", uuid::Uuid::new_v4()));
    rusqlite::Connection::open(&path).unwrap();
    let target = Target {
        host: String::new(),
        port: 0,
        user: String::new(),
        password: None,
        database: String::new(),
    };
    let config = config(
        "live-sqlite",
        "sqlite",
        &target,
        json!({ "filePath": path.to_string_lossy() }),
    );
    let dialect = Dialect {
        protocol: Protocol::Sqlite,
        schema: None,
        placeholder: question,
        create: "CREATE TABLE luxbase_live (id INTEGER PRIMARY KEY, name TEXT NOT NULL, \
                 amount NUMERIC, created_at TEXT, payload TEXT, data BLOB, flag BOOLEAN, ratio REAL)",
        bytes_expr: "X'DEADBEEF'",
        long_query: "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c) SELECT max(x) FROM c",
        timeout_query: None,
        active_timeout_queries: None,
    };
    run_sql_suite(config, None, dialect).await;
    let _ = std::fs::remove_file(&path);
}

/// LUXBASE_LIVE_SSH="host|port|user|private_key_path|known_hosts_path" tunnels the
/// LUXBASE_LIVE_MYSQL and LUXBASE_LIVE_POSTGRES targets (resolved from the SSH server).
#[tokio::test(flavor = "multi_thread")]
async fn live_ssh_tunnel() {
    let Some(raw) = std::env::var("LUXBASE_LIVE_SSH").ok() else { return };
    let parts = raw.split('|').collect::<Vec<_>>();
    let ssh = |key: &str, known_hosts: &str| {
        json!({
            "host": parts[0],
            "port": parts[1].parse::<u16>().unwrap(),
            "username": parts[2],
            "privateKeyPath": key,
            "knownHostsPath": known_hosts,
            "strictHostKeyChecking": true
        })
    };
    let good = ssh(parts[3], parts[4]);
    let manager = Arc::new(SqlConnectionManager::default());
    for (variable, protocol, sql) in [
        ("LUXBASE_LIVE_MYSQL", "mysql", "SELECT SLEEP(0.2) AS slept, CONNECTION_ID() AS id"),
        ("LUXBASE_LIVE_POSTGRES", "postgres", "SELECT pg_sleep(0.2)::text AS slept, pg_backend_pid() AS id"),
    ] {
        let Some(target) = target(variable) else { continue };
        let config = config(&format!("live-ssh-{protocol}"), protocol, &target, json!({ "ssh": good }));
        let tested = manager.test(&config, target.password.clone()).await;
        assert!(tested.ok && tested.server_version.is_some(), "{protocol} over SSH: {tested:?}");

        // More concurrent queries than pooled connections forces several forwarded channels
        // to carry interleaved traffic at once.
        let started = Instant::now();
        let mut tasks = Vec::new();
        for _ in 0..8 {
            let manager = Arc::clone(&manager);
            let config = config.clone();
            let password = target.password.clone();
            let sql = sql.to_string();
            tasks.push(tokio::spawn(async move {
                manager.fetch(&config, password, request(&config, "ssh", &sql, Vec::new())).await
            }));
        }
        for task in tasks {
            let result = task.await.unwrap().unwrap_or_else(|error| panic!("{protocol} over SSH: {error:?}"));
            assert_eq!(result.rows.len(), 1);
        }
        assert!(started.elapsed() < Duration::from_secs(10), "{protocol} tunnel took {:?}", started.elapsed());
        manager.disconnect(&config.id).await.unwrap();

        let empty_known_hosts = std::env::temp_dir().join(format!("luxbase-empty-known-hosts-{}", uuid::Uuid::new_v4()));
        std::fs::write(&empty_known_hosts, "").unwrap();
        let pinned = config_with_ssh(&config, ssh(parts[3], &empty_known_hosts.to_string_lossy()));
        let refused = manager.test(&pinned, target.password.clone()).await;
        assert!(!refused.ok, "unknown host key must be refused");
        assert!(refused.message.as_deref().unwrap_or("").contains("known_hosts"), "{refused:?}");
        let _ = std::fs::remove_file(empty_known_hosts);

        let wrong_key = config_with_ssh(&config, ssh("/nonexistent/luxbase-key", parts[4]));
        let refused = manager.test(&wrong_key, target.password.clone()).await;
        assert!(!refused.ok);
        assert!(refused.message.as_deref().unwrap_or("").starts_with("SSH tunnel"), "{refused:?}");
    }
    manager.close_all().await;
}

fn config_with_ssh(config: &ConnectionConfig, ssh: Value) -> ConnectionConfig {
    let mut config = config.clone();
    config.id = format!("{}-{}", config.id, uuid::Uuid::new_v4());
    config.ssh = Some(serde_json::from_value(ssh).unwrap());
    config
}

/// LUXBASE_LIVE_VAULT=1 stores a throw-away credential in the local vault, then proves that the
/// credential never reached the OS keychain and never sits in the database file as plaintext.
#[cfg(target_os = "macos")]
#[test]
fn live_local_vault_roundtrip() {
    if std::env::var("LUXBASE_LIVE_VAULT").ok().as_deref() != Some("1") {
        return;
    }
    let dir = std::env::temp_dir().join(format!("luxbase-live-vault-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).expect("create vault dir");
    let secrets = crate::security::SecretStore::new(dir.join("vault.key"));
    let store =
        crate::storage::LocalStore::open(dir.join("store.sqlite3"), secrets).expect("open local store");

    let id = format!("live-vault-{}", uuid::Uuid::new_v4());
    let secret = format!("pw-{}", uuid::Uuid::new_v4());
    let placeholder = Target {
        host: "localhost".into(),
        port: 3306,
        user: "vault".into(),
        password: None,
        database: String::new(),
    };
    let config = config(
        &id,
        "sqlite",
        &placeholder,
        json!({ "filePath": dir.join("data.sqlite3").to_string_lossy() }),
    );
    store
        .save_connection(&config, Some(&secret), None, None)
        .expect("save connection");
    assert_eq!(
        store.connection_password(&id).unwrap().as_deref(),
        Some(secret.as_str())
    );

    let account = format!("connection:{id}:password");
    let external = std::process::Command::new("/usr/bin/security")
        .args(["find-generic-password", "-s", "com.luxbase.studio", "-a", &account])
        .output()
        .expect("run security");
    assert!(
        !external.status.success(),
        "a credential was written to the OS keychain: {account}"
    );

    let mut stored_bytes: Vec<u8> = Vec::new();
    for name in ["store.sqlite3", "store.sqlite3-wal", "store.sqlite3-shm"] {
        if let Ok(bytes) = std::fs::read(dir.join(name)) {
            stored_bytes.extend_from_slice(&bytes);
        }
    }
    assert!(
        !stored_bytes
            .windows(secret.len())
            .any(|window| window == secret.as_bytes()),
        "the local database holds the password as plaintext"
    );

    store.delete_connection(&id).expect("delete connection");
    assert_eq!(store.connection_password(&id).unwrap(), None);
    std::fs::remove_dir_all(&dir).ok();
}

fn redis_request(config: &ConnectionConfig, command: &str, args: &[&str], confirmed: bool) -> RedisCommandRequest {
    RedisCommandRequest {
        connection_id: config.id.clone(),
        command: command.into(),
        args: args.iter().map(|arg| arg.to_string()).collect(),
        operation_id: format!("redis-{}", uuid::Uuid::new_v4()),
        confirmed_dangerous: confirmed,
    }
}

fn redis_value(key: &str, ttl: i64, value: RedisValueBody) -> RedisValue {
    RedisValue {
        key: key.into(),
        key_type: String::new(),
        ttl_seconds: ttl,
        encoding: None,
        size_bytes: None,
        raw_base64: None,
        value,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn live_redis() {
    let Some(target) = target("LUXBASE_LIVE_REDIS") else { return };
    // The third field is the password for Redis; there is no username or database.
    let password = Some(target.user.clone()).filter(|value| !value.is_empty());
    let target = Target { user: String::new(), ..target };
    let config = config("live-redis", "redis", &target, json!({ "redisMode": "standalone", "redisDb": 2 }));
    let manager = Arc::new(KvConnectionManager::default());

    let tested = manager.test(&config, password.clone()).await;
    assert!(tested.ok, "{tested:?}");
    assert!(tested.server_version.as_deref().is_some_and(|value| value.starts_with("Redis ")), "{tested:?}");
    let wrong = manager.test(&config, Some("wrong".into())).await;
    assert_eq!(wrong.error_class, Some(ErrorClass::PermissionDenied), "{wrong:?}");
    let anonymous = manager.test(&config, None).await;
    assert_eq!(anonymous.error_class, Some(ErrorClass::PermissionDenied), "{anonymous:?}");

    let prefix = "luxbase:live:";
    let values = vec![
        ("string", RedisValueBody::String("中文 value".into())),
        ("hash", RedisValueBody::Hash([("f1".to_string(), "v1".to_string()), ("f2".to_string(), "v2".to_string())].into())),
        ("list", RedisValueBody::List(vec![
            crate::models::IndexedString { index: 0, value: "a".into() },
            crate::models::IndexedString { index: 1, value: "b".into() },
        ])),
        ("set", RedisValueBody::Set(vec!["x".into(), "y".into()])),
        ("zset", RedisValueBody::ZSet(vec![
            crate::models::ScoredMember { member: "low".into(), score: 1.5 },
            crate::models::ScoredMember { member: "high".into(), score: 9.0 },
        ])),
    ];
    for (kind, body) in values {
        let key = format!("{prefix}{kind}");
        manager
            .set(&config, password.clone(), key.as_bytes(), 2, redis_value(&key, 120, body), "redis-set")
            .await
            .unwrap_or_else(|error| panic!("set {kind}: {error:?}"));
        let loaded = manager.get(&config, password.clone(), key.as_bytes(), 2, "redis-get").await.expect("get");
        assert_eq!(loaded.key_type, kind);
        assert!(loaded.encoding.as_deref().is_some_and(|value| !value.is_empty()), "{kind} encoding");
        assert!((1..=120).contains(&loaded.ttl_seconds), "{kind} ttl {}", loaded.ttl_seconds);
        match (kind, &loaded.value) {
            ("string", RedisValueBody::String(value)) => assert_eq!(value, "中文 value"),
            ("hash", RedisValueBody::Hash(fields)) => assert_eq!(fields.get("f2").map(String::as_str), Some("v2")),
            ("list", RedisValueBody::List(items)) => assert_eq!(items.len(), 2),
            ("set", RedisValueBody::Set(items)) => assert_eq!(items.len(), 2),
            ("zset", RedisValueBody::ZSet(items)) => assert_eq!(items[0].member, "low"),
            other => panic!("unexpected value shape {other:?}"),
        }
    }

    let mut cursor = 0;
    let mut found = Vec::new();
    loop {
        let page = manager
            .scan(
                &config,
                password.clone(),
                RedisScanRequest {
                    connection_id: config.id.clone(),
                    cursor,
                    pattern: format!("{prefix}*"),
                    count: 2,
                    type_filter: None,
                    db: 2,
                    operation_id: format!("scan-{}", uuid::Uuid::new_v4()),
                },
            )
            .await
            .expect("scan");
        found.extend(page.keys.into_iter().map(|key| (key.key, key.key_type)));
        cursor = page.cursor;
        if cursor == 0 {
            break;
        }
    }
    found.sort();
    assert_eq!(found.len(), 5, "{found:?}");
    let other_db = manager
        .scan(
            &config,
            password.clone(),
            RedisScanRequest {
                connection_id: config.id.clone(),
                cursor: 0,
                pattern: format!("{prefix}*"),
                count: 100,
                type_filter: None,
                db: 0,
                operation_id: "scan-db0".into(),
            },
        )
        .await
        .expect("scan db 0");
    assert!(other_db.keys.is_empty(), "keys leaked across databases");

    let info = manager.info(&config, password.clone(), "info").await.expect("info");
    assert!(info.memory_used_bytes.unwrap_or(0) > 0);
    assert_eq!(info.replication_role.as_deref(), Some("master"));

    let pong = manager.execute_raw(&config, password.clone(), redis_request(&config, "PING", &[], false)).await.expect("PING");
    assert_eq!(pong.value, json!("PONG"));

    let create_key = format!("{prefix}create-safe");
    let mut created_value = redis_value(&create_key, 90, RedisValueBody::String("original".into()));
    created_value.key_type = "string".into();
    manager.create(&config, password.clone(), 2, created_value, "redis-create").await.expect("create new key");
    let mut duplicate_value = redis_value(&create_key, 30, RedisValueBody::String("replacement".into()));
    duplicate_value.key_type = "string".into();
    let duplicate = manager
        .create(&config, password.clone(), 2, duplicate_value, "redis-create-duplicate")
        .await
        .expect_err("duplicate create must not overwrite");
    assert_eq!(duplicate.class, ErrorClass::Conflict);
    let preserved = manager.get(&config, password.clone(), create_key.as_bytes(), 2, "redis-get-created").await.expect("get created key");
    assert!(matches!(preserved.value, RedisValueBody::String(ref value) if value == "original"));
    assert!((1..=90).contains(&preserved.ttl_seconds), "duplicate create changed TTL: {}", preserved.ttl_seconds);

    let keys = manager.execute_raw(&config, password.clone(), redis_request(&config, "KEYS", &["*"], false)).await.expect_err("KEYS blocked");
    assert_eq!(keys.class, ErrorClass::ProtocolUnsupported);
    let flush = manager.execute_raw(&config, password.clone(), redis_request(&config, "FLUSHDB", &[], false)).await.expect_err("FLUSHDB needs confirmation");
    assert_eq!(flush.class, ErrorClass::Validation);

    let blocking = redis_request(&config, "BLPOP", &["luxbase:live:never", "10"], false);
    let blocking_id = blocking.operation_id.clone();
    let started = Instant::now();
    let running = {
        let manager = Arc::clone(&manager);
        let config = config.clone();
        let password = password.clone();
        tokio::spawn(async move { manager.execute_raw(&config, password, blocking).await })
    };
    tokio::time::sleep(Duration::from_millis(500)).await;
    manager.cancel(&config.id, &blocking_id).await.expect("cancel BLPOP");
    let cancelled = running.await.unwrap().expect_err("BLPOP cancelled");
    assert_eq!(cancelled.class, ErrorClass::Cancelled);
    assert!(started.elapsed() < Duration::from_secs(4));
    let pong = manager.execute_raw(&config, password.clone(), redis_request(&config, "PING", &[], false)).await.expect("PING after cancel");
    assert_eq!(pong.value, json!("PONG"));

    // get/set/del are DB-aware now; DB 2 is passed explicitly rather than relying on the last SCAN.
    let dbsize = manager.dbsize(&config, password.clone(), 2, "dbsize").await.expect("dbsize");
    assert!(dbsize >= 6, "DB 2 should hold the live keys, got {dbsize}");
    let mut all_keys = found.iter().map(|(key, _)| key.as_bytes().to_vec()).collect::<Vec<_>>();
    all_keys.push(create_key.as_bytes().to_vec());
    let unconfirmed = manager.del(&config, password.clone(), &all_keys, 2, false, "del").await.expect_err("bulk DEL needs confirmation");
    assert_eq!(unconfirmed.class, ErrorClass::Validation);
    let deleted = manager.del(&config, password.clone(), &all_keys, 2, true, "del").await.expect("confirmed DEL");
    assert_eq!(deleted, 6);
    manager.close_all().await;
}

/// Live Elasticsearch check. The secret is never passed on the command line: only the
/// host/port/user come from `LUXBASE_LIVE_ES="host|port|user"`, while the password is read from
/// `LUXBASE_LIVE_ES_PASSWORD_FILE` (default /tmp/luxbase-live/es-password). All writes target a
/// throw-away `luxbase_qa_*` index that is deleted before the test ends.
#[tokio::test(flavor = "multi_thread")]
async fn live_elasticsearch() {
    let Some(raw) = std::env::var("LUXBASE_LIVE_ES").ok() else { return };
    let parts = raw.split('|').collect::<Vec<_>>();
    let host = parts.first().map(|value| value.to_string()).unwrap_or_default();
    let port: u16 = parts.get(1).and_then(|v| v.parse().ok()).unwrap_or(9200);
    let user = parts.get(2).map(|value| value.to_string()).unwrap_or_else(|| "elastic".into());
    if host.is_empty() { return; }
    let password_file = std::env::var("LUXBASE_LIVE_ES_PASSWORD_FILE")
        .unwrap_or_else(|_| "/tmp/luxbase-live/es-password".into());
    let password = std::fs::read_to_string(&password_file)
        .expect("read ES password file")
        .trim()
        .to_string();
    let target = Target { host, port, user, password: Some(password.clone()), database: String::new() };
    let config = config("live-es", "elasticsearch", &target, json!({ "esAuthMode": "basic", "tlsMode": "disabled" }));
    let manager = Arc::new(SearchConnectionManager::default());

    let tested = manager.test(&config, Some(password.clone())).await;
    assert!(tested.ok, "{tested:?}");
    assert!(tested.server_version.as_deref().is_some_and(|v| v.starts_with("Elasticsearch")), "{tested:?}");

    let info = manager.info(&config, Some(password.clone()), "es-info").await.expect("info");
    assert!(info.version.is_some(), "{info:?}");
    let indices = manager.list_indices(&config, Some(password.clone()), "es-indices").await.expect("indices");
    println!("live ES cluster {:?} version {:?} with {} indices", info.cluster_name, info.version, indices.len());

    // Remove leftovers from earlier aborted runs. The luxbase_qa_ namespace is created only by
    // this test, so sweeping it never touches the user's real indices.
    for stale in indices.iter().filter(|i| i.name.starts_with("luxbase_qa_")) {
        let _ = manager.delete_index(&config, Some(password.clone()), &stale.name, true, "es-clean-stale").await;
    }

    let index = format!("luxbase_qa_{}", &uuid::Uuid::new_v4().to_string()[..8]);
    manager.create_index(&config, Some(password.clone()), &index, json!({
        "settings": { "number_of_shards": 1, "number_of_replicas": 0 },
        "mappings": { "properties": { "level": { "type": "keyword" }, "msg": { "type": "text" } } }
    }), "es-create").await.expect("create index");

    let alias = format!("{index}_alias");
    manager.modify_aliases(&config, Some(password.clone()), json!([{ "add": { "index": index, "alias": alias } }]), true, "es-alias-add").await.expect("add alias");

    manager.put_document(&config, Some(password.clone()), &index, "1", json!({ "level": "info", "msg": "hello luxbase" }), "es-put").await.expect("put doc");
    manager.execute_rest(&config, Some(password.clone()), crate::models::EsRestRequest {
        connection_id: config.id.clone(), method: "POST".into(), path: format!("/{index}/_refresh"),
        body: None, operation_id: "es-refresh".into(), confirmed_dangerous: false,
    }).await.expect("refresh");

    let doc = manager.get_document(&config, Some(password.clone()), &index, "1", "es-get").await.expect("get doc");
    assert!(doc.found && doc.source.get("msg").and_then(Value::as_str) == Some("hello luxbase"), "{doc:?}");

    let search = manager.search(&config, Some(password.clone()), EsSearchRequest {
        connection_id: config.id.clone(), index: index.clone(),
        body: json!({ "query": { "match": { "msg": "luxbase" } } }),
        from: 0, size: 10, operation_id: "es-search".into(),
    }).await.expect("search");
    assert!(search.total >= 1 && search.hits.iter().any(|h| h.id == "1"), "{search:?}");

    let mapping = manager.get_mapping(&config, Some(password.clone()), &index, "es-mapping").await.expect("mapping");
    assert!(mapping.to_string().contains("level"), "{mapping}");
    let aliases = manager.list_aliases(&config, Some(password.clone()), "es-aliases").await.expect("aliases");
    assert!(aliases.iter().any(|a| a.alias == alias && a.index == index), "{aliases:?}");

    // Tear down everything this test created; never touch pre-existing indices.
    manager.modify_aliases(&config, Some(password.clone()), json!([{ "remove": { "index": index, "alias": alias } }]), true, "es-alias-remove").await.expect("remove alias");
    manager.delete_document(&config, Some(password.clone()), &index, "1", true, "es-del-doc").await.expect("delete doc");
    manager.delete_index(&config, Some(password.clone()), &index, true, "es-del-index").await.expect("delete index");
    let after = manager.list_indices(&config, Some(password.clone()), "es-indices-after").await.expect("indices after");
    assert!(!after.iter().any(|i| i.name == index), "disposable index was not removed");
    manager.close_all().await;
}

/// 100k-row disposable SQLite transfer, exercising keyset paging and batched commits.
#[tokio::test(flavor = "multi_thread")]
async fn live_mysql_row_edits() {
    let Some(target) = target("LUXBASE_LIVE_MYSQL") else { return };
    let (state, dir) = live_state("row-edits");
    let config = config("live-row-edits", "mysql", &target, json!({ "tlsMode": "disabled" }));
    state.store.save_connection(&config, target.password.as_deref(), None, None).expect("save row-edit connection");
    let password = target.password.clone();
    live_execute(&state, &config, &password, "DROP TABLE IF EXISTS luxbase_qa_rowedits").await;
    live_execute(&state, &config, &password, "CREATE TABLE luxbase_qa_rowedits (id INT PRIMARY KEY, name VARCHAR(64), amount DECIMAL(12,2), data VARBINARY(16))").await;
    live_execute(&state, &config, &password, "INSERT INTO luxbase_qa_rowedits VALUES (1, 'one', 10.00, UNHEX('DEADBEEF')), (2, 'two', 20.00, NULL)").await;
    let rows = vec![
        crate::commands::RowEdit {
            key_columns: vec!["id".into()], key_values: vec![json!(1)],
            changes: vec![
                crate::commands::RowEditChange { column: "name".into(), value: json!("one-edited") },
                crate::commands::RowEditChange { column: "amount".into(), value: json!("11.50") },
                crate::commands::RowEditChange { column: "data".into(), value: json!({ "bytesBase64": "3q2+7w==" }) },
            ],
        },
        crate::commands::RowEdit {
            key_columns: vec!["id".into()], key_values: vec![json!(2)],
            changes: vec![crate::commands::RowEditChange { column: "name".into(), value: json!("two-edited") }],
        },
    ];
    let applied = crate::commands::apply_row_edits(&state, &config.id, &target.database, "luxbase_qa_rowedits", &rows, "qa-row-edits").await.expect("apply staged row edits");
    assert_eq!(applied.rows_affected, 2, "two rows updated in one transaction");
    let verified = fetch(&state.sql, &config, &target.password, "SELECT name, amount, HEX(data) FROM luxbase_qa_rowedits WHERE id = 1").await;
    assert_eq!(verified.rows[0][0].as_str().unwrap(), "one-edited");
    let amount = &verified.rows[0][1];
    assert!(amount.as_str().map(|text| text == "11.50").unwrap_or_else(|| amount.as_f64() == Some(11.5)), "amount updated: {amount}");
    assert_eq!(verified.rows[0][2].as_str().unwrap(), "DEADBEEF");
    // Second row overflows VARCHAR(64) in strict mode, so the whole batch must roll back.
    let failing = vec![
        crate::commands::RowEdit {
            key_columns: vec!["id".into()], key_values: vec![json!(1)],
            changes: vec![crate::commands::RowEditChange { column: "name".into(), value: json!("must-rollback") }],
        },
        crate::commands::RowEdit {
            key_columns: vec!["id".into()], key_values: vec![json!(2)],
            changes: vec![crate::commands::RowEditChange { column: "name".into(), value: json!("x".repeat(100)) }],
        },
    ];
    let rejected = crate::commands::apply_row_edits(&state, &config.id, &target.database, "luxbase_qa_rowedits", &failing, "qa-row-edits-rollback").await;
    assert!(rejected.is_err(), "over-long value must fail the batch: {rejected:?}");
    let after = fetch(&state.sql, &config, &target.password, "SELECT name FROM luxbase_qa_rowedits WHERE id = 1").await;
    assert_eq!(after.rows[0][0].as_str().unwrap(), "one-edited", "first row must roll back with the failed batch");
    live_execute(&state, &config, &password, "DROP TABLE luxbase_qa_rowedits").await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn live_transfer_perf_sqlite() {
    let (state, dir) = live_state("transfer-perf");
    let src_path = dir.join("src.sqlite3");
    let dst_path = dir.join("dst.sqlite3");
    // The SQLite driver requires the file to exist before opening; create empty files.
    let _ = std::fs::write(&src_path, b"");
    let _ = std::fs::write(&dst_path, b"");
    let placeholder = Target { host: "localhost".into(), port: 0, user: "vault".into(), password: None, database: String::new() };
    let secret = "perf-secret".to_string();
    let src_config = config("perf-src", "sqlite", &placeholder, json!({ "filePath": src_path.to_string_lossy() }));
    let dst_config = config("perf-dst", "sqlite", &placeholder, json!({ "filePath": dst_path.to_string_lossy() }));
    for cfg in [&src_config, &dst_config] {
        state.store.save_connection(cfg, Some(&secret), None, None).expect("save perf connection");
    }
    live_execute(&state, &src_config, &Some(secret.clone()), "CREATE TABLE luxbase_perf (id INTEGER PRIMARY KEY, name TEXT, amount REAL)").await;
    live_execute(&state, &src_config, &Some(secret.clone()), "CREATE INDEX luxbase_perf_name ON luxbase_perf(name)").await;
    for batch in 0..100u32 {
        let values = (0..1000u32)
            .map(|i| format!("('n{}',{})", batch * 1000 + i, i % 97))
            .collect::<Vec<_>>()
            .join(",");
        live_execute(&state, &src_config, &Some(secret.clone()), &format!("INSERT INTO luxbase_perf (name, amount) VALUES {values}")).await;
    }
    let request = crate::models::TransferRequest {
        source_connection_id: src_config.id.clone(),
        target_connection_id: dst_config.id.clone(),
        target_schema: "main".into(),
        tables: vec![crate::models::TransferTableRef { schema: "main".into(), table: "luxbase_perf".into(), target_table: None }],
        mode: crate::models::TransferMode::Append,
        create_missing: true,
        create_indexes: true,
        disable_indexes: true,
        disable_checks: true,
        batch_size: Some(5000),
        operation_id: "perf-100k".into(),
    };
    let source = crate::transfer::SideConnection { config: src_config.clone(), password: Some(secret.clone()) };
    let target = crate::transfer::SideConnection { config: dst_config.clone(), password: Some(secret.clone()) };
    println!("seeded source rows: {}", live_count(&state, &src_config, &Some(secret.clone()), "luxbase_perf").await);
    let job = std::sync::Arc::new(crate::jobs::Job::new(crate::transfer::initial_progress(&request, &src_config, &dst_config)));
    let started = Instant::now();
    crate::transfer::run_transfer(state.clone(), job.clone(), request, source, target).await;
    let elapsed = started.elapsed().as_millis();
    let snapshot = job.snapshot();
    println!("transfer 100k rows: {elapsed} ms · status {} · read {} · written {}", snapshot.status, snapshot.rows_read, snapshot.rows_written);
    assert_eq!(snapshot.status, "done", "{:?}", snapshot.error);
    assert_eq!((snapshot.rows_read, snapshot.rows_written), (100_000, 100_000));
    assert_eq!(snapshot.total_rows, 100_000);
    assert_eq!(live_count(&state, &dst_config, &Some(secret.clone()), "luxbase_perf").await, 100_000);
    let meta = state.sql.metadata(&dst_config, Some(secret.clone()), crate::models::MetadataRequest {
        connection_id: dst_config.id.clone(), schema: Some("main".into()), table: Some("luxbase_perf".into()),
        include_tables: false, include_columns: true,
    }).await.expect("target metadata");
    let indexes = meta.databases.into_iter().flat_map(|db| db.tables).find(|t| t.name == "luxbase_perf").map(|t| t.indexes).unwrap_or_default();
    assert!(indexes.iter().any(|index| index.name.contains("luxbase_perf_name")), "secondary index must be recreated after the copy: {indexes:?}");
    let _ = std::fs::remove_dir_all(&dir);
}

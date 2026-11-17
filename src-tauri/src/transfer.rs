use crate::app_state::AppState;
use crate::commands::{json_to_param, state_ref};
use crate::drivers::sql::{value_placeholders, MAX_BIND_PARAMETERS};
use crate::error::{AppError, AppResult};
use crate::jobs::{ActiveOperation, Job};
use crate::models::{
    ColumnMetadata, ConnectionConfig, DriverFamily, ErrorClass, MetadataRequest, Protocol,
    SqlRequest, TableMetadata, TransferMode, TransferProgress, TransferRequest,
    TransferTableProgress,
};
use std::sync::Arc;
use std::time::Instant;
use tauri::State;

const MAX_TRANSFER_TABLES: usize = 200;
const MAX_TRANSFER_COLUMNS: usize = 1_000;

/// One resolved connection plus the credential its driver needs.
pub(crate) struct SideConnection {
    pub(crate) config: ConnectionConfig,
    pub(crate) password: Option<String>,
}

pub(crate) fn transfer_target_allowed(source: Protocol, target: Protocol) -> bool {
    target.family() == DriverFamily::Sql && (source == target || target == Protocol::Sqlite)
}

/// Rows copied per round trip, limited by how many bind placeholders the dialect allows.
pub(crate) fn transfer_batch_rows(batch_size: Option<u32>, columns: usize) -> u32 {
    let requested = batch_size.unwrap_or(2_000).clamp(1, 5_000) as usize;
    let by_parameters = (MAX_BIND_PARAMETERS / columns.max(1)).max(1);
    (requested.min(by_parameters)).clamp(1, 10_000) as u32
}

#[derive(Debug, Clone)]
pub(crate) struct CopyPlan {
    /// Column names copied from the source, in the order they are selected and inserted.
    pub(crate) columns: Vec<String>,
    /// Source columns used to keep paging deterministic.
    pub(crate) order_columns: Vec<String>,
    pub(crate) notes: Vec<String>,
}

pub(crate) fn build_copy_plan(
    source: &TableMetadata,
    target: &TableMetadata,
    mode: TransferMode,
) -> CopyPlan {
    let target_names = target
        .columns
        .iter()
        .map(|column| column.name.as_str())
        .collect::<Vec<_>>();
    let columns = source
        .columns
        .iter()
        .filter(|column| target_names.contains(&column.name.as_str()))
        .map(|column| column.name.clone())
        .collect::<Vec<_>>();
    let mut notes = Vec::new();
    let missing = source
        .columns
        .iter()
        .filter(|column| !target_names.contains(&column.name.as_str()))
        .map(|column| column.name.as_str())
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        notes.push(format!("目标表没有列 {}，这些列未写入。", missing.join("、")));
    }
    let extra = target
        .columns
        .iter()
        .filter(|column| !columns.contains(&column.name))
        .collect::<Vec<_>>();
    if !extra.is_empty() {
        let risky = extra
            .iter()
            .filter(|column| !column.nullable && column.default_value.is_none())
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>();
        if risky.is_empty() {
            notes.push(format!(
                "目标表多出的列 {} 按默认值填充。",
                extra.iter().map(|column| column.name.as_str()).collect::<Vec<_>>().join("、")
            ));
        } else {
            notes.push(format!(
                "目标列 {} 不可为空且没有默认值，插入可能失败；请先调整目标结构。",
                risky.join("、")
            ));
        }
    }
    if mode == TransferMode::Append
        && !target.columns.iter().any(|column| column.is_primary_key)
    {
        notes.push("目标表没有主键：重复运行传输会重复插入同样的行。".into());
    }
    let mut columns = columns;
    if columns.len() > MAX_TRANSFER_COLUMNS {
        columns.truncate(MAX_TRANSFER_COLUMNS);
        notes.push(format!("超过 {MAX_TRANSFER_COLUMNS} 列的表只传输前 {MAX_TRANSFER_COLUMNS} 列。"));
    }
    let order_columns = source
        .columns
        .iter()
        .filter(|column| column.is_primary_key && columns.contains(&column.name))
        .map(|column| column.name.clone())
        .collect::<Vec<_>>();
    CopyPlan {
        order_columns,
        columns,
        notes,
    }
}

fn quote_identifier(protocol: Protocol, name: &str) -> String {
    match protocol {
        Protocol::Mysql => format!("`{}`", name.replace('`', "``")),
        _ => format!("\"{}\"", name.replace('"', "\"\"")),
    }
}

pub(crate) fn qualified_table(protocol: Protocol, schema: &str, table: &str) -> String {
    match protocol {
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
        _ => format!("\"{}\"", table.replace('"', "\"\"")),
    }
}

/// Map a server column type onto the SQLite storage classes.
pub(crate) fn sqlite_type_for(data_type: &str) -> &'static str {
    let name = data_type
        .trim()
        .to_ascii_lowercase()
        .split('(')
        .next()
        .unwrap_or("")
        .replace("unsigned", "")
        .replace("[]", "")
        .trim()
        .to_string();
    let head = name.split_whitespace().next().unwrap_or(name.as_str());
    match head {
        "tinyint" | "smallint" | "mediumint" | "int" | "integer" | "bigint" | "int2" | "int8"
        | "serial" | "bigserial" | "smallserial" | "oid" | "bool" | "boolean" => "INTEGER",
        "decimal" | "numeric" | "money" | "smallmoney" | "float" | "double" | "real" | "float4"
        | "float8" => "NUM",
        "date" | "time" | "datetime" | "timestamp" | "timestamptz" | "timetz" | "interval" => {
            "TEXT"
        }
        "binary" | "varbinary" | "blob" | "bytea" | "longblob" | "mediumblob" | "tinyblob"
        | "geometry" | "point" | "line" | "lseg" | "path" | "polygon" | "box" => "BLOB",
        _ => "TEXT",
    }
}

/// Only literal defaults survive the trip into SQLite; expressions are dialect-specific.
pub(crate) fn sqlite_default(raw: Option<&str>) -> Option<String> {
    let text = raw?.trim();
    if text.is_empty() || text.eq_ignore_ascii_case("null") {
        return None;
    }
    let numeric = text.contains(|c: char| c.is_ascii_digit())
        && text
            .chars()
            .all(|c| c.is_ascii_digit() || c == '.' || c == '-' || c == '+' || c.to_ascii_uppercase() == 'E');
    if numeric {
        return Some(text.to_string());
    }
    if text.len() >= 2 && text.starts_with('\'') && text.ends_with('\'') {
        return Some(text.to_string());
    }
    None
}

fn column_line(protocol: Protocol, column: &ColumnMetadata, index: usize) -> (String, Option<String>) {
    let name = quote_identifier(protocol, &column.name);
    match protocol {
        Protocol::Sqlite => {
            let mut line = format!("{name} {}", sqlite_type_for(&column.data_type));
            let integer_key = index == 0
                && column.is_primary_key
                && sqlite_type_for(&column.data_type) == "INTEGER";
            if integer_key {
                line.push_str(" PRIMARY KEY AUTOINCREMENT");
                return (line, None);
            }
            if !column.nullable {
                line.push_str(" NOT NULL");
            }
            let default = sqlite_default(column.default_value.as_deref());
            if let Some(default) = &default {
                line.push_str(&format!(" DEFAULT {default}"));
            }
            let dropped = if default.is_none() && has_default(&column.default_value) {
                column.default_value.clone()
            } else {
                None
            };
            (line, dropped)
        }
        Protocol::Postgres => {
            let mut line = format!("{name} {}", column.data_type);
            let extra = column.extra.as_deref().unwrap_or_default();
            if extra.eq_ignore_ascii_case("identity") {
                line.push_str(" GENERATED ALWAYS AS IDENTITY");
            } else if !extra.eq_ignore_ascii_case("generated") {
                if let Some(default) = column.default_value.as_deref() {
                    line.push_str(&format!(" DEFAULT {default}"));
                }
            }
            if !column.nullable && !column.is_primary_key {
                line.push_str(" NOT NULL");
            }
            (line, None)
        }
        _ => {
            let mut line = format!("{name} {}", column.data_type);
            if !column.nullable {
                line.push_str(" NOT NULL");
            }
            if let Some(default) = column.default_value.as_deref() {
                line.push_str(&format!(" DEFAULT {default}"));
            }
            if column
                .extra
                .as_deref()
                .unwrap_or_default()
                .to_ascii_lowercase()
                .contains("auto_increment")
            {
                line.push_str(" AUTO_INCREMENT");
            }
            (line, None)
        }
    }
}

fn has_default(value: &Option<String>) -> bool {
    value.as_deref().map(str::trim).is_some_and(|text| !text.is_empty() && !text.eq_ignore_ascii_case("null"))
}

/// `CREATE TABLE` for a target that does not exist yet, plus the secondary index statements.
/// Foreign keys, triggers and partitions are never copied, which the caller reports as a note.
pub(crate) fn create_table_statements(
    protocol: Protocol,
    qualified_target: &str,
    source: &TableMetadata,
    include_indexes: bool,
) -> (Vec<String>, Vec<String>) {
    let mut notes = Vec::new();
    let quote = |name: &str| quote_identifier(protocol, name);
    let mut lines = source
        .columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            let (line, dropped) = column_line(protocol, column, index);
            if let Some(default) = dropped {
                notes.push(format!(
                    "列 {} 的默认值 {default} 不适用于目标库，已省略。",
                    column.name
                ));
            }
            format!("  {line}")
        })
        .collect::<Vec<_>>();
    let keys = source
        .columns
        .iter()
        .filter(|column| column.is_primary_key)
        .map(|column| column.name.clone())
        .collect::<Vec<_>>();
    let inlined_key = protocol == Protocol::Sqlite
        && keys.len() == 1
        && source.columns.first().is_some_and(|column| {
            column.is_primary_key && sqlite_type_for(&column.data_type) == "INTEGER"
        });
    if !keys.is_empty() && !inlined_key {
        lines.push(format!(
            "  PRIMARY KEY ({})",
            keys.iter().map(|name| quote(name)).collect::<Vec<_>>().join(", ")
        ));
    }
    let mut statements = vec![format!(
        "CREATE TABLE {qualified_target} (\n{}\n)",
        lines.join(",\n")
    )];
    if include_indexes {
        // MySQL keeps index names inside the table, so they copy over unchanged. PostgreSQL puts
        // them in the schema namespace and SQLite in the whole file, so re-using the source name
        // beside the source table collides; those engines get a target-specific name.
        let target_prefix = if protocol == Protocol::Sqlite {
            format!("{}_", qualified_target.trim_matches('"').replace("\"\"", "\""))
        } else {
            String::new()
        };
        let mut renamed = 0_usize;
        for index in &source.indexes {
            if index.columns.is_empty() || index.name.eq_ignore_ascii_case("primary") {
                continue;
            }
            if index.columns.iter().any(|column| column == "(expression)") {
                notes.push(format!("索引 {} 含表达式列，未自动创建。", index.name));
                continue;
            }
            let name = match protocol {
                Protocol::Mysql => format!(" {}", quote(&index.name)),
                Protocol::Postgres => {
                    renamed += 1;
                    String::new()
                }
                _ => {
                    renamed += 1;
                    format!(" {}", quote(&format!("{target_prefix}{}", index.name)))
                }
            };
            statements.push(format!(
                "CREATE {}INDEX{name} ON {qualified_target} ({})",
                if index.unique { "UNIQUE " } else { "" },
                index
                    .columns
                    .iter()
                    .map(|column| quote(column))
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        if renamed > 0 {
            let (scope, wording) = match protocol {
                Protocol::Postgres => ("在同一模式内", "改由数据库自动命名"),
                _ => ("在整个库内", "已按目标表名重新命名"),
            };
            notes.push(format!(
                "{renamed} 个二级索引{wording}：{} 的索引名{scope}必须唯一，沿用源索引名会和源表冲突。",
                protocol.as_str()
            ));
        }
    }
    if !source.foreign_keys.is_empty() {
        notes.push(format!(
            "源表有 {} 个外键，未自动创建：请按目标库的依赖顺序手工补充。",
            source.foreign_keys.len()
        ));
    }
    (statements, notes)
}

fn index_create_statements(protocol: Protocol, qualified_target: &str, indexes: &[crate::models::IndexMetadata]) -> Vec<String> {
    let target_prefix = if protocol == Protocol::Sqlite {
        format!("{}_", qualified_target.trim_matches('"'))
    } else {
        String::new()
    };
    let quote = |name: &str| quote_identifier(protocol, name);
    let mut statements = Vec::new();
    for index in indexes {
        if index.columns.is_empty() || index.name.eq_ignore_ascii_case("primary") {
            continue;
        }
        if index.columns.iter().any(|column| column == "(expression)") {
            continue;
        }
        let name = match protocol {
            Protocol::Mysql => format!(" {}", quote(&index.name)),
            Protocol::Postgres => String::new(),
            _ => format!(" {}", quote(&format!("{target_prefix}{}", index.name))),
        };
        statements.push(format!(
            "CREATE {}INDEX{name} ON {qualified_target} ({})",
            if index.unique { "UNIQUE " } else { "" },
            index.columns.iter().map(|column| quote(column)).collect::<Vec<_>>().join(", ")
        ));
    }
    statements
}

fn order_clause(protocol: Protocol, plan: &CopyPlan) -> String {
    let chosen = if plan.order_columns.is_empty() {
        plan.columns.first().into_iter().collect::<Vec<_>>()
    } else {
        plan.order_columns.iter().collect::<Vec<_>>()
    };
    if chosen.is_empty() {
        return String::new();
    }
    format!(
        " ORDER BY {}",
        chosen
            .into_iter()
            .map(|name| quote_identifier(protocol, name))
            .collect::<Vec<_>>()
            .join(", ")
    )
}

async fn table_detail(
    state: &AppState,
    side: &SideConnection,
    schema: &str,
    table: &str,
) -> AppResult<Option<TableMetadata>> {
    let snapshot = state
        .sql
        .metadata(
            &side.config,
            side.password.clone(),
            MetadataRequest {
                connection_id: side.config.id.clone(),
                schema: Some(schema.into()),
                table: Some(table.into()),
                include_tables: false,
                include_columns: true,
            },
        )
        .await?;
    Ok(snapshot
        .databases
        .into_iter()
        .flat_map(|database| database.tables)
        .find(|item| item.name == table))
}

/// DDL runs statement by statement so cancellation lands between statements and a failure names
/// the statement that caused it.
async fn run_statements(
    state: &AppState,
    side: &SideConnection,
    schema: &str,
    statements: &[String],
    operation_id: &str,
    job: &Arc<Job<TransferProgress>>,
) -> AppResult<()> {
    for (index, sql) in statements.iter().enumerate() {
        if job.is_cancelled() {
            return Err(AppError::cancelled());
        }
        let id = format!("{operation_id}-{index}");
        let _guard = job.track(ActiveOperation {
            connection_id: side.config.id.clone(),
            operation_id: id.clone(),
        });
        state
            .sql
            .execute(
                &side.config,
                side.password.clone(),
                SqlRequest {
                    connection_id: side.config.id.clone(),
                    sql: sql.clone(),
                    params: Vec::new(),
                    operation_id: id,
                    page_size: None,
                    offset: None,
                    schema: (!schema.is_empty()).then(|| schema.to_string()),
                },
            )
            .await?;
    }
    Ok(())
}

async fn create_target_table(
    state: &AppState,
    source: &SideConnection,
    target: &SideConnection,
    request: &TransferRequest,
    source_table: &TableMetadata,
    target_name: &str,
    index: usize,
    job: &Arc<Job<TransferProgress>>,
    notes: &mut Vec<String>,
) -> AppResult<TableMetadata> {
    let operation_id = format!("{}-t{index}", request.operation_id);
    let qualified = qualified_table(target.config.protocol, &request.target_schema, target_name);
    let statements = if target.config.protocol == Protocol::Mysql
        && source.config.protocol == Protocol::Mysql
    {
        // `CREATE TABLE ... LIKE` copies columns, indexes and constraints in the server's own
        // dialect; foreign keys and triggers stay behind, which is exactly what transfer promises.
        notes.push("MySQL 目标表由 CREATE TABLE … LIKE 创建：索引与约束会复制，外键与触发器不会。".into());
        vec![format!(
            "CREATE TABLE {qualified} LIKE {}",
            qualified_table(Protocol::Mysql, &source_table.schema, &source_table.name)
        )]
    } else {
        let (statements, generated_notes) = create_table_statements(
            target.config.protocol,
            &qualified,
            source_table,
            request.create_indexes && !request.disable_indexes,
        );
        notes.extend(generated_notes);
        statements
    };
    run_statements(
        state,
        target,
        &request.target_schema,
        &statements,
        &format!("{operation_id}-create"),
        job,
    )
    .await?;
    let created = table_detail(state, target, &request.target_schema, target_name)
        .await?
        .ok_or_else(|| AppError::new(ErrorClass::Internal, format!("目标表 {target_name} 创建后仍无法读取结构。")))?;
    notes.push(format!("已创建目标表 {target_name}。"));
    Ok(created)
}

/// Prepare one target table and report the action taken.
async fn prepare_target(
    state: &AppState,
    source: &SideConnection,
    target: &SideConnection,
    request: &TransferRequest,
    source_table: &TableMetadata,
    target_name: &str,
    index: usize,
    job: &Arc<Job<TransferProgress>>,
) -> AppResult<(TableMetadata, String, Vec<String>)> {
    let operation_id = format!("{}-t{index}", request.operation_id);
    let mut notes = Vec::new();
    let existing = table_detail(state, target, &request.target_schema, target_name).await?;
    let (metadata, action) = match existing {
        Some(existing) => match request.mode {
            TransferMode::Append => (existing, "append".to_string()),
            TransferMode::Truncate => {
                let statement = if target.config.protocol == Protocol::Sqlite {
                    format!("DELETE FROM {}", qualified_table(Protocol::Sqlite, &request.target_schema, target_name))
                } else {
                    format!("TRUNCATE TABLE {}", qualified_table(target.config.protocol, &request.target_schema, target_name))
                };
                run_statements(
                    state,
                    target,
                    &request.target_schema,
                    std::slice::from_ref(&statement),
                    &format!("{operation_id}-clear"),
                    job,
                )
                .await?;
                notes.push("目标表已清空后再写入。".into());
                (existing, "truncated".to_string())
            }
            TransferMode::Recreate => {
                let qualified = qualified_table(target.config.protocol, &request.target_schema, target_name);
                run_statements(
                    state,
                    target,
                    &request.target_schema,
                    &[format!("DROP TABLE IF EXISTS {qualified}")],
                    &format!("{operation_id}-drop"),
                    job,
                )
                .await?;
                let created = create_target_table(
                    state, source, target, request, source_table, target_name, index, job, &mut notes,
                )
                .await?;
                notes.push("目标表已删除并按源结构重建。".into());
                (created, "recreated".to_string())
            }
        },
        None => {
            if !request.create_missing {
                return Err(AppError::validation(format!(
                    "目标库中不存在表 {target_name}。勾选“创建缺失的表”，或先在目标建好同名表。"
                )));
            }
            let created = create_target_table(
                state, source, target, request, source_table, target_name, index, job, &mut notes,
            )
            .await?;
            (created, "created".to_string())
        }
    };
    let mut metadata = metadata;
    metadata.name = target_name.to_string();
    Ok((metadata, action, notes))
}

pub(crate) async fn copy_table(
    state: &AppState,
    job: &Arc<Job<TransferProgress>>,
    source: &SideConnection,
    target: &SideConnection,
    request: &TransferRequest,
    source_table: &TableMetadata,
    target_table: &TableMetadata,
    plan: &CopyPlan,
    index: usize,
) -> AppResult<(u64, u64)> {
    let select = format!(
        "SELECT {} FROM {}{}",
        plan.columns
            .iter()
            .map(|name| quote_identifier(source.config.protocol, name))
            .collect::<Vec<_>>()
            .join(", "),
        qualified_table(
            source.config.protocol,
            &source_table.schema,
            &source_table.name
        ),
        order_clause(source.config.protocol, plan)
    );
    let insert_columns = plan
        .columns
        .iter()
        .map(|name| quote_identifier(target.config.protocol, name))
        .collect::<Vec<_>>()
        .join(", ");
    let qualified_target =
        qualified_table(target.config.protocol, &request.target_schema, &target_table.name);
    let page_rows = transfer_batch_rows(request.batch_size, plan.columns.len());
    // Single-PK tables page with a keyset cursor instead of deep OFFSET scans, which get
    // quadratically slower on large tables. Other shapes keep the OFFSET fallback.
    let keyset_column = (plan.order_columns.len() == 1).then(|| plan.order_columns[0].clone());
    let keyset_index = keyset_column
        .as_ref()
        .and_then(|name| plan.columns.iter().position(|column| column == name));
    let mut last_key: Option<crate::models::ParamValue> = None;
    let mut offset = 0_u64;
    let mut read = 0_u64;
    let mut written = 0_u64;
    let mut page = 0_u64;
    // Commit every N pages via the driver's transactional batch, instead of one fsync per page.
    const COMMIT_PAGES: usize = 16;
    let mut pending: Vec<SqlRequest> = Vec::new();
    loop {
        if job.is_cancelled() {
            return Err(AppError::cancelled());
        }
        let read_id = format!("{}-t{index}-r{page}", request.operation_id);
        let (select_page, select_params) = match (&keyset_column, keyset_index, &last_key) {
            (Some(column), Some(_), Some(last)) => {
                let mut params = Vec::new();
                params.push(last.clone());
                (
                    format!(
                        "SELECT {} FROM {} WHERE {} > {}{}",
                        plan.columns
                            .iter()
                            .map(|name| quote_identifier(source.config.protocol, name))
                            .collect::<Vec<_>>()
                            .join(", "),
                        qualified_table(source.config.protocol, &source_table.schema, &source_table.name),
                        quote_identifier(source.config.protocol, column),
                        one_placeholder(source.config.protocol, 1),
                        order_clause(source.config.protocol, plan)
                    ),
                    params,
                )
            }
            _ => (select.clone(), Vec::new()),
        };
        // A keyset cursor must not also apply OFFSET, or rows would be skipped twice.
        let fetch_offset = if keyset_column.is_some() { None } else { Some(offset) };
        let rows = {
            let _guard = job.track(ActiveOperation {
                connection_id: source.config.id.clone(),
                operation_id: read_id.clone(),
            });
            state
                .sql
                .fetch(
                    &source.config,
                    source.password.clone(),
                    SqlRequest {
                        connection_id: source.config.id.clone(),
                        sql: select_page.clone(),
                        params: select_params,
                        operation_id: read_id,
                        page_size: Some(page_rows),
                        offset: fetch_offset,
                        schema: Some(source_table.schema.clone()),
                    },
                )
                .await?
                .rows
        };
        if rows.is_empty() {
            break;
        }
        read += rows.len() as u64;
        let values = value_placeholders(target.config.protocol, plan.columns.len(), rows.len())?;
        let mut params = Vec::with_capacity(rows.len() * plan.columns.len());
        for row in &rows {
            for value in row {
                params.push(json_to_param(value.clone())?);
            }
        }
        pending.push(SqlRequest {
            connection_id: target.config.id.clone(),
            sql: format!("INSERT INTO {qualified_target} ({insert_columns}) VALUES {values}"),
            params,
            operation_id: format!("{}-t{index}-w{page}", request.operation_id),
            page_size: None,
            offset: None,
            schema: (!request.target_schema.is_empty()).then(|| request.target_schema.clone()),
        });
        if let Some(index) = keyset_index {
            if let Some(last) = rows.last() {
                last_key = Some(json_to_param(last[index].clone())?);
            }
        }
        offset += rows.len() as u64;
        page += 1;
        if pending.len() >= COMMIT_PAGES || rows.len() < page_rows as usize {
            written += flush_transfer_batch(state, target, request, index, page, std::mem::take(&mut pending)).await?;
        }
        job.edit(|progress| {
            if let Some(entry) = progress.tables.get_mut(index) {
                entry.rows_read = read;
                entry.rows_written = written;
            }
        });
        if rows.len() < page_rows as usize {
            break;
        }
    }
    // Always flush the trailing partial batch, even when the last fetch returned no rows.
    if !pending.is_empty() {
        written += flush_transfer_batch(state, target, request, index, page, std::mem::take(&mut pending)).await?;
    }
    Ok((read, written))
}

async fn flush_transfer_batch(
    state: &AppState,
    target: &SideConnection,
    request: &TransferRequest,
    index: usize,
    page: u64,
    statements: Vec<SqlRequest>,
) -> AppResult<u64> {
    if statements.is_empty() {
        return Ok(0);
    }
    let write_id = format!("{}-t{index}-w{page}", request.operation_id);
    let results = state
        .sql
        .execute_batch(
            &target.config,
            target.password.clone(),
            crate::models::SqlBatchRequest {
                connection_id: target.config.id.clone(),
                statements,
                operation_id: write_id,
                wrap_in_transaction: true,
            },
        )
        .await?;
    Ok(results.iter().map(|entry| entry.rows_affected).sum())
}

fn one_placeholder(protocol: Protocol, index: usize) -> String {
    match protocol {
        Protocol::Postgres => format!("${index}"),
        _ => "?".to_string(),
    }
}

pub(crate) fn initial_progress(
    request: &TransferRequest,
    source: &ConnectionConfig,
    target: &ConnectionConfig,
) -> TransferProgress {
    TransferProgress {
        operation_id: request.operation_id.clone(),
        status: "running".into(),
        phase: "prepare".into(),
        source_name: source.name.clone(),
        target_name: target.name.clone(),
        source_protocol: source.protocol.as_str().into(),
        target_protocol: target.protocol.as_str().into(),
        rows_read: 0,
        rows_written: 0,
        total_rows: 0,
        tables_total: request.tables.len(),
        tables_done: 0,
        elapsed_ms: 0,
        tables: request
            .tables
            .iter()
            .map(|table| TransferTableProgress {
                source_schema: table.schema.clone(),
                source_table: table.table.clone(),
                target_table: table
                    .target_table
                    .clone()
                    .unwrap_or_else(|| table.table.clone()),
                status: "pending".into(),
                rows_read: 0,
                rows_written: 0,
                total_rows: None,
                action: None,
                duration_ms: 0,
                notes: Vec::new(),
                error: None,
            })
            .collect(),
        error: None,
    }
}

pub(crate) async fn run_transfer(
    state: Arc<AppState>,
    job: Arc<Job<TransferProgress>>,
    request: TransferRequest,
    source: SideConnection,
    target: SideConnection,
) {
    let started = Instant::now();
    if target.config.protocol == Protocol::Sqlite {
        // Best-effort bulk-load tuning for large SQLite targets; safe to ignore if the driver
        // rejects session pragmas.
        for pragma in ["PRAGMA journal_mode=WAL;", "PRAGMA synchronous=NORMAL;"] {
            let _ = state
                .sql
                .execute(
                    &target.config,
                    target.password.clone(),
                    SqlRequest {
                        connection_id: target.config.id.clone(),
                        sql: pragma.into(),
                        params: Vec::new(),
                        operation_id: format!("{}-wal", request.operation_id),
                        page_size: None,
                        offset: None,
                        schema: None,
                    },
                )
                .await;
        }
    }
    let mut failures = Vec::new();
    if request.disable_checks {
        match target.config.protocol {
            Protocol::Mysql => {
                for statement in ["SET UNIQUE_CHECKS=0", "SET FOREIGN_KEY_CHECKS=0"] {
                    let _ = run_statements(&state, &target, &request.target_schema, &[statement.into()], &format!("{}-checks-off", request.operation_id), &job).await;
                }
            }
            Protocol::Sqlite => {
                let _ = run_statements(&state, &target, &request.target_schema, &["PRAGMA foreign_keys=OFF".into()], &format!("{}-checks-off", request.operation_id), &job).await;
            }
            _ => {}
        }
    }
    for (index, table) in request.tables.iter().enumerate() {
        let target_name = table
            .target_table
            .clone()
            .unwrap_or_else(|| table.table.clone());
        job.edit(|progress| {
            progress.phase = "copy".into();
            if let Some(entry) = progress.tables.get_mut(index) {
                entry.status = "running".into();
            }
        });
        let table_started = Instant::now();
        let outcome = async {
            let source_table = table_detail(&state, &source, &table.schema, &table.table)
                .await?
                .ok_or_else(|| {
                    AppError::validation(format!("源表 {}.{} 已不存在。", table.schema, table.table))
                })?;
            if source_table.table_type.to_ascii_uppercase().contains("VIEW") {
                return Err(AppError::validation(format!(
                    "{} 是视图，数据传输只复制基表。",
                    source_table.name
                )));
            }
            let (target_table, action, mut notes) = prepare_target(
                &state, &source, &target, &request, &source_table, &target_name, index, &job,
            )
            .await?;
            let plan = build_copy_plan(&source_table, &target_table, request.mode);
            notes.extend(plan.notes.clone());
            if plan.columns.is_empty() {
                return Err(AppError::validation(format!(
                    "源表 {} 与目标表 {target_name} 没有同名列，无法传输数据。",
                    source_table.name
                )));
            }
            if plan.order_columns.is_empty() {
                notes.push("源表没有主键：分页按第一列排序，传输期间请勿写入该表。".into());
            }
            // One cheap COUNT per table drives percentage, rate and ETA on the progress UI.
            let count_sql = format!(
                "SELECT COUNT(*) AS n FROM {}",
                qualified_table(source.config.protocol, &source_table.schema, &source_table.name)
            );
            let total_rows = state
                .sql
                .fetch(
                    &source.config,
                    source.password.clone(),
                    SqlRequest {
                        connection_id: source.config.id.clone(),
                        sql: count_sql,
                        params: Vec::new(),
                        operation_id: format!("{}-t{index}-count", request.operation_id),
                        page_size: Some(1),
                        offset: Some(0),
                        schema: Some(source_table.schema.clone()),
                    },
                )
                .await?
                .rows
                .first()
                .and_then(|row| row.first().and_then(|value| value.as_u64()))
                .unwrap_or(0);
            job.edit(|progress| {
                progress.total_rows += total_rows;
                if let Some(entry) = progress.tables.get_mut(index) {
                    entry.total_rows = Some(total_rows);
                }
            });
            let dropped_indexes = if request.disable_indexes && !target_table.indexes.is_empty() {
                for idx in &target_table.indexes {
                    if idx.name.eq_ignore_ascii_case("primary") {
                        continue;
                    }
                    let qualified = qualified_table(target.config.protocol, &request.target_schema, &target_table.name);
                    let statement = match target.config.protocol {
                        Protocol::Mysql => format!("DROP INDEX {} ON {qualified}", quote_identifier(Protocol::Mysql, &idx.name)),
                        Protocol::Postgres => format!("DROP INDEX IF EXISTS {}.{}", quote_identifier(Protocol::Postgres, &request.target_schema), quote_identifier(Protocol::Postgres, &idx.name)),
                        _ => format!("DROP INDEX IF EXISTS {}", quote_identifier(Protocol::Sqlite, &idx.name)),
                    };
                    run_statements(&state, &target, &request.target_schema, &[statement], &format!("{}-t{index}-dropidx", request.operation_id), &job).await?;
                }
                target_table.indexes.clone()
            } else {
                Vec::new()
            };
            let (read, written) = copy_table(
                &state, &job, &source, &target, &request, &source_table, &target_table, &plan, index,
            )
            .await?;
            if request.disable_indexes {
                let recreate_from = if dropped_indexes.is_empty() { &source_table.indexes } else { &dropped_indexes };
                let qualified = qualified_table(target.config.protocol, &request.target_schema, &target_table.name);
                let creates = index_create_statements(target.config.protocol, &qualified, recreate_from);
                if !creates.is_empty() {
                    run_statements(&state, &target, &request.target_schema, &creates, &format!("{}-t{index}-recreate-idx", request.operation_id), &job).await?;
                }
            }
            Ok::<_, AppError>((read, written, action, notes))
        }
        .await;
        let duration_ms = table_started.elapsed().as_millis() as u64;
        match outcome {
            Ok((read, written, action, notes)) => {
                job.edit(|progress| {
                    progress.tables_done += 1;
                    progress.rows_read += read;
                    progress.rows_written += written;
                    progress.elapsed_ms = started.elapsed().as_millis() as u64;
                    if let Some(entry) = progress.tables.get_mut(index) {
                        entry.status = "done".into();
                        entry.action = Some(action);
                        entry.notes = notes;
                        entry.duration_ms = duration_ms;
                    }
                });
            }
            Err(error) => {
                let cancelled = error.class == ErrorClass::Cancelled || job.is_cancelled();
                job.edit(|progress| {
                    progress.elapsed_ms = started.elapsed().as_millis() as u64;
                    if let Some(entry) = progress.tables.get_mut(index) {
                        entry.status = if cancelled { "cancelled" } else { "failed" }.into();
                        entry.error = Some(error.message.clone());
                        entry.notes = notes_of(&error);
                        entry.duration_ms = duration_ms;
                    }
                    if cancelled {
                        progress.status = "cancelled".into();
                    } else {
                        progress.tables_done += 1;
                    }
                });
                if cancelled {
                    break;
                }
                failures.push(format!("{}.{}：{}", table.schema, table.table, error.message));
            }
        }
    }
    job.edit(|progress| {
        progress.elapsed_ms = started.elapsed().as_millis() as u64;
        progress.phase = "finished".into();
        if progress.status == "running" {
            progress.status = if failures.is_empty() { "done".into() } else { "failed".into() };
        }
        progress.error = if failures.is_empty() {
            None
        } else {
            Some(failures.join("；"))
        };
    });
    if request.disable_checks {
        match target.config.protocol {
            Protocol::Mysql => {
                for statement in ["SET UNIQUE_CHECKS=1", "SET FOREIGN_KEY_CHECKS=1"] {
                    let _ = run_statements(&state, &target, &request.target_schema, &[statement.into()], &format!("{}-checks-on", request.operation_id), &job).await;
                }
            }
            Protocol::Sqlite => {
                let _ = run_statements(&state, &target, &request.target_schema, &["PRAGMA foreign_keys=ON".into()], &format!("{}-checks-on", request.operation_id), &job).await;
            }
            _ => {}
        }
    }
    let snapshot = job.snapshot();
    let _ = state.store.append_audit(
        Some(&target.config.id),
        Some(&target.config.name),
        Some(target.config.protocol.as_str()),
        "data_transfer",
        &format!(
            "Transfer {} of {} tables: {} rows read, {} rows written",
            snapshot.tables.iter().filter(|entry| entry.status == "done").count(),
            snapshot.tables_total,
            snapshot.rows_read,
            snapshot.rows_written
        ),
        Some(snapshot.rows_written as i64),
        if snapshot.status == "failed" { "failure" } else { "success" },
        snapshot.error.as_deref(),
    );
    job.mark_finished();
}

fn notes_of(error: &AppError) -> Vec<String> {
    error
        .detail
        .clone()
        .into_iter()
        .collect::<Vec<_>>()
}

#[tauri::command]
pub async fn transfer_start(
    state: State<'_, Arc<AppState>>,
    request: TransferRequest,
) -> AppResult<TransferProgress> {
    let state = state_ref(&state);
    state.ensure_running()?;
    if request.operation_id.trim().is_empty() {
        return Err(AppError::validation("A transfer needs an operation id so it can be stopped."));
    }
    let source_config = state.connection(&request.source_connection_id)?;
    let target_config = state.connection(&request.target_connection_id)?;
    validate_transfer_request(&request, &source_config, &target_config)?;
    let source = SideConnection {
        password: state.connection_password(&source_config)?,
        config: source_config,
    };
    let target = SideConnection {
        password: state.connection_password(&target_config)?,
        config: target_config,
    };
    let progress = initial_progress(&request, &source.config, &target.config);
    let job = state.transfer_jobs.register(request.operation_id.clone(), progress.clone());
    tokio::spawn(run_transfer(
        Arc::clone(&state),
        job,
        request,
        source,
        target,
    ));
    Ok(progress)
}

#[tauri::command]
pub async fn transfer_status(
    state: State<'_, Arc<AppState>>,
    operation_id: String,
) -> AppResult<TransferProgress> {
    let state = state_ref(&state);
    state.ensure_running()?;
    let job = state.transfer_jobs.get(&operation_id)?;
    Ok(job.snapshot())
}

#[tauri::command]
pub async fn transfer_cancel(
    state: State<'_, Arc<AppState>>,
    operation_id: String,
) -> AppResult<TransferProgress> {
    let state = state_ref(&state);
    let job = state.transfer_jobs.get(&operation_id)?;
    for active in state.transfer_jobs.cancel(&operation_id)? {
        let _ = state.sql.cancel(&active.connection_id, &active.operation_id).await;
    }
    Ok(job.snapshot())
}

pub(crate) fn validate_transfer_request(
    request: &TransferRequest,
    source: &ConnectionConfig,
    target: &ConnectionConfig,
) -> AppResult<()> {
    if request.tables.is_empty() {
        return Err(AppError::validation("Select at least one table to transfer."));
    }
    if request.tables.len() > MAX_TRANSFER_TABLES {
        return Err(AppError::validation(format!(
            "一次传输最多 {MAX_TRANSFER_TABLES} 张表。"
        )));
    }
    if source.protocol.family() != DriverFamily::Sql
        || target.protocol.family() != DriverFamily::Sql
    {
        return Err(AppError::unsupported(
            "数据传输只在 MySQL、PostgreSQL 与 SQLite 连接之间进行。",
        ));
    }
    if request
        .tables
        .iter()
        .any(|table| table.schema.trim().is_empty() || table.table.trim().is_empty())
    {
        return Err(AppError::validation("每张表都需要指定源数据库和表名。"));
    }
    if target.protocol == Protocol::Sqlite
        && !request.target_schema.is_empty()
        && request.target_schema != "main"
    {
        return Err(AppError::validation(
            "SQLite 只有一个 main 模式，请选择 main 作为目标。",
        ));
    }
    if request.mode == TransferMode::Append
        && request.source_connection_id == request.target_connection_id
        && request.tables.iter().any(|table| {
            table.schema == request.target_schema
                && table.target_table.clone().unwrap_or_else(|| table.table.clone()) == table.table
        })
    {
        return Err(AppError::validation(
            "源表和目标表是同一张表，请把数据写入另一张表或另一个库。",
        ));
    }
    if !transfer_target_allowed(source.protocol, target.protocol) {
        return Err(AppError::unsupported(format!(
            "不支持 {} → {}：数据传输只做同引擎复制，以及任意 SQL 源写入 SQLite。",
            source.protocol.as_str(),
            target.protocol.as_str()
        )));
    }
    Ok(())
}

#[cfg(test)]
mod transfer_tests {
    use super::*;
    use crate::models::{ForeignKeyMetadata, IndexMetadata, TransferTableRef};

    fn column(name: &str, data_type: &str, nullable: bool, primary: bool, extra: Option<&str>) -> ColumnMetadata {
        ColumnMetadata {
            name: name.into(),
            data_type: data_type.into(),
            nullable,
            default_value: None,
            is_primary_key: primary,
            ordinal: 1,
            comment: None,
            extra: extra.map(str::to_string),
        }
    }

    fn table(name: &str, columns: Vec<ColumnMetadata>) -> TableMetadata {
        TableMetadata {
            schema: "shop".into(),
            name: name.into(),
            table_type: "BASE TABLE".into(),
            columns,
            indexes: Vec::new(),
            foreign_keys: Vec::new(),
        }
    }

    fn request(source: &str, target: &str, tables: Vec<TransferTableRef>) -> TransferRequest {
        TransferRequest {
            source_connection_id: source.into(),
            target_connection_id: target.into(),
            target_schema: "shop".into(),
            tables,
            mode: TransferMode::Append,
            create_missing: true,
            create_indexes: true,
            disable_indexes: false,
            disable_checks: false,
            batch_size: None,
            operation_id: "op-1".into(),
        }
    }

    fn config(id: &str, protocol: Protocol) -> ConnectionConfig {
        serde_json::from_value(serde_json::json!({ "id": id, "name": id, "protocol": protocol }))
            .expect("test connection config")
    }

    #[test]
    fn only_same_engine_and_any_source_into_sqlite_may_be_transferred() {
        for protocol in [Protocol::Mysql, Protocol::Postgres, Protocol::Sqlite] {
            assert!(transfer_target_allowed(protocol, protocol));
        }
        assert!(transfer_target_allowed(Protocol::Mysql, Protocol::Sqlite));
        assert!(transfer_target_allowed(Protocol::Postgres, Protocol::Sqlite));
        assert!(!transfer_target_allowed(Protocol::Mysql, Protocol::Postgres));
        assert!(!transfer_target_allowed(Protocol::Sqlite, Protocol::Mysql));
        assert!(!transfer_target_allowed(Protocol::Mysql, Protocol::Redis));
        assert!(!transfer_target_allowed(Protocol::Redis, Protocol::Mysql));
    }

    #[test]
    fn batch_rows_stay_inside_the_bind_parameter_budget() {
        assert_eq!(transfer_batch_rows(None, 4), 2_000);
        assert_eq!(transfer_batch_rows(Some(10_000), 4), 5_000);
        assert_eq!(transfer_batch_rows(Some(2_000), 40), 750);
        assert_eq!(transfer_batch_rows(Some(5_000), 1_000), 30);
        assert_eq!(transfer_batch_rows(Some(0), 1), 1);
    }

    #[test]
    fn server_types_map_onto_sqlite_storage_classes() {
        assert_eq!(sqlite_type_for("bigint(20) unsigned"), "INTEGER");
        assert_eq!(sqlite_type_for("int"), "INTEGER");
        assert_eq!(sqlite_type_for("SERIAL"), "INTEGER");
        assert_eq!(sqlite_type_for("boolean"), "INTEGER");
        assert_eq!(sqlite_type_for("decimal(10,2)"), "NUM");
        assert_eq!(sqlite_type_for("double precision"), "NUM");
        assert_eq!(sqlite_type_for("character varying(64)"), "TEXT");
        assert_eq!(sqlite_type_for("enum('a','b')"), "TEXT");
        assert_eq!(sqlite_type_for("jsonb"), "TEXT");
        assert_eq!(sqlite_type_for("timestamp without time zone"), "TEXT");
        assert_eq!(sqlite_type_for("bytea"), "BLOB");
        assert_eq!(sqlite_type_for("varbinary(16)"), "BLOB");
        assert_eq!(sqlite_type_for("geometry"), "BLOB");
        assert_eq!(sqlite_type_for("weird-type"), "TEXT");
    }

    #[test]
    fn only_literal_defaults_cross_into_sqlite() {
        assert_eq!(sqlite_default(Some("0")).as_deref(), Some("0"));
        assert_eq!(sqlite_default(Some("-1.5")).as_deref(), Some("-1.5"));
        assert_eq!(sqlite_default(Some("'draft'")).as_deref(), Some("'draft'"));
        assert_eq!(sqlite_default(Some("NULL")), None);
        assert_eq!(sqlite_default(Some("CURRENT_TIMESTAMP")), None);
        assert_eq!(sqlite_default(Some("nextval('orders_id_seq'::regclass)")), None);
        assert_eq!(sqlite_default(None), None);
    }

    #[test]
    fn copy_plan_uses_the_shared_columns_and_keeps_the_source_key_order() {
        let source = table(
            "orders",
            vec![
                column("id", "bigint", false, true, Some("auto_increment")),
                column("code", "varchar(64)", false, false, None),
                column("legacy_note", "text", true, false, None),
            ],
        );
        let target = table(
            "orders",
            vec![
                column("id", "int", false, true, None),
                column("code", "varchar(64)", false, false, None),
                column("tenant", "varchar(16)", true, false, None),
            ],
        );
        let plan = build_copy_plan(&source, &target, TransferMode::Append);
        assert_eq!(plan.columns, vec!["id".to_string(), "code".to_string()]);
        assert_eq!(plan.order_columns, vec!["id".to_string()]);
        assert!(plan.notes.iter().any(|note| note.contains("legacy_note")));
        assert!(plan.notes.iter().any(|note| note.contains("tenant")));

        let keyless = table("logs", vec![column("msg", "text", true, false, None)]);
        let plan = build_copy_plan(&keyless, &keyless, TransferMode::Append);
        assert!(plan.order_columns.is_empty());
        assert!(plan.notes.iter().any(|note| note.contains("没有主键")));
        let wide = (0..=MAX_TRANSFER_COLUMNS)
            .map(|index| column(&format!("c{index}"), "int", true, false, None))
            .collect::<Vec<_>>();
        let truncated = build_copy_plan(
            &table("wide", wide.clone()),
            &table("wide", wide),
            TransferMode::Append,
        );
        assert_eq!(truncated.columns.len(), MAX_TRANSFER_COLUMNS);
        assert!(truncated.notes.iter().any(|note| note.contains("前 1000 列")));
    }

    #[test]
    fn copy_plan_rejects_targets_without_a_shared_column() {
        let source = table("orders", vec![column("id", "int", false, true, None)]);
        let target = table("orders", vec![column("order_id", "int", false, true, None)]);
        let plan = build_copy_plan(&source, &target, TransferMode::Append);
        assert!(plan.columns.is_empty());
    }

    #[test]
    fn postgres_target_ddl_carries_types_keys_and_indexes() {
        let mut source = table(
            "orders",
            vec![
                column("id", "bigint", false, true, None),
                column("status", "character varying(32)", false, false, None),
                column("total", "numeric(10,2)", true, false, None),
            ],
        );
        source.columns[1].default_value = Some("'new'::character varying".into());
        source.columns[2].default_value = Some("0".into());
        source.indexes = vec![
            IndexMetadata { name: "PRIMARY".into(), columns: vec!["id".into()], unique: true },
            IndexMetadata { name: "Idx Status".into(), columns: vec!["status".into(), "total".into()], unique: false },
        ];
        source.foreign_keys = vec![ForeignKeyMetadata {
            name: "orders_customer".into(),
            columns: vec!["customer_id".into()],
            referenced_table: "customers".into(),
            referenced_columns: vec!["id".into()],
        }];
        let (statements, notes) = create_table_statements(
            Protocol::Postgres,
            "\"shop\".\"orders\"",
            &source,
            true,
        );
        let create = &statements[0];
        assert!(create.starts_with("CREATE TABLE \"shop\".\"orders\" ("));
        assert!(create.contains("\"id\" bigint"));
        assert!(create.contains("PRIMARY KEY (\"id\")"));
        assert!(create.contains("\"status\" character varying(32) DEFAULT 'new'::character varying NOT NULL"));
        assert_eq!(statements.len(), 2);
        assert_eq!(statements[1], "CREATE INDEX ON \"shop\".\"orders\" (\"status\", \"total\")");
        assert!(notes.iter().any(|note| note.contains("外键")));
        assert!(notes.iter().any(|note| note.contains("自动命名") && note.contains("模式")));
    }

    #[test]
    fn mysql_target_indexes_keep_their_names() {
        let mut source = table("orders", vec![column("id", "int", false, true, None), column("code", "varchar(32)", true, false, None)]);
        source.indexes = vec![
            IndexMetadata { name: "PRIMARY".into(), columns: vec!["id".into()], unique: true },
            IndexMetadata { name: "idx_orders_code".into(), columns: vec!["code".into()], unique: true },
        ];
        let (statements, notes) = create_table_statements(Protocol::Mysql, "`shop`.`orders`", &source, true);
        assert_eq!(statements.len(), 2);
        assert_eq!(statements[1], "CREATE UNIQUE INDEX `idx_orders_code` ON `shop`.`orders` (`code`)");
        assert!(notes.iter().all(|note| !note.contains("自动命名")), "{notes:?}");
    }

    #[test]
    fn sqlite_target_ddl_maps_types_and_inlines_the_rowid_key() {
        let mut source = table(
            "orders",
            vec![
                column("id", "bigint(20) unsigned", false, true, Some("auto_increment")),
                column("payload", "json", true, false, None),
                column("note", "text", true, false, None),
            ],
        );
        source.columns[1].default_value = Some("json()".into());
        let (statements, notes) = create_table_statements(Protocol::Sqlite, "\"orders\"", &source, false);
        let create = &statements[0];
        assert!(create.contains("\"id\" INTEGER PRIMARY KEY AUTOINCREMENT"));
        assert!(create.contains("\"payload\" TEXT"));
        assert!(!create.contains("PRIMARY KEY ("));
        assert!(notes.iter().any(|note| note.contains("payload")));
        assert_eq!(statements.len(), 1);

        let text_key = table("codes", vec![column("code", "char(8)", false, true, None)]);
        let (statements, _) = create_table_statements(Protocol::Sqlite, "\"codes\"", &text_key, false);
        assert!(statements[0].contains("\"code\" TEXT NOT NULL"));
        assert!(statements[0].contains("PRIMARY KEY (\"code\")"));
    }

    #[test]
    fn expression_indexes_are_reported_instead_of_emitted() {
        let mut source = table("logs", vec![column("id", "int", false, true, None)]);
        source.indexes = vec![IndexMetadata {
            name: "idx_hash".into(),
            columns: vec!["(expression)".into()],
            unique: false,
        }];
        let (statements, notes) = create_table_statements(Protocol::Postgres, "\"t\"", &source, true);
        assert_eq!(statements.len(), 1);
        assert!(notes.iter().any(|note| note.contains("idx_hash")));
    }

    #[test]
    fn ordering_is_deterministic_and_dialect_aware() {
        let plan = CopyPlan {
            columns: vec!["id".into(), "code".into()],
            order_columns: vec!["id".into()],
            notes: Vec::new(),
        };
        assert_eq!(order_clause(Protocol::Mysql, &plan), " ORDER BY `id`");
        let no_key = CopyPlan {
            columns: vec!["code".into()],
            order_columns: Vec::new(),
            notes: Vec::new(),
        };
        assert_eq!(order_clause(Protocol::Sqlite, &no_key), " ORDER BY \"code\"");
        let empty = CopyPlan { columns: Vec::new(), order_columns: Vec::new(), notes: Vec::new() };
        assert_eq!(order_clause(Protocol::Postgres, &empty), "");
    }

    #[test]
    fn qualified_tables_follow_each_dialect() {
        assert_eq!(qualified_table(Protocol::Mysql, "shop", "orders"), "`shop`.`orders`");
        assert_eq!(qualified_table(Protocol::Postgres, "pub lic", "or\"ders"), "\"pub lic\".\"or\"\"ders\"");
        assert_eq!(qualified_table(Protocol::Sqlite, "main", "orders"), "\"orders\"");
    }

    #[test]
    fn requests_are_validated_before_any_connection_is_opened() {
        let mysql = config("src", Protocol::Mysql);
        let other = config("tgt", Protocol::Mysql);
        let one_table = vec![TransferTableRef { schema: "shop".into(), table: "orders".into(), target_table: None }];
        assert!(validate_transfer_request(&request("src", "tgt", one_table.clone()), &mysql, &other).is_ok());
        assert!(validate_transfer_request(&request("src", "tgt", Vec::new()), &mysql, &other)
            .unwrap_err()
            .message
            .contains("at least one"));
        let same_table = request("src", "src", one_table.clone());
        assert!(validate_transfer_request(&same_table, &mysql, &mysql)
            .unwrap_err()
            .message
            .contains("同一张表"));
        let renamed = TransferRequest {
            tables: vec![TransferTableRef { schema: "shop".into(), table: "orders".into(), target_table: Some("orders_copy".into()) }],
            ..same_table
        };
        assert!(validate_transfer_request(&renamed, &mysql, &mysql).is_ok());
        let postgres = config("tgt", Protocol::Postgres);
        assert!(validate_transfer_request(&request("src", "tgt", one_table.clone()), &mysql, &postgres)
            .unwrap_err()
            .message
            .contains("不支持"));
        let sqlite = config("tgt", Protocol::Sqlite);
        let into_sqlite = TransferRequest {
            target_schema: "main".into(),
            ..request("src", "tgt", one_table.clone())
        };
        assert!(validate_transfer_request(&into_sqlite, &mysql, &sqlite).is_ok());
        let wrong_schema = TransferRequest {
            target_schema: "main2".into(),
            ..request("src", "tgt", one_table.clone())
        };
        assert!(validate_transfer_request(&wrong_schema, &mysql, &sqlite)
            .unwrap_err()
            .message
            .contains("main"));
        let blank = request("src", "tgt", vec![TransferTableRef { schema: " ".into(), table: "orders".into(), target_table: None }]);
        assert!(validate_transfer_request(&blank, &mysql, &other).is_err());
        let redis = config("tgt", Protocol::Redis);
        assert!(validate_transfer_request(&request("src", "tgt", one_table), &mysql, &redis).is_err());
    }

    #[test]
    fn progress_starts_with_one_entry_per_selected_table() {
        let request = request(
            "src",
            "tgt",
            vec![
                TransferTableRef { schema: "shop".into(), table: "orders".into(), target_table: None },
                TransferTableRef { schema: "shop".into(), table: "users".into(), target_table: Some("accounts".into()) },
            ],
        );
        let progress = initial_progress(
            &request,
            &config("src", Protocol::Mysql),
            &config("tgt", Protocol::Sqlite),
        );
        assert_eq!(progress.tables_total, 2);
        assert_eq!(progress.tables[1].target_table, "accounts");
        assert!(progress.tables.iter().all(|entry| entry.status == "pending"));
        assert_eq!(progress.status, "running");
        assert_eq!(progress.target_protocol, "sqlite");
    }

    /// Runs one transfer through the same plumbing `transfer_start` uses and returns the final
    /// snapshot; `cancel_first` stops it before the first page is read.
    async fn live_transfer(
        state: &Arc<AppState>,
        source: &SideConnection,
        target: &SideConnection,
        source_schema: &str,
        target_schema: &str,
        tables: &[(&str, Option<&str>)],
        mode: TransferMode,
        create_missing: bool,
        create_indexes: bool,
        cancel_first: bool,
    ) -> TransferProgress {
        let request = TransferRequest {
            source_connection_id: source.config.id.clone(),
            target_connection_id: target.config.id.clone(),
            target_schema: target_schema.into(),
            tables: tables
                .iter()
                .map(|(table, alias)| TransferTableRef {
                    schema: source_schema.into(),
                    table: (*table).into(),
                    target_table: alias.map(str::to_string),
                })
                .collect(),
            mode,
            create_missing,
            create_indexes,
            batch_size: Some(200),
            disable_indexes: false,
            disable_checks: false,
            operation_id: format!("live-transfer-{}", uuid::Uuid::new_v4()),
        };
        let progress = initial_progress(&request, &source.config, &target.config);
        let job = state.transfer_jobs.register(request.operation_id.clone(), progress.clone());
        if cancel_first {
            state.transfer_jobs.cancel(&request.operation_id).unwrap();
        }
        let source = SideConnection {
            config: source.config.clone(),
            password: source.password.clone(),
        };
        let target = SideConnection {
            config: target.config.clone(),
            password: target.password.clone(),
        };
        run_transfer(Arc::clone(state), job.clone(), request, source, target).await;
        job.snapshot()
    }

    /// End-to-end checks against the throw-away servers described at the top of `live_tests`:
    /// same-engine rebuild/truncate/append copies, an export into a temporary SQLite file and a
    /// cooperative stop. Each engine only runs when its `LUXBASE_LIVE_*` variable is set.
    #[tokio::test(flavor = "multi_thread")]
    async fn live_transfer_rebuilds_appends_and_exports_to_sqlite() {
        use crate::live_tests::{config, live_count, live_execute, live_number, live_state, target, Target};
        use serde_json::json;

        const ROWS: i64 = 900;
        let (state, dir) = live_state("transfer");
        let engines = [
            ("mysql", "LUXBASE_LIVE_MYSQL", Protocol::Mysql, "shop"),
            ("postgres", "LUXBASE_LIVE_POSTGRES", Protocol::Postgres, "shop"),
        ];
        let expected = engines.iter().filter(|(_, variable, ..)| std::env::var(variable).is_ok()).count();
        let mut ran = 0_usize;
        let sqlite = Target {
            host: String::new(),
            port: 0,
            user: String::new(),
            password: None,
            database: String::new(),
        };

        for (tag, variable, protocol, schema) in engines {
            let Some(server) = target(variable) else { continue };
            ran += 1;
            let password = server.password.clone();
            let side = |id: &str| SideConnection {
                config: config(id, tag, &server, json!({})),
                password: password.clone(),
            };
            let source = side(&format!("live-xfer-src-{tag}"));
            let sink = side(&format!("live-xfer-dst-{tag}"));
            let name = |table: &str| qualified_table(protocol, schema, table);
            let (key_type, note_type) = match protocol {
                Protocol::Mysql => ("BIGINT", "VARCHAR(8)"),
                _ => ("bigint", "varchar(8)"),
            };

            for table in ["luxbase_xfer_src", "luxbase_xfer_two"] {
                live_execute(&state, &sink.config, &password, &format!("DROP TABLE IF EXISTS {}", name(table)))
                    .await;
            }
            // The target starts as a differently shaped table with a stale row, so 删除重建 has
            // something to drop and rebuild instead of merely creating a new table.
            live_execute(&state, &sink.config, &password, &format!("DROP TABLE IF EXISTS {}", name("luxbase_xfer_dst")))
                .await;
            live_execute(
                &state,
                &sink.config,
                &password,
                &format!(
                    "CREATE TABLE {} (id {key_type} NOT NULL PRIMARY KEY, stale {note_type} NOT NULL)",
                    name("luxbase_xfer_dst")
                ),
            )
            .await;
            live_execute(
                &state,
                &sink.config,
                &password,
                &format!("INSERT INTO {} (id, stale) VALUES (0, 'stale')", name("luxbase_xfer_dst")),
            )
            .await;
            let create = match protocol {
                Protocol::Mysql => format!(
                    "CREATE TABLE {} (id BIGINT NOT NULL PRIMARY KEY, code VARCHAR(32) NOT NULL, \
                     amount DECIMAL(12,2) NULL, note VARCHAR(64) NULL, KEY idx_xfer_src_code (code))",
                    name("luxbase_xfer_src")
                ),
                _ => format!(
                    "CREATE TABLE {} (id bigint PRIMARY KEY, code varchar(32) NOT NULL, \
                     amount numeric(12,2), note varchar(64))",
                    name("luxbase_xfer_src")
                ),
            };
            live_execute(&state, &source.config, &password, &create).await;
            if protocol == Protocol::Postgres {
                live_execute(
                    &state,
                    &source.config,
                    &password,
                    &format!("CREATE INDEX luxbase_xfer_src_code ON {} (code)", name("luxbase_xfer_src")),
                )
                .await;
            }
            for start in (1..=ROWS).step_by(300) {
                let values = (start..=(start + 299).min(ROWS))
                    .map(|row| {
                        format!(
                            "({}, 'C{:05}', {:.2}, {})",
                            row,
                            row,
                            row as f64 * 1.5,
                            if row % 7 == 0 { "NULL".to_string() } else { format!("'备注 {row}'") }
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                live_execute(
                    &state,
                    &source.config,
                    &password,
                    &format!("INSERT INTO {} (id, code, amount, note) VALUES {values}", name("luxbase_xfer_src")),
                )
                .await;
            }
            assert_eq!(live_count(&state, &source.config, &password, &name("luxbase_xfer_src")).await, ROWS);

            // 1. 删除重建: the target is dropped, rebuilt from the source structure and filled.
            let rebuilt = live_transfer(
                &state,
                &source,
                &sink,
                schema,
                schema,
                &[("luxbase_xfer_src", Some("luxbase_xfer_dst"))],
                TransferMode::Recreate,
                true,
                true,
                false,
            )
            .await;
            assert_eq!(rebuilt.status, "done", "{rebuilt:?}");
            assert!(rebuilt.error.is_none(), "{:?}", rebuilt.error);
            assert_eq!(rebuilt.phase, "finished");
            assert_eq!((rebuilt.rows_read, rebuilt.rows_written), (ROWS as u64, ROWS as u64));
            assert_eq!(rebuilt.tables_done, rebuilt.tables_total);
            assert_eq!(rebuilt.tables[0].action.as_deref(), Some("recreated"));
            assert!(rebuilt.tables[0].notes.iter().any(|note| note.contains("重建")));
            assert_eq!(live_count(&state, &sink.config, &password, &name("luxbase_xfer_dst")).await, ROWS);
            let source_sum =
                live_number(&state, &source.config, &password, &format!("SELECT SUM(id) FROM {}", name("luxbase_xfer_src"))).await;
            let target_sum =
                live_number(&state, &sink.config, &password, &format!("SELECT SUM(id) FROM {}", name("luxbase_xfer_dst"))).await;
            assert_eq!(source_sum, target_sum);
            let created = table_detail(&state, &sink, schema, "luxbase_xfer_dst")
                .await
                .unwrap()
                .expect("rebuilt target table");
            assert_eq!(created.columns.len(), 4, "{created:?}");
            assert!(
                created.columns.iter().all(|column| column.name != "stale"),
                "rebuilding must drop the old shape: {:?}",
                created.columns
            );
            assert!(created.indexes.iter().any(|index| index.columns == vec!["id".to_string()]), "{:?}", created.indexes);
            assert!(
                created.indexes.iter().any(|index| index.columns == vec!["code".to_string()]),
                "create_indexes must carry secondary indexes: {:?}",
                created.indexes
            );

            // 2. 清空后写入: rows are replaced, never doubled.
            let truncated = live_transfer(
                &state,
                &source,
                &sink,
                schema,
                schema,
                &[("luxbase_xfer_src", Some("luxbase_xfer_dst"))],
                TransferMode::Truncate,
                true,
                true,
                false,
            )
            .await;
            assert_eq!(truncated.status, "done", "{truncated:?}");
            assert_eq!(truncated.tables[0].action.as_deref(), Some("truncated"));
            assert_eq!(live_count(&state, &sink.config, &password, &name("luxbase_xfer_dst")).await, ROWS);

            // 3. 追加写入 onto a table that does not exist yet: it is created from the source.
            let appended = live_transfer(
                &state,
                &source,
                &sink,
                schema,
                schema,
                &[("luxbase_xfer_src", Some("luxbase_xfer_two"))],
                TransferMode::Append,
                true,
                false,
                false,
            )
            .await;
            assert_eq!(appended.status, "done", "{appended:?}");
            assert_eq!(appended.tables[0].action.as_deref(), Some("created"));
            assert_eq!(live_count(&state, &sink.config, &password, &name("luxbase_xfer_two")).await, ROWS);

            // 4. Any SQL source into a SQLite file, including NULLs and multi-byte text.
            let path = dir.join(format!("luxbase-xfer-{tag}.sqlite3"));
            rusqlite::Connection::open(&path).expect("create the target SQLite file");
            let sqlite_side = SideConnection {
                config: config(
                    &format!("live-xfer-sqlite-{tag}"),
                    "sqlite",
                    &sqlite,
                    json!({ "filePath": path.to_string_lossy() }),
                ),
                password: None,
            };
            let exported = live_transfer(
                &state,
                &source,
                &sqlite_side,
                schema,
                "main",
                &[("luxbase_xfer_src", Some("luxbase_xfer_dst"))],
                TransferMode::Recreate,
                true,
                true,
                false,
            )
            .await;
            assert_eq!(exported.status, "done", "{exported:?}");
            assert_eq!(exported.target_protocol, "sqlite");
            let quoted = name("luxbase_xfer_src").replace('"', "\"");
            let source_nulls = live_number(
                &state,
                &source.config,
                &password,
                &format!("SELECT COUNT(*) FROM {} WHERE note IS NULL", name("luxbase_xfer_src")),
            )
            .await;
            assert!(source_nulls > 0.0, "{quoted}: the seed needs NULL values");
            assert_eq!(
                live_count(&state, &sqlite_side.config, &None, "\"luxbase_xfer_dst\"").await,
                ROWS
            );
            assert_eq!(
                live_number(
                    &state,
                    &sqlite_side.config,
                    &None,
                    "SELECT COUNT(*) FROM \"luxbase_xfer_dst\" WHERE note IS NULL"
                )
                .await,
                source_nulls,
                "{quoted}: NULLs must survive the SQLite export"
            );
            assert_eq!(
                live_number(&state, &sqlite_side.config, &None, "SELECT SUM(id) FROM \"luxbase_xfer_dst\"").await,
                target_sum
            );
            assert_eq!(
                live_number(
                    &state,
                    &sqlite_side.config,
                    &None,
                    "SELECT COUNT(*) FROM \"luxbase_xfer_dst\" WHERE note = '备注 43'"
                )
                .await,
                1.0,
                "{quoted}: multi-byte text must survive the SQLite export"
            );

            // 5. Stopping mid-run leaves the plan reported as cancelled, without an error.
            let stopped = live_transfer(
                &state,
                &source,
                &sink,
                schema,
                schema,
                &[("luxbase_xfer_src", Some("luxbase_xfer_dst"))],
                TransferMode::Append,
                false,
                false,
                true,
            )
            .await;
            assert_eq!(stopped.status, "cancelled", "{stopped:?}");
            assert_eq!(stopped.tables[0].status, "cancelled");
            assert!(stopped.error.is_none(), "{:?}", stopped.error);

            for table in ["luxbase_xfer_src", "luxbase_xfer_dst", "luxbase_xfer_two"] {
                live_execute(&state, &sink.config, &password, &format!("DROP TABLE IF EXISTS {}", name(table))).await;
            }
        }

        // Cross-engine copies are refused before a connection is opened, with a pointer to the
        // feature that does exist.
        let mysql = target("LUXBASE_LIVE_MYSQL");
        let postgres = target("LUXBASE_LIVE_POSTGRES");
        if let (Some(mysql), Some(postgres)) = (mysql, postgres) {
            let source = config("live-xfer-mysql", "mysql", &mysql, json!({}));
            let sink = config("live-xfer-pg", "postgres", &postgres, json!({}));
            let request = request_of("live-xfer-mysql", "live-xfer-pg", "shop", "luxbase_xfer_src");
            let error = validate_transfer_request(&request, &source, &sink).unwrap_err();
            assert!(error.message.contains("不支持"), "{error:?}");
            assert!(error.message.contains("SQLite"), "{error:?}");
        }
        state.sql.close_all().await;
        let _ = std::fs::remove_dir_all(&dir);
        assert_eq!(ran, expected, "每一台已配置的引擎都要跑完传输检查");
    }

    fn request_of(source: &str, target: &str, schema: &str, table: &str) -> TransferRequest {
        TransferRequest {
            source_connection_id: source.into(),
            target_connection_id: target.into(),
            target_schema: schema.into(),
            tables: vec![TransferTableRef {
                schema: schema.into(),
                table: table.into(),
                target_table: None,
            }],
            mode: TransferMode::Append,
            create_missing: true,
            create_indexes: false,
            disable_indexes: false,
            disable_checks: false,
            batch_size: None,
            operation_id: "live-cross-engine".into(),
        }
    }
}

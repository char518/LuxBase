use crate::app_state::AppState;
use crate::commands::{json_to_param, qualified_table, state_ref};
use crate::drivers::sql::MAX_BIND_PARAMETERS;
use crate::error::{AppError, AppResult};
use crate::jobs::{ActiveOperation, Job};
use crate::models::{
    DriverFamily, ErrorClass, MetadataRequest, Protocol, SqlBatchRequest, SqlRequest,
    SyncApplyRequest, SyncApplyResult, SyncOperation, SyncPlan, SyncPlanRequest, SyncProgress,
    SyncSide, TableMetadata,
};
use crate::transfer::SideConnection;
use base64::Engine;
use parking_lot::Mutex;
use serde_json::Value;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tauri::State;

/// More differences than this belong in Data Transfer, not in a reviewed row diff.
pub(crate) const MAX_PLAN_OPERATIONS: usize = 20_000;
const MAX_SYNC_PAGE_ROWS: u32 = 500;
const MAX_PLAN_KEYS: usize = 6;
const PLAN_TTL: Duration = Duration::from_secs(30 * 60);
const MAX_CACHED_PLANS: usize = 8;

#[derive(Debug, Clone)]
pub(crate) struct PlannedStatement {
    pub(crate) id: usize,
    pub(crate) kind: &'static str,
    pub(crate) sql: String,
    pub(crate) params: Vec<crate::models::ParamValue>,
}

pub(crate) struct CachedPlan {
    pub(crate) plan: SyncPlan,
    pub(crate) statements: HashMap<usize, PlannedStatement>,
    pub(crate) created: Instant,
}

/// The diff is cached by id so the review payload stays small and execution keeps its bound
/// parameters in Rust instead of rebuilding them from interpolated SQL in the webview.
#[derive(Default)]
pub(crate) struct PlanCache {
    plans: Mutex<HashMap<String, Arc<CachedPlan>>>,
    order: Mutex<VecDeque<String>>,
}

impl PlanCache {
    pub(crate) fn insert(&self, plan: CachedPlan) {
        let id = plan.plan.plan_id.clone();
        self.plans.lock().insert(id.clone(), Arc::new(plan));
        let mut order = self.order.lock();
        order.retain(|key| *key != id);
        order.push_back(id);
        while order.len() > MAX_CACHED_PLANS {
            if let Some(oldest) = order.pop_front() {
                self.plans.lock().remove(&oldest);
            }
        }
    }

    pub(crate) fn get(&self, id: &str) -> AppResult<Arc<CachedPlan>> {
        let entry = self
            .plans
            .lock()
            .get(id)
            .cloned()
            .ok_or_else(|| AppError::validation("这个差异计划不存在或已执行，请重新比较。"))?;
        if entry.created.elapsed() > PLAN_TTL {
            self.remove(id);
            return Err(AppError::validation("差异计划已超过 30 分钟，请重新比较后再执行。"));
        }
        Ok(entry)
    }

    pub(crate) fn remove(&self, id: &str) {
        self.plans.lock().remove(id);
        let mut order = self.order.lock();
        order.retain(|key| key != id);
    }

    pub(crate) fn len(&self) -> usize {
        self.plans.lock().len()
    }
}

/// Everything a safe row diff needs, resolved from the two table structures before any scan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SyncSpec {
    pub(crate) keys: Vec<String>,
    /// Non-key columns compared and written, in source order.
    pub(crate) compared: Vec<String>,
    /// Columns existing on only one side; they cannot take part in the diff.
    pub(crate) skipped: Vec<String>,
    pub(crate) notes: Vec<String>,
}

impl SyncSpec {
    /// The column list both sides are read with: keys first, then the compared columns.
    pub(crate) fn columns(&self) -> Vec<String> {
        self.keys.iter().chain(self.compared.iter()).cloned().collect()
    }
}

pub(crate) fn validate_sync_tables(
    source_protocol: Protocol,
    target_protocol: Protocol,
    source: &TableMetadata,
    target: &TableMetadata,
) -> AppResult<SyncSpec> {
    if source_protocol != target_protocol {
        return Err(AppError::unsupported(format!(
            "数据同步只做同引擎比对：当前是 {} → {}。跨引擎搬运请使用数据传输。",
            source_protocol.as_str(),
            target_protocol.as_str()
        )));
    }
    for (side, table) in [("源", &source), ("目标", &target)] {
        if table.table_type.to_ascii_uppercase().contains("VIEW") {
            return Err(AppError::validation(format!("{side} {} 是视图，无法同步行数据。", table.name)));
        }
    }
    let keys = source
        .columns
        .iter()
        .filter(|column| column.is_primary_key)
        .map(|column| column.name.clone())
        .collect::<Vec<_>>();
    if keys.is_empty() {
        return Err(AppError::validation(
            "源表没有主键，无法逐行比对。请先定义主键，或改用数据传输做整表替换。",
        ));
    }
    if keys.len() > MAX_PLAN_KEYS {
        return Err(AppError::validation(format!(
            "主键列超过 {MAX_PLAN_KEYS} 个的表暂不支持逐行比对。"
        )));
    }
    let target_keys = target
        .columns
        .iter()
        .filter(|column| column.is_primary_key)
        .map(|column| column.name.clone())
        .collect::<Vec<_>>();
    if target_keys != keys {
        return Err(AppError::validation(format!(
            "两侧主键定义不同（源 {}/目标 {}），请先用结构比较统一后再同步数据。",
            keys.join("+"),
            if target_keys.is_empty() { "无主键".to_string() } else { target_keys.join("+") }
        )));
    }
    let in_source = source
        .columns
        .iter()
        .filter(|column| !column.is_primary_key)
        .map(|column| column.name.clone())
        .collect::<Vec<_>>();
    let in_target = target
        .columns
        .iter()
        .filter(|column| !column.is_primary_key)
        .map(|column| column.name.clone())
        .collect::<Vec<_>>();
    let compared = in_source
        .iter()
        .filter(|name| in_target.contains(name))
        .cloned()
        .collect::<Vec<_>>();
    let skipped = in_source
        .iter()
        .chain(in_target.iter())
        .filter(|name| !compared.contains(name))
        .cloned()
        .collect::<Vec<_>>();
    let mut notes = Vec::new();
    if !skipped.is_empty() {
        notes.push(format!(
            "列 {} 只存在于一侧，不参与比对；结构差异请先用结构比较处理。",
            skipped.join("、")
        ));
    }
    if compared.is_empty() {
        return Err(AppError::validation("两侧除主键外没有同名列，无法比较行数据。"));
    }
    Ok(SyncSpec { keys, compared, skipped, notes })
}

fn quote(protocol: Protocol, name: &str) -> String {
    match protocol {
        Protocol::Mysql => format!("`{}`", name.replace('`', "``")),
        _ => format!("\"{}\"", name.replace('"', "\"\"")),
    }
}

fn placeholder(protocol: Protocol, index: usize) -> String {
    match protocol {
        Protocol::Postgres => format!("${index}"),
        _ => "?".into(),
    }
}

/// Rows per scan page, kept small enough that the matching key lookup fits the bind budget.
pub(crate) fn sync_page_rows(key_count: usize) -> u32 {
    let by_parameters = (MAX_BIND_PARAMETERS / key_count.max(1)).max(1) as u32;
    MAX_SYNC_PAGE_ROWS.min(by_parameters)
}

/// `SELECT` fetching the rows matching `groups` key tuples, one bind parameter per key column.
pub(crate) fn matching_select(
    protocol: Protocol,
    qualified: &str,
    columns: &[String],
    keys: &[String],
    groups: usize,
) -> AppResult<String> {
    if groups == 0 || keys.is_empty() {
        return Err(AppError::validation("没有可用于比对的主键值。"));
    }
    if keys.len().saturating_mul(groups) > MAX_BIND_PARAMETERS {
        return Err(AppError::validation(format!(
            "一次比对 {} 行会产生 {} 个绑定参数，超过上限 {MAX_BIND_PARAMETERS}。",
            groups,
            keys.len() * groups
        )));
    }
    let predicates = (0..groups)
        .map(|group| {
            format!(
                "({})",
                keys.iter()
                    .enumerate()
                    .map(|(offset, key)| format!(
                        "{} = {}",
                        quote(protocol, key),
                        placeholder(protocol, group * keys.len() + offset + 1)
                    ))
                    .collect::<Vec<_>>()
                    .join(" AND ")
            )
        })
        .collect::<Vec<_>>()
        .join(" OR ");
    Ok(format!(
        "SELECT {} FROM {} WHERE {}",
        columns.iter().map(|name| quote(protocol, name)).collect::<Vec<_>>().join(", "),
        qualified,
        predicates
    ))
}

pub(crate) fn scan_select(
    protocol: Protocol,
    qualified: &str,
    columns: &[String],
    keys: &[String],
) -> String {
    format!(
        "SELECT {} FROM {} ORDER BY {}",
        columns.iter().map(|name| quote(protocol, name)).collect::<Vec<_>>().join(", "),
        qualified,
        keys.iter().map(|name| quote(protocol, name)).collect::<Vec<_>>().join(", ")
    )
}

pub(crate) fn position_of(columns: &[String], name: &str) -> Option<usize> {
    columns.iter().position(|column| column == name)
}

fn value_token(value: &Value) -> String {
    match value {
        Value::Null => "null".into(),
        Value::Number(number) => format!("n{number}"),
        Value::String(text) => format!("s{}:{text}", text.len()),
        Value::Bool(flag) => format!("b{flag}"),
        Value::Object(map) => match map.get("bytesBase64").and_then(Value::as_str) {
            Some(encoded) => format!("x{encoded}"),
            None => format!("j{map:?}"),
        },
        other => format!("j{other}"),
    }
}

/// Stable identity of the leading `key_count` cells; `None` when a key cell is NULL.
pub(crate) fn key_signature(row: &[Value], key_count: usize) -> Option<String> {
    if row.len() < key_count {
        return None;
    }
    row.iter()
        .take(key_count)
        .map(|value| {
            if value.is_null() {
                None
            } else {
                Some(value_token(value))
            }
        })
        .collect::<Option<Vec<_>>>()
        .map(|parts| parts.join("\u{1}"))
}

/// Same-engine cells arrive with the same JSON shape, so this only has to absorb number shapes.
pub(crate) fn values_equal(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Null, Value::Null) => true,
        (Value::Number(a), Value::Number(b)) => {
            if let (Some(a), Some(b)) = (a.as_i64(), b.as_i64()) {
                a == b
            } else if let (Some(a), Some(b)) = (a.as_u64(), b.as_u64()) {
                a == b
            } else {
                match (a.as_f64(), b.as_f64()) {
                    (Some(a), Some(b)) => (a - b).abs() <= f64::EPSILON,
                    _ => a == b,
                }
            }
        }
        (Value::Bool(a), Value::Bool(b)) => a == b,
        (Value::String(a), Value::String(b)) => a == b,
        (Value::Object(a), Value::Object(b)) => a == b,
        _ => false,
    }
}

/// Human-readable literal used in the review preview only; execution always binds parameters.
pub(crate) fn literal_preview(protocol: Protocol, value: &Value) -> String {
    match value {
        Value::Null => "NULL".into(),
        Value::Bool(flag) => flag.to_string(),
        Value::Number(number) => number.to_string(),
        Value::String(text) => format!("'{}'", text.replace('\'', "''")),
        Value::Array(items) => format!(
            "'[{}]'",
            items.iter().map(|item| literal_preview(protocol, item)).collect::<Vec<_>>().join(",")
        ),
        Value::Object(map) => match map.get("bytesBase64").and_then(Value::as_str) {
            Some(encoded) => {
                let hex = base64::engine::general_purpose::STANDARD
                    .decode(encoded)
                    .map(|bytes| bytes.iter().map(|byte| format!("{byte:02X}")).collect::<String>())
                    .unwrap_or_else(|_| encoded.to_string());
                match protocol {
                    Protocol::Mysql => format!("0x{hex}"),
                    Protocol::Postgres => format!("decode('{hex}','hex')"),
                    _ => format!("x'{hex}'"),
                }
            }
            None => format!("'{}'", value.to_string().replace('\'', "''")),
        },
    }
}

fn key_summary(protocol: Protocol, row: &[Value], keys: &[String]) -> String {
    keys.iter()
        .enumerate()
        .map(|(index, name)| format!("{name}={}", literal_preview(protocol, row.get(index).unwrap_or(&Value::Null))))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Flattens the leading key cells of a page, in row order, to bind into `matching_select`.
pub(crate) fn collect_keys(rows: &[Vec<Value>], key_count: usize) -> Vec<Value> {
    rows.iter().flat_map(|row| row.iter().take(key_count).cloned()).collect()
}

pub(crate) fn insert_statement(
    protocol: Protocol,
    qualified: &str,
    columns: &[String],
    row: &[Value],
    id: usize,
) -> AppResult<PlannedStatement> {
    let params = row
        .iter()
        .take(columns.len())
        .cloned()
        .map(json_to_param)
        .collect::<AppResult<Vec<_>>>()?;
    Ok(PlannedStatement {
        id,
        kind: "insert",
        sql: format!(
            "INSERT INTO {qualified} ({}) VALUES ({})",
            columns.iter().map(|name| quote(protocol, name)).collect::<Vec<_>>().join(", "),
            (1..=columns.len()).map(|index| placeholder(protocol, index)).collect::<Vec<_>>().join(", ")
        ),
        params,
    })
}

pub(crate) fn update_statement(
    protocol: Protocol,
    qualified: &str,
    columns: &[String],
    spec: &SyncSpec,
    row: &[Value],
    changed: &[String],
    id: usize,
) -> AppResult<PlannedStatement> {
    let mut params = Vec::with_capacity(changed.len() + spec.keys.len());
    let sets = changed
        .iter()
        .map(|name| {
            let index = position_of(columns, name)
                .ok_or_else(|| AppError::validation(format!("列 {name} 不在比对范围内。")))?;
            params.push(json_to_param(row.get(index).cloned().unwrap_or(Value::Null))?);
            Ok(format!(
                "{} = {}",
                quote(protocol, name),
                placeholder(protocol, params.len())
            ))
        })
        .collect::<AppResult<Vec<_>>>()?;
    let predicates = key_predicate(protocol, columns, spec, row, params.len(), &mut params)?;
    Ok(PlannedStatement {
        id,
        kind: "update",
        sql: format!("UPDATE {qualified} SET {} WHERE {}", sets.join(", "), predicates.join(" AND ")),
        params,
    })
}

pub(crate) fn delete_statement(
    protocol: Protocol,
    qualified: &str,
    columns: &[String],
    spec: &SyncSpec,
    row: &[Value],
    id: usize,
) -> AppResult<PlannedStatement> {
    let mut params = Vec::with_capacity(spec.keys.len());
    let predicates = key_predicate(protocol, columns, spec, row, 0, &mut params)?;
    Ok(PlannedStatement {
        id,
        kind: "delete",
        sql: format!("DELETE FROM {qualified} WHERE {}", predicates.join(" AND ")),
        params,
    })
}

/// `key = $n` for every key column after `first` already-bound parameters, binding each key value.
fn key_predicate(
    protocol: Protocol,
    columns: &[String],
    spec: &SyncSpec,
    row: &[Value],
    first: usize,
    params: &mut Vec<crate::models::ParamValue>,
) -> AppResult<Vec<String>> {
    let mut parts = Vec::with_capacity(spec.keys.len());
    for (offset, key) in spec.keys.iter().enumerate() {
        let index = position_of(columns, key)
            .ok_or_else(|| AppError::validation(format!("主键列 {key} 不在比对范围内。")))?;
        let value = row.get(index).cloned().unwrap_or(Value::Null);
        if value.is_null() {
            return Err(AppError::validation("主键值为空，无法定位目标行。"));
        }
        params.push(json_to_param(value)?);
        parts.push(format!(
            "{} = {}",
            quote(protocol, key),
            placeholder(protocol, first + offset + 1)
        ));
    }
    Ok(parts)
}

#[derive(Debug, Default, Clone)]
pub(crate) struct PlannedDiff {
    pub(crate) operations: Vec<SyncOperation>,
    pub(crate) statements: Vec<PlannedStatement>,
}

/// Decide between insert, update and identical for one source page.
///
/// Both sides are read with the very same positional column list, so a cell comparison is a
/// plain index lookup rather than a name resolution per row.
pub(crate) fn classify_source_rows(
    protocol: Protocol,
    qualified_target: &str,
    spec: &SyncSpec,
    columns: &[String],
    source_rows: &[Vec<Value>],
    target_rows: &[Vec<Value>],
    first_id: usize,
) -> AppResult<(PlannedDiff, u64, u64)> {
    let key_count = spec.keys.len();
    let mut lookup: HashMap<String, usize> = HashMap::with_capacity(target_rows.len());
    for (index, row) in target_rows.iter().enumerate() {
        if let Some(signature) = key_signature(row, key_count) {
            lookup.insert(signature, index);
        }
    }
    let mut diff = PlannedDiff::default();
    let mut identical = 0_u64;
    let mut keyless = 0_u64;
    let mut id = first_id;
    for row in source_rows {
        let Some(signature) = key_signature(row, key_count) else {
            keyless += 1;
            continue;
        };
        id += 1;
        match lookup.get(&signature) {
            None => {
                let statement = insert_statement(protocol, qualified_target, columns, row, id)?;
                let preview = format!(
                    "{}  -- {}",
                    statement.sql,
                    values_preview(protocol, columns, row)
                );
                diff.operations.push(SyncOperation {
                    id,
                    kind: "insert".into(),
                    key: key_summary(protocol, row, &spec.keys),
                    columns: columns[key_count..].to_vec(),
                    preview_sql: preview,
                    selected: true,
                });
                diff.statements.push(statement);
            }
            Some(target_index) => {
                let current = &target_rows[*target_index];
                let changed = spec
                    .compared
                    .iter()
                    .filter(|name| {
                        let index = position_of(columns, name).unwrap_or(usize::MAX);
                        !values_equal(
                            row.get(index).unwrap_or(&Value::Null),
                            current.get(index).unwrap_or(&Value::Null),
                        )
                    })
                    .cloned()
                    .collect::<Vec<_>>();
                if changed.is_empty() {
                    identical += 1;
                    continue;
                }
                let statement = update_statement(protocol, qualified_target, columns, spec, row, &changed, id)?;
                diff.operations.push(SyncOperation {
                    id,
                    kind: "update".into(),
                    key: key_summary(protocol, row, &spec.keys),
                    columns: changed.clone(),
                    preview_sql: statement.sql.clone(),
                    selected: true,
                });
                diff.statements.push(statement);
            }
        }
    }
    Ok((diff, identical, keyless))
}

/// Target rows with no source counterpart become deletes, unchecked by default.
pub(crate) fn classify_target_rows(
    protocol: Protocol,
    qualified_target: &str,
    spec: &SyncSpec,
    columns: &[String],
    target_rows: &[Vec<Value>],
    source_rows: &[Vec<Value>],
    first_id: usize,
) -> AppResult<(PlannedDiff, u64, u64)> {
    let key_count = spec.keys.len();
    let mut present: HashMap<String, ()> = HashMap::with_capacity(source_rows.len());
    for row in source_rows {
        if let Some(signature) = key_signature(row, key_count) {
            present.insert(signature, ());
        }
    }
    let mut diff = PlannedDiff::default();
    let mut kept = 0_u64;
    let mut keyless = 0_u64;
    let mut id = first_id;
    for row in target_rows {
        let Some(signature) = key_signature(row, key_count) else {
            keyless += 1;
            continue;
        };
        if present.contains_key(&signature) {
            kept += 1;
            continue;
        }
        id += 1;
        let statement = delete_statement(protocol, qualified_target, columns, spec, row, id)?;
        diff.operations.push(SyncOperation {
            id,
            kind: "delete".into(),
            key: key_summary(protocol, row, &spec.keys),
            columns: Vec::new(),
            preview_sql: statement.sql.clone(),
            selected: false,
        });
        diff.statements.push(statement);
    }
    Ok((diff, kept, keyless))
}

fn values_preview(protocol: Protocol, columns: &[String], row: &[Value]) -> String {
    columns
        .iter()
        .enumerate()
        .map(|(index, name)| format!("{name}: {}", literal_preview(protocol, row.get(index).unwrap_or(&Value::Null))))
        .collect::<Vec<_>>()
        .join(", ")
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

async fn fetch_bound(
    state: &AppState,
    side: &SideConnection,
    schema: &str,
    sql: String,
    params: Vec<crate::models::ParamValue>,
    page_size: u32,
    offset: Option<u64>,
    operation_id: &str,
    job: Option<&Arc<Job<SyncProgress>>>,
) -> AppResult<Vec<Vec<Value>>> {
    let guard = job.map(|job| {
        job.track(ActiveOperation {
            connection_id: side.config.id.clone(),
            operation_id: operation_id.into(),
        })
    });
    let result = state
        .sql
        .fetch(
            &side.config,
            side.password.clone(),
            SqlRequest {
                connection_id: side.config.id.clone(),
                sql,
                params,
                operation_id: operation_id.into(),
                page_size: Some(page_size),
                offset,
                schema: (!schema.is_empty()).then(|| schema.to_string()),
            },
        )
        .await;
    drop(guard);
    Ok(result?.rows)
}

struct Page {
    rows: Vec<Vec<Value>>,
    operation_id: String,
}

/// One paged scan of a table ordered by its keys.
///
/// Advancing the cursor is a method rather than a callback so the per-page work can await the
/// matching key lookup on the other side.
struct Scanner<'a> {
    state: &'a AppState,
    job: &'a Arc<Job<SyncProgress>>,
    side: &'a SideConnection,
    schema: &'a str,
    select: String,
    page_rows: u32,
    operation_id: &'a str,
    max_rows: u64,
    offset: u64,
    scanned: u64,
    page: u64,
    done: bool,
}

impl<'a> Scanner<'a> {
    fn new(
        state: &'a AppState,
        job: &'a Arc<Job<SyncProgress>>,
        side: &'a SideConnection,
        schema: &'a str,
        select: String,
        page_rows: u32,
        operation_id: &'a str,
        max_rows: u64,
    ) -> Self {
        Self {
            state,
            job,
            side,
            schema,
            select,
            page_rows,
            operation_id,
            max_rows,
            offset: 0,
            scanned: 0,
            page: 0,
            done: false,
        }
    }

    async fn next(&mut self) -> AppResult<Option<Page>> {
        if self.done {
            return Ok(None);
        }
        if self.job.is_cancelled() {
            return Err(AppError::cancelled());
        }
        let operation_id = format!("{}-{}", self.operation_id, self.page);
        let rows = fetch_bound(
            self.state,
            self.side,
            self.schema,
            self.select.clone(),
            Vec::new(),
            self.page_rows,
            Some(self.offset),
            &operation_id,
            Some(self.job),
        )
        .await?;
        if rows.is_empty() {
            self.done = true;
            return Ok(None);
        }
        self.scanned += rows.len() as u64;
        if self.scanned > self.max_rows {
            return Err(AppError::validation(format!(
                "扫描行数超过上限 {}。数据同步用于追平少量差异，整表搬运请使用数据传输。",
                self.max_rows
            )));
        }
        self.done = rows.len() < self.page_rows as usize;
        self.offset += rows.len() as u64;
        self.page += 1;
        Ok(Some(Page { rows, operation_id }))
    }
}

pub(crate) struct BuiltPlan {
    pub(crate) plan: SyncPlan,
    pub(crate) statements: Vec<PlannedStatement>,
}

/// Rows on the other side matching the key tuples of one scanned page.
async fn fetch_matches(
    state: &AppState,
    job: &Arc<Job<SyncProgress>>,
    side: &SideConnection,
    schema: &str,
    qualified: &str,
    protocol: Protocol,
    spec: &SyncSpec,
    columns: &[String],
    rows: &[Vec<Value>],
    operation_id: &str,
) -> AppResult<Vec<Vec<Value>>> {
    let sql = matching_select(protocol, qualified, columns, &spec.keys, rows.len())?;
    let params = collect_keys(rows, spec.keys.len())
        .into_iter()
        .map(json_to_param)
        .collect::<AppResult<Vec<_>>>()?;
    fetch_bound(
        state,
        side,
        schema,
        sql,
        params,
        MAX_SYNC_PAGE_ROWS * 2,
        None,
        &format!("{operation_id}-m"),
        Some(job),
    )
    .await
}

async fn build_plan(
    state: &AppState,
    job: &Arc<Job<SyncProgress>>,
    request: &SyncPlanRequest,
    source: &SideConnection,
    target: &SideConnection,
    spec: &SyncSpec,
) -> AppResult<BuiltPlan> {
    let started = Instant::now();
    let protocol = source.config.protocol;
    let columns = spec.columns();
    let qualified_source =
        qualified_table(&protocol, &request.source_schema, &request.source_table)?;
    let qualified_target =
        qualified_table(&target.config.protocol, &request.target_schema, &request.target_table)?;
    let page_rows = sync_page_rows(spec.keys.len());
    let mut operations: Vec<SyncOperation> = Vec::new();
    let mut statements: Vec<PlannedStatement> = Vec::new();
    let mut notes = spec.notes.clone();
    let mut identical = 0_u64;
    let mut keyless = 0_u64;

    // Pass 1: each source row decides between insert, update and identical.
    job.edit(|progress| progress.phase = "source".into());
    let source_scan_id = format!("{}-s", request.operation_id);
    let mut scanner = Scanner::new(
        state,
        job,
        source,
        &request.source_schema,
        scan_select(protocol, &qualified_source, &columns, &spec.keys),
        page_rows,
        &source_scan_id,
        request.max_rows,
    );
    while let Some(page) = scanner.next().await? {
        let matches = fetch_matches(
            state,
            job,
            target,
            &request.target_schema,
            &qualified_target,
            target.config.protocol,
            spec,
            &columns,
            &page.rows,
            &page.operation_id,
        )
        .await?;
        let (diff, same, skipped) = classify_source_rows(
            target.config.protocol,
            &qualified_target,
            spec,
            &columns,
            &page.rows,
            &matches,
            operations.len(),
        )?;
        identical += same;
        keyless += skipped;
        operations.extend(diff.operations);
        statements.extend(diff.statements);
        job.edit(|progress| {
            progress.source_rows = scanner.scanned;
            progress.identical = identical;
            progress.elapsed_ms = started.elapsed().as_millis() as u64;
            recount(progress, &operations);
        });
        if operations.len() > MAX_PLAN_OPERATIONS {
            return Err(too_many_changes(operations.len()));
        }
    }
    let source_rows = scanner.scanned;

    // Pass 2: target rows with no source counterpart become deletes.
    job.edit(|progress| progress.phase = "target".into());
    let target_scan_id = format!("{}-t", request.operation_id);
    let mut scanner = Scanner::new(
        state,
        job,
        target,
        &request.target_schema,
        scan_select(
            target.config.protocol,
            &qualified_target,
            &columns,
            &spec.keys,
        ),
        page_rows,
        &target_scan_id,
        request.max_rows,
    );
    while let Some(page) = scanner.next().await? {
        let matches = fetch_matches(
            state,
            job,
            source,
            &request.source_schema,
            &qualified_source,
            protocol,
            spec,
            &columns,
            &page.rows,
            &page.operation_id,
        )
        .await?;
        let (diff, _, skipped) = classify_target_rows(
            target.config.protocol,
            &qualified_target,
            spec,
            &columns,
            &page.rows,
            &matches,
            operations.len(),
        )?;
        keyless += skipped;
        operations.extend(diff.operations);
        statements.extend(diff.statements);
        job.edit(|progress| {
            progress.target_rows = scanner.scanned;
            progress.elapsed_ms = started.elapsed().as_millis() as u64;
            recount(progress, &operations);
        });
        if operations.len() > MAX_PLAN_OPERATIONS {
            return Err(too_many_changes(operations.len()));
        }
    }
    let target_rows = scanner.scanned;

    if keyless > 0 {
        notes.push(format!("{keyless} 行的主键为空，已跳过。"));
    }
    let plan = SyncPlan {
        plan_id: uuid::Uuid::new_v4().to_string(),
        source: side_of(source, &request.source_schema, &request.source_table),
        target: side_of(target, &request.target_schema, &request.target_table),
        key_columns: spec.keys.clone(),
        copied_columns: columns,
        skipped_columns: spec.skipped.clone(),
        source_rows,
        target_rows,
        identical,
        elapsed_ms: started.elapsed().as_millis() as u64,
        operations,
        notes,
    };
    Ok(BuiltPlan { plan, statements })
}

fn too_many_changes(operations: usize) -> AppError {
    AppError::validation(format!(
        "差异条数 {operations} 超过上限 {MAX_PLAN_OPERATIONS}，请改用数据传输。"
    ))
}

fn recount(progress: &mut SyncProgress, operations: &[SyncOperation]) {
    progress.inserts = operations.iter().filter(|entry| entry.kind == "insert").count() as u64;
    progress.updates = operations.iter().filter(|entry| entry.kind == "update").count() as u64;
    progress.deletes = operations.iter().filter(|entry| entry.kind == "delete").count() as u64;
}

fn side_of(side: &SideConnection, schema: &str, table: &str) -> SyncSide {
    SyncSide {
        connection_id: side.config.id.clone(),
        connection_name: side.config.name.clone(),
        protocol: side.config.protocol.as_str().into(),
        schema: schema.into(),
        table: table.into(),
    }
}

pub(crate) async fn run_sync_plan(
    state: Arc<AppState>,
    job: Arc<Job<SyncProgress>>,
    request: SyncPlanRequest,
    source: SideConnection,
    target: SideConnection,
    spec: SyncSpec,
) {
    match build_plan(&state, &job, &request, &source, &target, &spec).await {
        Ok(BuiltPlan { plan, statements }) => {
            let operations = plan.operations.len();
            job.edit(|progress| {
                recount(progress, &plan.operations);
                progress.elapsed_ms = plan.elapsed_ms;
            });
            let summary = format!(
                "Row diff of {}.{} against {}.{}: {operations} changes",
                plan.source.schema, plan.source.table, plan.target.schema, plan.target.table
            );
            let _ = state.store.append_audit(
                Some(&plan.target.connection_id),
                Some(&plan.target.connection_name),
                Some(&plan.target.protocol),
                "data_sync",
                &summary,
                Some(operations as i64),
                "success",
                None,
            );
            let cached = CachedPlan {
                plan,
                statements: statements
                    .into_iter()
                    .map(|statement| (statement.id, statement))
                    .collect(),
                created: Instant::now(),
            };
            let id = cached.plan.plan_id.clone();
            state.sync_plans.insert(cached);
            job.edit(|progress| {
                progress.status = "done".into();
                progress.phase = "finished".into();
                progress.plan_id = Some(id);
            });
        }
        Err(error) => {
            let cancelled = error.class == ErrorClass::Cancelled || job.is_cancelled();
            job.edit(|progress| {
                progress.status = if cancelled { "cancelled" } else { "failed" }.into();
                progress.phase = "finished".into();
                progress.error = Some(error.message.clone());
            });
            let _ = state.store.append_audit(
                Some(&target.config.id),
                Some(&target.config.name),
                Some(target.config.protocol.as_str()),
                "data_sync",
                "Row diff scan failed",
                None,
                "failure",
                Some(&error.message),
            );
        }
    }
    job.mark_finished();
}

#[tauri::command]
pub async fn sync_plan_start(
    state: State<'_, Arc<AppState>>,
    request: SyncPlanRequest,
) -> AppResult<SyncProgress> {
    let state = state_ref(&state);
    state.ensure_running()?;
    validate_plan_request(&request)?;
    let source_config = state.connection(&request.source_connection_id)?;
    let target_config = state.connection(&request.target_connection_id)?;
    if source_config.protocol.family() != DriverFamily::Sql
        || target_config.protocol.family() != DriverFamily::Sql
    {
        return Err(AppError::unsupported("数据同步只在 SQL 连接之间进行。"));
    }
    let source = SideConnection {
        password: state.connection_password(&source_config)?,
        config: source_config,
    };
    let target = SideConnection {
        password: state.connection_password(&target_config)?,
        config: target_config,
    };
    let source_table = table_detail(&state, &source, &request.source_schema, &request.source_table)
        .await?
        .ok_or_else(|| AppError::validation("源表不存在，请刷新后重试。"))?;
    let target_table = table_detail(&state, &target, &request.target_schema, &request.target_table)
        .await?
        .ok_or_else(|| AppError::validation("目标表不存在，请刷新后重试。"))?;
    let spec = validate_sync_tables(
        source.config.protocol,
        target.config.protocol,
        &source_table,
        &target_table,
    )?;
    let progress = SyncProgress {
        operation_id: request.operation_id.clone(),
        status: "running".into(),
        phase: "source".into(),
        source: side_of(&source, &request.source_schema, &request.source_table),
        target: side_of(&target, &request.target_schema, &request.target_table),
        source_rows: 0,
        target_rows: 0,
        identical: 0,
        inserts: 0,
        updates: 0,
        deletes: 0,
        elapsed_ms: 0,
        plan_id: None,
        error: None,
    };
    let job = state.sync_jobs.register(request.operation_id.clone(), progress.clone());
    tokio::spawn(run_sync_plan(
        Arc::clone(&state),
        job,
        request,
        source,
        target,
        spec,
    ));
    Ok(progress)
}

pub(crate) fn validate_plan_request(request: &SyncPlanRequest) -> AppResult<()> {
    if request.operation_id.trim().is_empty() {
        return Err(AppError::validation("一次比对需要一个 operationId 才能停止。"));
    }
    if request.max_rows == 0 || request.max_rows > 5_000_000 {
        return Err(AppError::validation("扫描行数上限需要在 1 到 5000000 之间。"));
    }
    for (side, schema, table) in [
        ("源", request.source_schema.as_str(), request.source_table.as_str()),
        ("目标", request.target_schema.as_str(), request.target_table.as_str()),
    ] {
        if table.trim().is_empty() {
            return Err(AppError::validation(format!("请选择{side}表。")));
        }
        if schema.trim().is_empty() {
            return Err(AppError::validation(format!("请为{side}选择数据库或模式。")));
        }
    }
    if request.source_connection_id == request.target_connection_id
        && request.source_schema == request.target_schema
        && request.source_table == request.target_table
    {
        return Err(AppError::validation("源表和目标表是同一张表，无需同步。"));
    }
    Ok(())
}

#[tauri::command]
pub async fn sync_plan_status(
    state: State<'_, Arc<AppState>>,
    operation_id: String,
) -> AppResult<SyncProgress> {
    let state = state_ref(&state);
    state.ensure_running()?;
    Ok(state.sync_jobs.get(&operation_id)?.snapshot())
}

#[tauri::command]
pub async fn sync_plan_cancel(
    state: State<'_, Arc<AppState>>,
    operation_id: String,
) -> AppResult<SyncProgress> {
    let state = state_ref(&state);
    let job = state.sync_jobs.get(&operation_id)?;
    for active in state.sync_jobs.cancel(&operation_id)? {
        let _ = state.sql.cancel(&active.connection_id, &active.operation_id).await;
    }
    Ok(job.snapshot())
}

#[tauri::command]
pub async fn sync_get_plan(
    state: State<'_, Arc<AppState>>,
    plan_id: String,
) -> AppResult<SyncPlan> {
    let state = state_ref(&state);
    state.ensure_running()?;
    Ok(state.sync_plans.get(&plan_id)?.plan.clone())
}

fn renumber_postgres(text: &str, offset: usize) -> String {
    let mut out = String::with_capacity(text.len() + 8);
    let mut rest = text;
    while let Some(position) = rest.find('$') {
        out.push_str(&rest[..position]);
        rest = &rest[position + 1..];
        let digits: String = rest.chars().take_while(|ch| ch.is_ascii_digit()).collect();
        if digits.is_empty() {
            out.push('$');
            continue;
        }
        let number: usize = digits.parse().unwrap_or(0);
        out.push_str(&format!("${}", number + offset));
        rest = &rest[digits.len()..];
    }
    out.push_str(rest);
    out
}

/// Collapse per-row diffs into fewer statements: inserts become multi-row VALUES, deletes
/// become OR-ed predicates. Updates stay per-row because each has a different SET list.
/// PostgreSQL placeholders are renumbered whenever predicates are merged.
fn group_apply_statements(protocol: Protocol, selected: &[PlannedStatement]) -> Vec<PlannedStatement> {
    const GROUP_ROWS: usize = 500;
    let mut grouped: Vec<PlannedStatement> = Vec::new();
    let mut inserts: Vec<PlannedStatement> = Vec::new();
    let mut deletes: Vec<PlannedStatement> = Vec::new();
    for entry in selected {
        match entry.kind {
            "insert" => inserts.push(entry.clone()),
            "delete" => deletes.push(entry.clone()),
            _ => grouped.push(entry.clone()),
        }
    }
    for chunk in inserts.chunks(GROUP_ROWS) {
        if chunk.is_empty() {
            continue;
        }
        let prefix = chunk[0].sql.split(" VALUES ").next().unwrap_or("").to_string();
        let columns = chunk[0].params.len();
        let values = crate::drivers::sql::value_placeholders(protocol, columns, chunk.len())
            .expect("placeholder budget validated at plan time");
        let params = chunk
            .iter()
            .flat_map(|entry| entry.params.iter().cloned())
            .collect::<Vec<_>>();
        grouped.push(PlannedStatement {
            id: chunk[0].id,
            kind: "insert",
            sql: format!("{prefix} VALUES {values}"),
            params,
        });
    }
    for chunk in deletes.chunks(GROUP_ROWS) {
        if chunk.is_empty() {
            continue;
        }
        let prefix = chunk[0].sql.split(" WHERE ").next().unwrap_or("").to_string();
        let mut params = Vec::new();
        let mut predicates = Vec::new();
        for entry in chunk {
            let predicate = entry.sql.split(" WHERE ").nth(1).unwrap_or("");
            let offset = params.len();
            let renumbered = if protocol == Protocol::Postgres {
                renumber_postgres(predicate, offset)
            } else {
                predicate.to_string()
            };
            predicates.push(format!("({renumbered})"));
            params.extend(entry.params.iter().cloned());
        }
        grouped.push(PlannedStatement {
            id: chunk[0].id,
            kind: "delete",
            sql: format!("{prefix} WHERE {}", predicates.join(" OR ")),
            params,
        });
    }
    grouped
}

#[tauri::command]
pub async fn sync_apply(
    state: State<'_, Arc<AppState>>,
    request: SyncApplyRequest,
) -> AppResult<SyncApplyResult> {
    let state = state_ref(&state);
    state.ensure_running()?;
    if request.operation_id.trim().is_empty() {
        return Err(AppError::validation("执行差异需要一个 operationId。"));
    }
    let cached = state.sync_plans.get(&request.plan_id)?;
    let mut selected = Vec::with_capacity(request.operation_ids.len());
    for id in &request.operation_ids {
        let statement = cached
            .statements
            .get(id)
            .cloned()
            .ok_or_else(|| AppError::validation(format!("差异项 {id} 不属于这个计划。")))?;
        selected.push(statement);
    }
    if selected.is_empty() {
        return Err(AppError::validation("没有勾选要执行的差异。"));
    }
    let deletes = selected.iter().filter(|entry| entry.kind == "delete").count();
    if deletes > 0 && !request.confirmed_dangerous {
        return Err(AppError::new(
            ErrorClass::PermissionDenied,
            "这个计划会删除目标行，必须明确确认后才能执行。",
        ));
    }
    // Inserts restore missing rows first, updates follow, deletes run last so a key reuse works.
    selected.sort_by_key(|entry| match entry.kind {
        "delete" => 2,
        "update" => 1,
        _ => 0,
    });
    let target = cached.plan.target.clone();
    let config = state.connection(&target.connection_id)?;
    let password = state.connection_password(&config)?;
    let target_protocol = match target.protocol.as_str() {
        "postgres" => Protocol::Postgres,
        "sqlite" => Protocol::Sqlite,
        _ => Protocol::Mysql,
    };
    let statements = group_apply_statements(target_protocol, &selected)
        .into_iter()
        .enumerate()
        .map(|(index, entry)| SqlRequest {
            connection_id: target.connection_id.clone(),
            sql: entry.sql.clone(),
            params: entry.params.clone(),
            operation_id: format!("{}-{index}", request.operation_id),
            page_size: None,
            offset: None,
            schema: (!target.schema.is_empty()).then(|| target.schema.clone()),
        })
        .collect::<Vec<_>>();
    let started = Instant::now();
    let inserts = selected.iter().filter(|entry| entry.kind == "insert").count();
    let updates = selected.iter().filter(|entry| entry.kind == "update").count();
    let result = state
        .sql
        .execute_batch(
            &config,
            password,
            SqlBatchRequest {
                connection_id: target.connection_id.clone(),
                statements,
                operation_id: request.operation_id.clone(),
                wrap_in_transaction: true,
            },
        )
        .await;
    match result {
        Ok(results) => {
            // A consumed plan would silently re-apply an outdated diff, so it is dropped here.
            state.sync_plans.remove(&request.plan_id);
            let response = SyncApplyResult {
                operation_id: request.operation_id.clone(),
                applied: selected.len(),
                inserts,
                updates,
                deletes,
                rows_affected: results.iter().map(|entry| entry.rows_affected).sum(),
                duration_ms: started.elapsed().as_millis() as u64,
                committed: true,
            };
            let _ = state.store.append_audit(
                Some(&target.connection_id),
                Some(&target.connection_name),
                Some(&target.protocol),
                "data_sync",
                &format!(
                    "Applied {} row changes on {}.{} ({} insert, {} update, {} delete)",
                    response.applied, target.schema, target.table, inserts, updates, deletes
                ),
                Some(response.rows_affected as i64),
                "success",
                None,
            );
            Ok(response)
        }
        Err(error) => {
            let _ = state.store.append_audit(
                Some(&target.connection_id),
                Some(&target.connection_name),
                Some(&target.protocol),
                "data_sync",
                &format!("Row sync on {}.{} failed", target.schema, target.table),
                None,
                "failure",
                Some(&error.message),
            );
            Err(error)
        }
    }
}

#[cfg(test)]
mod rowsync_tests {
    use super::*;
    use crate::models::{ColumnMetadata, ParamValue};
    use serde_json::json;

    fn column(name: &str, primary: bool) -> ColumnMetadata {
        ColumnMetadata {
            name: name.into(),
            data_type: "int".into(),
            nullable: !primary,
            default_value: None,
            is_primary_key: primary,
            ordinal: 1,
            comment: None,
            extra: None,
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

    fn sync_spec(keys: &[&str], compared: &[&str]) -> SyncSpec {
        SyncSpec {
            keys: keys.iter().map(|name| name.to_string()).collect(),
            compared: compared.iter().map(|name| name.to_string()).collect(),
            skipped: Vec::new(),
            notes: Vec::new(),
        }
    }

    fn side(name: &str) -> SyncSide {
        SyncSide {
            connection_id: "conn".into(),
            connection_name: name.into(),
            protocol: "mysql".into(),
            schema: "shop".into(),
            table: "orders".into(),
        }
    }

    fn cached_plan(id: &str, created: Instant) -> CachedPlan {
        CachedPlan {
            plan: SyncPlan {
                plan_id: id.into(),
                source: side("source"),
                target: side("target"),
                key_columns: vec!["id".into()],
                copied_columns: vec!["id".into(), "status".into()],
                skipped_columns: Vec::new(),
                source_rows: 1,
                target_rows: 1,
                identical: 0,
                elapsed_ms: 1,
                operations: Vec::new(),
                notes: Vec::new(),
            },
            statements: HashMap::new(),
            created,
        }
    }

    #[test]
    fn same_engine_tables_with_matching_keys_yield_a_column_plan() {
        let source = table(
            "orders",
            vec![column("id", true), column("status", false), column("note", false)],
        );
        let target = table(
            "orders",
            vec![column("id", true), column("status", false), column("tenant", false)],
        );
        let spec = validate_sync_tables(Protocol::Mysql, Protocol::Mysql, &source, &target).unwrap();
        assert_eq!(spec.keys, vec!["id".to_string()]);
        assert_eq!(spec.compared, vec!["status".to_string()]);
        assert_eq!(spec.skipped, vec!["note".to_string(), "tenant".to_string()]);
        assert_eq!(spec.columns(), vec!["id".to_string(), "status".to_string()]);
        assert!(spec.notes.iter().any(|note| note.contains("note") && note.contains("结构比较")));
    }

    #[test]
    fn unsafe_sync_pairs_are_rejected_before_any_scan() {
        let orders = table("orders", vec![column("id", true), column("status", false)]);
        let keyless = table("orders", vec![column("status", false)]);
        let renamed = table("orders", vec![column("order_id", true), column("status", false)]);
        let view = TableMetadata { table_type: "VIEW".into(), ..orders.clone() };
        let keys_only = table("orders", vec![column("id", true)]);
        let wide_key = table(
            "orders",
            (1..=7)
                .map(|index| column(&format!("k{index}"), true))
                .chain(vec![column("status", false)])
                .collect(),
        );
        let cases: Vec<(Protocol, Protocol, TableMetadata, TableMetadata, &str)> = vec![
            (Protocol::Mysql, Protocol::Postgres, orders.clone(), orders.clone(), "数据传输"),
            (Protocol::Mysql, Protocol::Mysql, keyless, orders.clone(), "没有主键"),
            (Protocol::Mysql, Protocol::Mysql, orders.clone(), renamed, "主键定义不同"),
            (Protocol::Mysql, Protocol::Mysql, view, orders.clone(), "视图"),
            (Protocol::Mysql, Protocol::Mysql, keys_only.clone(), keys_only, "同名列"),
            (Protocol::Mysql, Protocol::Mysql, wide_key.clone(), wide_key, "主键列超过"),
        ];
        for (source_engine, target_engine, source, target, expected) in cases {
            let message = validate_sync_tables(source_engine, target_engine, &source, &target)
                .unwrap_err()
                .message;
            assert!(message.contains(expected), "{message} should mention {expected}");
        }
    }

    #[test]
    fn key_lookups_stay_inside_the_bind_budget() {
        assert_eq!(sync_page_rows(1), MAX_SYNC_PAGE_ROWS);
        assert_eq!(sync_page_rows(6), MAX_SYNC_PAGE_ROWS);
        assert_eq!(sync_page_rows(300), 100);
        assert_eq!(
            matching_select(
                Protocol::Postgres,
                "\"shop\".\"orders\"",
                &["id".to_string(), "status".to_string()],
                &["id".to_string()],
                2,
            )
            .unwrap(),
            "SELECT \"id\", \"status\" FROM \"shop\".\"orders\" WHERE (\"id\" = $1) OR (\"id\" = $2)"
        );
        let mysql = matching_select(
            Protocol::Mysql,
            "`shop`.`orders`",
            &["id".to_string()],
            &["a".to_string(), "b".to_string()],
            2,
        )
        .unwrap();
        assert!(mysql.contains("(`a` = ? AND `b` = ?) OR (`a` = ? AND `b` = ?)"));
        assert!(matching_select(Protocol::Mysql, "`t`", &["id".to_string()], &[], 1).is_err());
        assert!(matching_select(
            Protocol::Mysql,
            "`t`",
            &["id".to_string()],
            &["a".to_string(), "b".to_string()],
            MAX_BIND_PARAMETERS / 2 + 1
        )
        .is_err());
        assert_eq!(
            scan_select(
                Protocol::Mysql,
                "`shop`.`orders`",
                &["id".to_string(), "status".to_string()],
                &["id".to_string()]
            ),
            "SELECT `id`, `status` FROM `shop`.`orders` ORDER BY `id`"
        );
    }

    #[test]
    fn statements_bind_values_and_only_touch_changed_columns() {
        let spec = sync_spec(&["id"], &["status", "note"]);
        let columns = spec.columns();
        let row = vec![json!(7), json!("paid"), Value::Null];
        let insert =
            insert_statement(Protocol::Mysql, "`shop`.`orders`", &columns, &row, 1).unwrap();
        assert_eq!(
            insert.sql,
            "INSERT INTO `shop`.`orders` (`id`, `status`, `note`) VALUES (?, ?, ?)"
        );
        assert!(matches!(insert.params[0], ParamValue::I64(7)));
        assert!(matches!(insert.params[2], ParamValue::Null));

        let update = update_statement(
            Protocol::Postgres,
            "\"shop\".\"orders\"",
            &columns,
            &spec,
            &row,
            &["note".to_string()],
            2,
        )
        .unwrap();
        assert_eq!(
            update.sql,
            "UPDATE \"shop\".\"orders\" SET \"note\" = $1 WHERE \"id\" = $2"
        );
        assert!(matches!(update.params[0], ParamValue::Null));
        assert!(matches!(update.params[1], ParamValue::I64(7)));

        let delete =
            delete_statement(Protocol::Postgres, "\"shop\".\"orders\"", &columns, &spec, &row, 3)
                .unwrap();
        assert_eq!(delete.sql, "DELETE FROM \"shop\".\"orders\" WHERE \"id\" = $1");

        // A composite key binds every key column after the changed columns.
        let composite = sync_spec(&["tenant", "id"], &["status"]);
        let composite_columns = composite.columns();
        let composite_row = vec![json!("acme"), json!(4), json!("paid")];
        let update = update_statement(
            Protocol::Postgres,
            "\"t\"",
            &composite_columns,
            &composite,
            &composite_row,
            &["status".to_string()],
            7,
        )
        .unwrap();
        assert_eq!(
            update.sql,
            "UPDATE \"t\" SET \"status\" = $1 WHERE \"tenant\" = $2 AND \"id\" = $3"
        );
        assert_eq!(update.params.len(), 3);
        assert!(matches!(&update.params[1], ParamValue::Text(text) if text == "acme"));

        // A NULL key cannot address a row, so no statement is planned for it.
        let broken = vec![Value::Null, json!("a"), json!("b")];
        assert!(delete_statement(Protocol::Mysql, "`t`", &columns, &spec, &broken, 4).is_err());
        assert!(update_statement(
            Protocol::Mysql,
            "`t`",
            &columns,
            &spec,
            &broken,
            &["status".to_string()],
            5
        )
        .is_err());
        assert!(update_statement(
            Protocol::Mysql,
            "`t`",
            &columns,
            &spec,
            &row,
            &["legacy".to_string()],
            6
        )
        .is_err());
    }

    #[test]
    fn source_rows_become_inserts_updates_or_nothing() {
        let spec = sync_spec(&["id"], &["status"]);
        let columns = spec.columns();
        let source = vec![
            vec![json!(1), json!("new")],
            vec![json!(2), json!("paid")],
            vec![json!(3), json!("paid")],
            vec![Value::Null, json!("orphan")],
        ];
        let target = vec![vec![json!(2), json!("open")], vec![json!(3), json!("paid")]];
        let (diff, identical, keyless) = classify_source_rows(
            Protocol::Mysql,
            "`shop`.`orders`",
            &spec,
            &columns,
            &source,
            &target,
            0,
        )
        .unwrap();
        assert_eq!(identical, 1);
        assert_eq!(keyless, 1);
        assert_eq!(diff.operations.len(), 2);
        assert_eq!(diff.statements.len(), 2);
        assert_eq!(diff.operations[0].kind, "insert");
        assert!(diff.operations[0].selected);
        assert_eq!(diff.operations[0].key, "id=1");
        assert!(diff.operations[0].preview_sql.contains("status: 'new'"));
        assert_eq!(diff.operations[1].kind, "update");
        assert_eq!(diff.operations[1].id, 2);
        assert_eq!(diff.operations[1].columns, vec!["status".to_string()]);

        // Equal cells in the accepted number shape produce no operation at all.
        let target = vec![
            vec![json!(1), json!("new")],
            vec![json!(2), json!("paid")],
            vec![json!(3), json!("paid")],
        ];
        let (diff, identical, keyless) = classify_source_rows(
            Protocol::Mysql,
            "`shop`.`orders`",
            &spec,
            &columns,
            &source,
            &target,
            0,
        )
        .unwrap();
        assert!(diff.operations.is_empty());
        assert_eq!(identical, 3);
        assert_eq!(keyless, 1);
    }

    #[test]
    fn target_only_rows_are_planned_as_unchecked_deletes() {
        let spec = sync_spec(&["id"], &["status"]);
        let columns = spec.columns();
        let target_rows = vec![vec![json!(9), json!("stale")], vec![json!(1), json!("live")]];
        let source_rows = vec![vec![json!(1), json!("live")]];
        let (diff, kept, keyless) = classify_target_rows(
            Protocol::Mysql,
            "`shop`.`orders`",
            &spec,
            &columns,
            &target_rows,
            &source_rows,
            5,
        )
        .unwrap();
        assert_eq!(kept, 1);
        assert_eq!(keyless, 0);
        assert_eq!(diff.operations.len(), 1);
        assert_eq!(diff.operations[0].kind, "delete");
        assert_eq!(diff.operations[0].id, 6);
        assert!(!diff.operations[0].selected);
        assert_eq!(diff.statements[0].sql, "DELETE FROM `shop`.`orders` WHERE `id` = ?");
    }

    #[test]
    fn cells_compare_across_number_shapes_and_types() {
        assert!(values_equal(&json!(1), &json!(1.0)));
        assert!(!values_equal(&json!("1"), &json!(1)));
        assert!(values_equal(&Value::Null, &Value::Null));
        assert!(values_equal(
            &json!({"bytesBase64": "AA=="}),
            &json!({"bytesBase64": "AA=="})
        ));
        assert_eq!(key_signature(&[json!(1), json!("a")], 1).as_deref(), Some("n1"));
        assert_eq!(key_signature(&[json!("a"), json!(2)], 2).as_deref(), Some("s1:a\u{1}n2"));
        assert!(key_signature(&[Value::Null], 1).is_none());
        assert_eq!(key_signature(&[json!(1)], 2), None);
        assert_eq!(position_of(&["id".to_string(), "status".to_string()], "status"), Some(1));
        assert_eq!(position_of(&["id".to_string()], "legacy"), None);
        assert_eq!(collect_keys(&[vec![json!(1), json!("a")]], 1), vec![json!(1)]);
        assert_eq!(literal_preview(Protocol::Postgres, &json!({"bytesBase64": "AA=="})), "decode('00','hex')");
        assert_eq!(literal_preview(Protocol::Mysql, &json!("it's")), "'it''s'");
        assert_eq!(literal_preview(Protocol::Sqlite, &Value::Null), "NULL");
        assert_eq!(literal_preview(Protocol::Mysql, &json!(true)), "true");
    }

    #[test]
    fn cached_plans_expire_and_stay_bounded() {
        let cache = PlanCache::default();
        cache.insert(cached_plan("p-1", Instant::now()));
        assert_eq!(cache.get("p-1").unwrap().plan.plan_id, "p-1");
        assert!(cache.get("absent").is_err());
        for index in 0..MAX_CACHED_PLANS + 4 {
            cache.insert(cached_plan(&format!("p-{index}"), Instant::now()));
        }
        assert_eq!(cache.len(), MAX_CACHED_PLANS);
        assert!(cache.get("p-0").is_err());
        assert!(cache.get("p-11").is_ok());

        cache.insert(cached_plan("stale", Instant::now() - PLAN_TTL - Duration::from_secs(1)));
        let error = cache
            .get("stale")
            .err()
            .expect("an expired plan must be refused");
        assert!(error.message.contains("30 分钟"));
        assert_eq!(cache.len(), MAX_CACHED_PLANS - 1);
    }

    #[test]
    fn sync_requests_need_two_tables_and_a_stoppable_operation_id() {
        let base = SyncPlanRequest {
            source_connection_id: "src".into(),
            source_schema: "shop".into(),
            source_table: "orders".into(),
            target_connection_id: "tgt".into(),
            target_schema: "shop".into(),
            target_table: "orders".into(),
            max_rows: 200_000,
            operation_id: "op-1".into(),
        };
        assert!(validate_plan_request(&base).is_ok());
        for (label, request) in [
            ("operationId", SyncPlanRequest { operation_id: " ".into(), ..base.clone() }),
            ("scan cap", SyncPlanRequest { max_rows: 0, ..base.clone() }),
            ("scan cap", SyncPlanRequest { max_rows: 6_000_000, ..base.clone() }),
            ("table", SyncPlanRequest { source_table: String::new(), ..base.clone() }),
            ("schema", SyncPlanRequest { target_schema: " ".into(), ..base.clone() }),
        ] {
            assert!(
                validate_plan_request(&request).is_err(),
                "{label} should be rejected"
            );
        }
        assert!(validate_plan_request(&SyncPlanRequest {
            target_connection_id: "src".into(),
            ..base
        })
        .unwrap_err()
        .message
        .contains("同一张表"));
    }

    /// Mirrors `sync_plan_start` without the Tauri `State` extractor so the live check can await
    /// the scan inline and read the cached plan back, exactly as `sync_apply` does.
    async fn live_plan(
        state: &Arc<AppState>,
        source: &SideConnection,
        target: &SideConnection,
        schema: &str,
        source_table: &str,
        target_table: &str,
    ) -> (SyncProgress, SyncPlan) {
        let request = SyncPlanRequest {
            source_connection_id: source.config.id.clone(),
            source_schema: schema.into(),
            source_table: source_table.into(),
            target_connection_id: target.config.id.clone(),
            target_schema: schema.into(),
            target_table: target_table.into(),
            max_rows: 200_000,
            operation_id: format!("live-sync-{}", uuid::Uuid::new_v4()),
        };
        let source_detail = table_detail(state, source, schema, source_table)
            .await
            .unwrap()
            .expect("source table");
        let target_detail = table_detail(state, target, schema, target_table)
            .await
            .unwrap()
            .expect("target table");
        let spec = validate_sync_tables(
            source.config.protocol,
            target.config.protocol,
            &source_detail,
            &target_detail,
        )
        .unwrap();
        let progress = SyncProgress {
            operation_id: request.operation_id.clone(),
            status: "running".into(),
            phase: "source".into(),
            source: side_of(source, schema, source_table),
            target: side_of(target, schema, target_table),
            source_rows: 0,
            target_rows: 0,
            identical: 0,
            inserts: 0,
            updates: 0,
            deletes: 0,
            elapsed_ms: 0,
            plan_id: None,
            error: None,
        };
        let job = state.sync_jobs.register(request.operation_id.clone(), progress.clone());
        run_sync_plan(
            Arc::clone(state),
            job.clone(),
            request,
            SideConnection {
                config: source.config.clone(),
                password: source.password.clone(),
            },
            SideConnection {
                config: target.config.clone(),
                password: target.password.clone(),
            },
            spec,
        )
        .await;
        let snapshot = job.snapshot();
        assert_eq!(snapshot.status, "done", "{snapshot:?}");
        assert!(snapshot.error.is_none(), "{:?}", snapshot.error);
        let plan_id = snapshot.plan_id.clone().expect("a finished scan caches its plan");
        let plan = state.sync_plans.get(&plan_id).unwrap().plan.clone();
        (snapshot, plan)
    }

    /// Runs the selected plan ids through the same transactional batch `sync_apply` uses.
    async fn live_apply(
        state: &Arc<AppState>,
        target: &SideConnection,
        schema: &str,
        plan_id: &str,
        ids: &[usize],
    ) -> Vec<crate::models::ExecuteResult> {
        use crate::models::SqlBatchRequest;

        let cached = state.sync_plans.get(plan_id).unwrap();
        let mut picked = ids
            .iter()
            .filter_map(|id| cached.statements.get(id).cloned())
            .collect::<Vec<_>>();
        assert_eq!(picked.len(), ids.len(), "every id must belong to the plan");
        picked.sort_by_key(|entry| match entry.kind {
            "delete" => 2,
            "update" => 1,
            _ => 0,
        });
        let operation_id = format!("live-apply-{}", uuid::Uuid::new_v4());
        let statements = picked
            .iter()
            .enumerate()
            .map(|(index, entry)| SqlRequest {
                connection_id: target.config.id.clone(),
                sql: entry.sql.clone(),
                params: entry.params.clone(),
                operation_id: format!("{operation_id}-{index}"),
                page_size: None,
                offset: None,
                schema: (!schema.is_empty()).then(|| schema.to_string()),
            })
            .collect::<Vec<_>>();
        state
            .sql
            .execute_batch(
                &target.config,
                target.password.clone(),
                SqlBatchRequest {
                    connection_id: target.config.id.clone(),
                    statements,
                    operation_id,
                    wrap_in_transaction: true,
                },
            )
            .await
            .expect("applying the reviewed plan")
    }

    /// End-to-end row diff against the throw-away MySQL/PostgreSQL servers: the plan is built by
    /// the real scanner, deletes arrive unchecked, and applying the reviewed statements converges
    /// the target onto the source.
    #[tokio::test(flavor = "multi_thread")]
    async fn live_sync_diffs_applies_and_converges() {
        use crate::live_tests::{config, live_count, live_execute, live_number, live_state, target};
        use serde_json::json;

        let (state, dir) = live_state("rowsync");
        let engines = [
            ("mysql", "LUXBASE_LIVE_MYSQL", Protocol::Mysql, "shop"),
            ("postgres", "LUXBASE_LIVE_POSTGRES", Protocol::Postgres, "shop"),
        ];
        let expected = engines.iter().filter(|(_, variable, ..)| std::env::var(variable).is_ok()).count();
        let mut ran = 0_usize;
        for (tag, variable, protocol, schema) in engines {
            let Some(server) = target(variable) else { continue };
            ran += 1;
            let password = server.password.clone();
            let source = SideConnection {
                config: config(&format!("live-sync-src-{tag}"), tag, &server, json!({})),
                password: password.clone(),
            };
            let sink = SideConnection {
                config: config(&format!("live-sync-dst-{tag}"), tag, &server, json!({})),
                password: password.clone(),
            };
            let name = |table: &str| crate::transfer::qualified_table(protocol, schema, table);
            let (int_type, text16, numeric, text64) = match protocol {
                Protocol::Mysql => ("BIGINT", "VARCHAR(16)", "DECIMAL(12,2)", "VARCHAR(64)"),
                _ => ("bigint", "varchar(16)", "numeric(12,2)", "varchar(64)"),
            };

            for table in ["luxbase_sync_src", "luxbase_sync_dst"] {
                live_execute(&state, &sink.config, &password, &format!("DROP TABLE IF EXISTS {}", name(table)))
                    .await;
            }
            live_execute(
                &state,
                &source.config,
                &password,
                &format!(
                    "CREATE TABLE {} (id {int_type} NOT NULL PRIMARY KEY, status {text16} NOT NULL, \
                     amount {numeric} NULL, src_note {text64} NULL)",
                    name("luxbase_sync_src")
                ),
            )
            .await;
            live_execute(
                &state,
                &sink.config,
                &password,
                &format!(
                    "CREATE TABLE {} (id {int_type} NOT NULL PRIMARY KEY, status {text16} NOT NULL, \
                     amount {numeric} NULL)",
                    name("luxbase_sync_dst")
                ),
            )
            .await;
            // Source rows 1..=100; rows 11..=27 are the ones only the source has changed.
            let values = (1..=100)
                .map(|id| {
                    format!(
                        "({}, '{}', {}, {})",
                        id,
                        if (11..=27).contains(&id) { "shipped" } else { "paid" },
                        id as f64 * 1.5,
                        if id % 5 == 0 { format!("'仅源表 {id}'") } else { "NULL".to_string() }
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            live_execute(
                &state,
                &source.config,
                &password,
                &format!("INSERT INTO {} (id, status, amount, src_note) VALUES {values}", name("luxbase_sync_src")),
            )
            .await;
            // The target is missing 81..=100, is unchanged where 11..=27 differ, and keeps 20 extras.
            let values = (1..=80)
                .chain(201..=220)
                .map(|id| format!("({id}, 'paid', {})", id as f64 * 1.5))
                .collect::<Vec<_>>()
                .join(", ");
            live_execute(
                &state,
                &sink.config,
                &password,
                &format!("INSERT INTO {} (id, status, amount) VALUES {values}", name("luxbase_sync_dst")),
            )
            .await;

            let (progress, plan) =
                live_plan(&state, &source, &sink, schema, "luxbase_sync_src", "luxbase_sync_dst").await;
            assert_eq!((progress.source_rows, progress.target_rows), (100, 100));
            assert_eq!((progress.inserts, progress.updates, progress.deletes, progress.identical), (20, 17, 20, 63));
            assert_eq!(progress.phase, "finished");
            assert_eq!(plan.key_columns, vec!["id".to_string()]);
            assert_eq!(
                plan.copied_columns,
                vec!["id".to_string(), "status".to_string(), "amount".to_string()]
            );
            assert_eq!(plan.skipped_columns, vec!["src_note".to_string()]);
            assert!(plan.notes.iter().any(|note| note.contains("src_note") && note.contains("结构比较")), "{:?}", plan.notes);
            let count = |kind: &str| plan.operations.iter().filter(|op| op.kind == kind).count();
            assert_eq!((count("insert"), count("update"), count("delete")), (20, 17, 20));
            assert_eq!(plan.operations.len(), 57);
            // Deletes are the only unchecked-by-default rows, and the preview never runs.
            for operation in &plan.operations {
                assert_eq!(
                    operation.selected,
                    operation.kind != "delete",
                    "{} {} must follow the delete default",
                    operation.kind,
                    operation.key
                );
                assert!(operation.preview_sql.contains("luxbase_sync"), "{}", operation.preview_sql);
            }
            assert!(
                plan.operations
                    .iter()
                    .any(|op| op.kind == "update" && op.columns == vec!["status".to_string()]),
                "only the changed column is listed: {:?}",
                plan.operations
            );

            let ids = plan
                .operations
                .iter()
                .filter(|op| op.selected)
                .map(|op| op.id)
                .collect::<Vec<_>>();
            let applied = live_apply(&state, &sink, schema, &progress.plan_id.clone().unwrap(), &ids).await;
            assert_eq!(applied.len(), 37);
            assert!(applied.iter().all(|entry| entry.rows_affected == 1), "{applied:?}");
            assert_eq!(live_count(&state, &sink.config, &password, &name("luxbase_sync_dst")).await, 120);
            assert_eq!(
                live_number(
                    &state,
                    &sink.config,
                    &password,
                    &format!("SELECT COUNT(*) FROM {} WHERE status = 'shipped'", name("luxbase_sync_dst"))
                )
                .await,
                17.0
            );

            // Re-diffing shows only the 20 target-only rows left, still as unchecked deletes.
            let (second, second_plan) =
                live_plan(&state, &source, &sink, schema, "luxbase_sync_src", "luxbase_sync_dst").await;
            assert_eq!((second.inserts, second.updates, second.deletes), (0, 0, 20));
            assert_eq!((second.source_rows, second.target_rows, second.identical), (100, 120, 100));
            assert!(second_plan.operations.iter().all(|op| op.kind == "delete" && !op.selected));
            let deletes = second_plan
                .operations
                .iter()
                .map(|op| op.id)
                .collect::<Vec<_>>();
            let applied = live_apply(&state, &sink, schema, &second.plan_id.clone().unwrap(), &deletes).await;
            assert_eq!(applied.len(), 20);
            assert_eq!(live_count(&state, &sink.config, &password, &name("luxbase_sync_dst")).await, 100);

            // Converged: a third scan finds nothing left to write.
            let (third, third_plan) =
                live_plan(&state, &source, &sink, schema, "luxbase_sync_src", "luxbase_sync_dst").await;
            assert_eq!(third.identical, 100);
            assert_eq!((third.inserts, third.updates, third.deletes), (0, 0, 0));
            assert!(third_plan.operations.is_empty(), "{:?}", third_plan.operations);

            for table in ["luxbase_sync_src", "luxbase_sync_dst"] {
                live_execute(&state, &sink.config, &password, &format!("DROP TABLE IF EXISTS {}", name(table))).await;
            }
        }
        state.sql.close_all().await;
        let _ = std::fs::remove_dir_all(&dir);
        assert_eq!(ran, expected, "每一台已配置的引擎都要跑完差异检查");
    }
}

#[cfg(test)]
mod group_tests {
    use super::*;

    fn stmt(id: usize, kind: &'static str, sql: &str, params: Vec<crate::models::ParamValue>) -> PlannedStatement {
        PlannedStatement { id, kind, sql: sql.into(), params }
    }

    #[test]
    fn groups_inserts_and_merges_deletes_with_renumbered_placeholders() {
        let value = crate::models::ParamValue::I64;
        let selected = vec![
            stmt(0, "insert", "INSERT INTO t (a,b) VALUES ($1,$2)", vec![value(0), value(1)]),
            stmt(1, "insert", "INSERT INTO t (a,b) VALUES ($1,$2)", vec![value(2), value(3)]),
            stmt(2, "insert", "INSERT INTO t (a,b) VALUES ($1,$2)", vec![value(4), value(5)]),
            stmt(10, "delete", "DELETE FROM t WHERE id = $1", vec![value(10)]),
            stmt(11, "delete", "DELETE FROM t WHERE id = $1", vec![value(11)]),
        ];
        let grouped = group_apply_statements(Protocol::Postgres, &selected);
        let insert = grouped.iter().find(|entry| entry.kind == "insert").expect("grouped insert");
        assert_eq!(insert.params.len(), 6);
        assert!(insert.sql.contains("$5") && insert.sql.contains("$6"), "{0}", insert.sql);
        let delete = grouped.iter().find(|entry| entry.kind == "delete").expect("merged delete");
        assert!(delete.sql.contains(" OR "), "{0}", delete.sql);
        assert!(delete.sql.contains("$1") && delete.sql.contains("$2"), "{0}", delete.sql);
        assert_eq!(delete.params.len(), 2);
    }
}


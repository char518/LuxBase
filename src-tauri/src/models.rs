use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

pub type ConnectionId = String;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Protocol {
    Mysql,
    Postgres,
    Sqlite,
    Redis,
    Elasticsearch,
}

impl Protocol {
    pub fn family(self) -> DriverFamily {
        match self {
            Self::Mysql | Self::Postgres | Self::Sqlite => DriverFamily::Sql,
            Self::Redis => DriverFamily::Kv,
            Self::Elasticsearch => DriverFamily::Search,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Mysql => "mysql",
            Self::Postgres => "postgres",
            Self::Sqlite => "sqlite",
            Self::Redis => "redis",
            Self::Elasticsearch => "elasticsearch",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DriverFamily {
    Sql,
    Kv,
    Search,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PoolConfig {
    pub min_idle: u32,
    pub max_size: u32,
    pub connection_timeout_ms: u64,
    pub idle_timeout_ms: u64,
    pub max_lifetime_ms: u64,
    pub health_check_timeout_ms: u64,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            min_idle: 2,
            max_size: 20,
            connection_timeout_ms: 10_000,
            idle_timeout_ms: 300_000,
            max_lifetime_ms: 1_800_000,
            health_check_timeout_ms: 3_000,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SshConfig {
    pub host: String,
    pub port: u16,
    pub username: String,
    pub private_key_path: Option<String>,
    pub known_hosts_path: Option<String>,
    pub strict_host_key_checking: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SentinelNode {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TlsMode {
    Disabled,
    Preferred,
    Required,
    VerifyFull,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RedisMode {
    Standalone,
    Sentinel,
    Cluster,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionConfig {
    pub id: ConnectionId,
    pub name: String,
    pub protocol: Protocol,
    #[serde(default)]
    pub host: String,
    #[serde(default)]
    pub port: u16,
    #[serde(default)]
    pub username: String,
    #[serde(default)]
    pub database: String,
    #[serde(default)]
    pub file_path: Option<String>,
    #[serde(default)]
    pub tls_mode: Option<TlsMode>,
    #[serde(default)]
    pub ca_cert_path: Option<String>,
    #[serde(default)]
    pub client_cert_path: Option<String>,
    #[serde(default)]
    pub ssh: Option<SshConfig>,
    #[serde(skip)]
    pub ssh_passphrase: Option<String>,
    #[serde(skip)]
    pub ssh_password: Option<String>,
    #[serde(default)]
    pub redis_mode: Option<RedisMode>,
    #[serde(default)]
    pub sentinels: Vec<SentinelNode>,
    #[serde(default)]
    pub sentinel_master_name: Option<String>,
    #[serde(default)]
    pub cluster_nodes: Vec<String>,
    #[serde(default)]
    pub redis_db: u32,
    /// Elasticsearch authentication: `none`, `basic` (username + password) or `apiKey`.
    /// The secret itself lives in the vault under the connection password account.
    #[serde(default)]
    pub es_auth_mode: Option<String>,
    #[serde(default)]
    pub pool: PoolConfig,
    #[serde(default = "default_query_timeout")]
    pub query_timeout_ms: u64,
}

fn default_query_timeout() -> u64 {
    60_000
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SavedConnection {
    pub id: ConnectionId,
    pub name: String,
    pub protocol: Protocol,
    pub host_summary: String,
    pub database: String,
    pub updated_at: String,
    pub has_ssh: bool,
    pub tls_enabled: bool,
    #[serde(default)]
    pub redis_db: u32,
    #[serde(default)]
    pub redis_mode: Option<RedisMode>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionTestResult {
    pub ok: bool,
    pub latency_ms: u64,
    pub server_version: Option<String>,
    pub error_class: Option<ErrorClass>,
    pub message: Option<String>,
}

/* ---------- batch connection import / export ---------- */

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionExportEntry {
    pub config: ConnectionConfig,
    /// Passphrase-encrypted envelope of {password, sshPassphrase, sshPassword}; absent when the
    /// export was created without secrets.
    #[serde(default)]
    pub secrets: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionExportFile {
    pub format: String,
    pub version: u32,
    pub exported_at: String,
    pub includes_secrets: bool,
    pub connections: Vec<ConnectionExportEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionImportPreviewEntry {
    pub id: String,
    pub name: String,
    pub protocol: Protocol,
    pub host_summary: String,
    pub has_secrets: bool,
    pub exists: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionImportPreview {
    pub includes_secrets: bool,
    pub entries: Vec<ConnectionImportPreviewEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionImportResult {
    pub imported: usize,
    pub skipped: usize,
    pub failed: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DriverMetadata {
    pub driver: String,
    pub server_version: Option<String>,
    pub database: Option<String>,
    pub capabilities: DriverCapabilities,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DriverCapabilities {
    pub sql: bool,
    pub transactions: bool,
    pub metadata: bool,
    pub editable_grid: bool,
    pub schema_compare: bool,
    pub users_permissions: bool,
    pub scan_keyspace: bool,
    pub typed_value_editors: bool,
    pub raw_command_console: bool,
    pub unsupported_reason: Option<String>,
}

impl DriverCapabilities {
    pub fn for_sql() -> Self {
        Self {
            sql: true,
            transactions: true,
            metadata: true,
            editable_grid: true,
            schema_compare: true,
            users_permissions: true,
            scan_keyspace: false,
            typed_value_editors: false,
            raw_command_console: false,
            unsupported_reason: None,
        }
    }

    pub fn for_redis() -> Self {
        Self {
            sql: false,
            transactions: false,
            metadata: false,
            editable_grid: false,
            schema_compare: false,
            users_permissions: false,
            scan_keyspace: true,
            typed_value_editors: true,
            raw_command_console: true,
            unsupported_reason: Some(
                "Redis is a key-value protocol; SQL grids, table design and schema comparison do not apply."
                    .into(),
            ),
        }
    }

    pub fn for_elasticsearch() -> Self {
        Self {
            sql: false,
            transactions: false,
            metadata: false,
            editable_grid: false,
            schema_compare: false,
            users_permissions: false,
            scan_keyspace: false,
            typed_value_editors: false,
            raw_command_console: true,
            unsupported_reason: Some(
                "Elasticsearch is a search protocol; SQL grids, transactions and schema comparison do not apply. Use the index browser, document view and Query DSL / REST console."
                    .into(),
            ),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PoolMetrics {
    pub active: u64,
    pub idle: u64,
    pub total: u64,
    pub waiters: u64,
    pub timeouts: u64,
    pub max_size: u32,
    pub acquired_total: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum ParamValue {
    Null,
    Bool(bool),
    I64(i64),
    U64(u64),
    F64(f64),
    Text(String),
    Bytes(Vec<u8>),
    Json(Value),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SqlRequest {
    pub connection_id: ConnectionId,
    pub sql: String,
    #[serde(default)]
    pub params: Vec<ParamValue>,
    pub operation_id: String,
    #[serde(default)]
    pub page_size: Option<u32>,
    #[serde(default)]
    pub offset: Option<u64>,
    /// Database (MySQL) or schema search path (PostgreSQL) the statement runs against.
    #[serde(default)]
    pub schema: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SqlBatchRequest {
    pub connection_id: ConnectionId,
    pub statements: Vec<SqlRequest>,
    pub operation_id: String,
    #[serde(default = "default_true")]
    pub wrap_in_transaction: bool,
}

fn default_true() -> bool {
    true
}

/// A multi-statement script run on one session. Each statement yields its own result set or
/// affected-row count, the way the query editor shows one result tab per statement.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SqlScriptRequest {
    pub connection_id: ConnectionId,
    pub statements: Vec<SqlRequest>,
    pub operation_id: String,
    #[serde(default)]
    pub page_size: Option<u32>,
    /// MySQL DDL commits implicitly, so a script containing it can only run statement by
    /// statement; the UI must obtain explicit consent before sending this flag.
    #[serde(default)]
    pub allow_partial_commit: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScriptStatementResult {
    pub index: usize,
    pub returns_rows: bool,
    pub columns: Vec<ResultColumn>,
    pub rows: Vec<Vec<Value>>,
    pub has_more: bool,
    pub page_size: u32,
    pub rows_affected: u64,
    pub last_insert_id: Option<i64>,
    pub duration_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScriptFailure {
    pub index: usize,
    pub class: ErrorClass,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SqlScriptResult {
    pub operation_id: String,
    pub results: Vec<ScriptStatementResult>,
    pub transactional: bool,
    pub committed: bool,
    pub failure: Option<ScriptFailure>,
    pub duration_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SqlExportRequest {
    pub connection_id: String,
    pub schema: String,
    pub format: String,
    pub path: String,
    pub table: Option<String>,
    pub operation_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SqlQueryExportRequest {
    pub request: SqlRequest,
    pub format: String,
    pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SqlExportResult {
    pub rows_exported: u64,
    pub tables_exported: u64,
    pub output_path: String,
    pub snapshot_isolated: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SqlImportRequest {
    pub connection_id: ConnectionId,
    pub format: String,
    pub path: String,
    pub schema: Option<String>,
    pub table: String,
    pub operation_id: String,
    #[serde(default = "default_import_batch_size")]
    pub batch_size: u32,
}

fn default_import_batch_size() -> u32 {
    2_000
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecuteResult {
    pub operation_id: String,
    pub rows_affected: u64,
    pub last_insert_id: Option<i64>,
    pub duration_ms: u64,
    pub committed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryResult {
    pub operation_id: String,
    pub columns: Vec<ResultColumn>,
    pub rows: Vec<Vec<Value>>,
    pub rows_affected: u64,
    pub page: ResultPage,
    pub duration_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResultColumn {
    pub name: String,
    pub type_name: String,
    pub nullable: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResultPage {
    pub offset: u64,
    pub limit: u32,
    pub has_more: bool,
    pub streamed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MetadataRequest {
    pub connection_id: ConnectionId,
    pub schema: Option<String>,
    pub table: Option<String>,
    #[serde(default)]
    pub include_tables: bool,
    /// Columns, indexes and foreign keys for every table of `schema` (used by schema compare).
    #[serde(default)]
    pub include_columns: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MetadataSnapshot {
    pub databases: Vec<DatabaseMetadata>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DatabaseMetadata {
    pub name: String,
    pub tables: Vec<TableMetadata>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TableMetadata {
    pub schema: String,
    pub name: String,
    pub table_type: String,
    pub columns: Vec<ColumnMetadata>,
    pub indexes: Vec<IndexMetadata>,
    pub foreign_keys: Vec<ForeignKeyMetadata>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ColumnMetadata {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    pub default_value: Option<String>,
    pub is_primary_key: bool,
    pub ordinal: i64,
    #[serde(default)]
    pub comment: Option<String>,
    /// Server-specific extras such as MySQL `auto_increment` or a PostgreSQL identity column.
    #[serde(default)]
    pub extra: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IndexMetadata {
    pub name: String,
    pub columns: Vec<String>,
    pub unique: bool,
}

/// One row of the object list: catalog statistics the server keeps for a table or view.
/// Row counts and sizes are the server's estimates, not exact counts.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ObjectStat {
    pub name: String,
    pub object_type: String,
    pub rows: Option<i64>,
    pub data_bytes: Option<i64>,
    pub index_bytes: Option<i64>,
    pub engine: Option<String>,
    pub auto_increment: Option<i64>,
    pub collation: Option<String>,
    pub created: Option<String>,
    pub updated: Option<String>,
    pub comment: Option<String>,
    pub owner: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ForeignKeyMetadata {
    pub name: String,
    pub columns: Vec<String>,
    pub referenced_table: String,
    pub referenced_columns: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RedisScanRequest {
    pub connection_id: ConnectionId,
    pub cursor: u64,
    pub pattern: String,
    pub count: u32,
    pub type_filter: Option<String>,
    pub db: u32,
    pub operation_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RedisKeySummary {
    pub key: String,
    /// Present only when the raw key is not valid UTF-8; carries the exact bytes (base64) so
    /// get/set/del can address binary keys that the lossy `key` display cannot round-trip.
    #[serde(default)]
    pub key_base64: Option<String>,
    pub key_type: String,
    pub ttl_seconds: i64,
    pub size_bytes: Option<u64>,
    pub length: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RedisScanResult {
    pub cursor: u64,
    pub keys: Vec<RedisKeySummary>,
    pub scanned: u64,
    pub duration_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RedisValue {
    pub key: String,
    pub key_type: String,
    pub ttl_seconds: i64,
    pub encoding: Option<String>,
    pub size_bytes: Option<u64>,
    /// Exact bytes (base64) when a string value is not valid UTF-8, so the UI can offer
    /// alternative decodings (GBK/Latin-1) or a hex view instead of mojibake.
    #[serde(default)]
    pub raw_base64: Option<String>,
    pub value: RedisValueBody,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
pub enum RedisValueBody {
    String(String),
    Hash(BTreeMap<String, String>),
    List(Vec<IndexedString>),
    Set(Vec<String>),
    ZSet(Vec<ScoredMember>),
    Stream(Vec<StreamEntry>),
    Unsupported(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IndexedString {
    pub index: u64,
    pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScoredMember {
    pub member: String,
    pub score: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StreamEntry {
    pub id: String,
    pub fields: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RedisKeyspaceEntry {
    pub db: u32,
    pub keys: u64,
    pub expires: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RedisInfo {
    pub raw: String,
    pub memory_used_bytes: Option<u64>,
    pub memory_peak_bytes: Option<u64>,
    pub connected_clients: Option<u64>,
    pub hit_rate: Option<f64>,
    pub replication_role: Option<String>,
    pub connected_replicas: Option<u64>,
    pub cluster_enabled: bool,
    #[serde(default)]
    pub keyspace: Vec<RedisKeyspaceEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RedisCommandRequest {
    pub connection_id: ConnectionId,
    pub command: String,
    pub args: Vec<String>,
    pub operation_id: String,
    #[serde(default)]
    pub confirmed_dangerous: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RedisCommandResult {
    pub operation_id: String,
    pub protocol: String,
    pub value: Value,
    pub duration_ms: u64,
}

/* ---------- Elasticsearch (search driver family) ---------- */

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EsClusterInfo {
    pub cluster_name: Option<String>,
    pub version: Option<String>,
    pub distribution: Option<String>,
    pub status: Option<String>,
    pub number_of_nodes: Option<u64>,
    pub active_shards: Option<u64>,
    pub raw: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EsIndex {
    pub name: String,
    pub health: Option<String>,
    pub status: Option<String>,
    pub docs_count: Option<u64>,
    pub docs_deleted: Option<u64>,
    pub store_bytes: Option<u64>,
    pub primary_shards: Option<u32>,
    pub replica_shards: Option<u32>,
    #[serde(default)]
    pub aliases: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EsAlias {
    pub alias: String,
    pub index: String,
    pub is_write_index: bool,
    pub filter: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EsSearchRequest {
    pub connection_id: ConnectionId,
    pub index: String,
    /// Query DSL body (query / aggs / sort / _source). `from` and `size` are applied by the driver.
    #[serde(default)]
    pub body: Value,
    #[serde(default)]
    pub from: u64,
    #[serde(default = "default_es_size")]
    pub size: u32,
    pub operation_id: String,
}

fn default_es_size() -> u32 {
    50
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EsHit {
    pub id: String,
    pub index: String,
    pub score: Option<f64>,
    pub source: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EsSearchResult {
    pub took_ms: u64,
    pub total: u64,
    pub total_relation: String,
    pub hits: Vec<EsHit>,
    pub aggregations: Option<Value>,
    pub duration_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EsDocument {
    pub index: String,
    pub id: String,
    pub found: bool,
    pub version: Option<i64>,
    pub source: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EsRestRequest {
    pub connection_id: ConnectionId,
    pub method: String,
    pub path: String,
    #[serde(default)]
    pub body: Option<Value>,
    pub operation_id: String,
    #[serde(default)]
    pub confirmed_dangerous: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EsRestResult {
    pub status: u16,
    pub body: Value,
    pub duration_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ErrorClass {
    ConnectionFailed,
    PermissionDenied,
    SyntaxError,
    Timeout,
    Cancelled,
    ProtocolUnsupported,
    Validation,
    Conflict,
    Internal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DiagnosedError {
    pub class: ErrorClass,
    pub message: String,
    pub detail: Option<String>,
    pub retryable: bool,
    pub code: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AuditEntry {
    pub id: i64,
    pub occurred_at: String,
    pub connection_id: Option<String>,
    pub connection_label: Option<String>,
    pub protocol: Option<String>,
    pub event_kind: String,
    pub command_summary: String,
    pub affected_rows: Option<i64>,
    pub outcome: String,
    pub metadata_json: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryHistoryEntry {
    pub id: i64,
    pub connection_id: String,
    pub sql_text: String,
    pub status: String,
    pub duration_ms: Option<i64>,
    pub rows_affected: Option<i64>,
    pub error_class: Option<String>,
    pub executed_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryDraft {
    pub id: i64,
    pub connection_id: String,
    pub name: String,
    pub sql_text: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AppBootstrap {
    pub connections: Vec<SavedConnection>,
    pub recent_history: Vec<QueryHistoryEntry>,
    pub recent_audit: Vec<AuditEntry>,
    pub protocols: Vec<ProtocolDescriptor>,
    pub security: SecurityState,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProtocolDescriptor {
    pub id: Protocol,
    pub family: DriverFamily,
    pub label: String,
    pub supports: Vec<String>,
    pub unsupported: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SecurityState {
    pub local_vault_available: bool,
    pub encryption_at_rest: bool,
    pub audit_append_only: bool,
    pub message: String,
}

/// Outcome of the one-time, user-triggered recovery of credentials that an older build left in the
/// operating system keychain. Carries counts only — never a secret or an account's value.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LegacyImportReport {
    pub config_key_imported: bool,
    pub imported: usize,
    pub kept_existing: usize,
    pub missing: usize,
    pub access_denied: usize,
    pub keychain_cleanup_failed: usize,
}

/* ---------- data transfer (connection -> connection) ---------- */

/// What happens to rows that already live in the target table.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TransferMode {
    /// Keep existing target rows and add the copied rows.
    Append,
    /// Empty the existing target table first, then copy.
    Truncate,
    /// Drop the existing target table and rebuild it from the source structure.
    Recreate,
}

impl Default for TransferMode {
    fn default() -> Self {
        Self::Append
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransferTableRef {
    pub schema: String,
    pub table: String,
    /// Copy into a differently named target table; defaults to `table`.
    #[serde(default)]
    pub target_table: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransferRequest {
    pub source_connection_id: String,
    pub target_connection_id: String,
    /// Database (MySQL) / schema (PostgreSQL) the target tables live in; ignored for SQLite.
    #[serde(default)]
    pub target_schema: String,
    pub tables: Vec<TransferTableRef>,
    #[serde(default)]
    pub mode: TransferMode,
    /// Build the target table from the source structure when it does not exist yet.
    #[serde(default)]
    pub create_missing: bool,
    /// Also recreate secondary indexes; foreign keys, triggers and partitions are never copied.
    #[serde(default)]
    pub create_indexes: bool,
    /// Bulk-load speed: drop secondary indexes during the copy and recreate them afterwards.
    #[serde(default)]
    pub disable_indexes: bool,
    /// Bulk-load speed: disable unique / foreign-key checks for MySQL and SQLite during the copy.
    #[serde(default)]
    pub disable_checks: bool,
    #[serde(default)]
    pub batch_size: Option<u32>,
    pub operation_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransferTableProgress {
    pub source_schema: String,
    pub source_table: String,
    pub target_table: String,
    /// `pending`, `running`, `done`, `failed` or `skipped`.
    pub status: String,
    pub rows_read: u64,
    pub rows_written: u64,
    /// Estimated source row count, used for percentage / ETA; None if not yet counted.
    #[serde(default)]
    pub total_rows: Option<u64>,
    /// `created`, `truncated`, `recreated` or `append` once the table starts copying.
    pub action: Option<String>,
    pub duration_ms: u64,
    pub notes: Vec<String>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransferProgress {
    pub operation_id: String,
    /// `running`, `done`, `failed` or `cancelled`.
    pub status: String,
    pub phase: String,
    pub source_name: String,
    pub target_name: String,
    pub source_protocol: String,
    pub target_protocol: String,
    pub rows_read: u64,
    pub rows_written: u64,
    /// Sum of estimated source row counts, used for overall percentage / ETA.
    #[serde(default)]
    pub total_rows: u64,
    pub tables_total: usize,
    pub tables_done: usize,
    pub elapsed_ms: u64,
    pub tables: Vec<TransferTableProgress>,
    pub error: Option<String>,
}

/* ---------- row-level data synchronisation ---------- */

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncSide {
    pub connection_id: String,
    pub connection_name: String,
    pub protocol: String,
    pub schema: String,
    pub table: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncPlanRequest {
    pub source_connection_id: String,
    pub source_schema: String,
    pub source_table: String,
    pub target_connection_id: String,
    pub target_schema: String,
    pub target_table: String,
    /// Rows scanned per side before the run is refused; guards against unbounded work.
    #[serde(default = "default_sync_scan_limit")]
    pub max_rows: u64,
    pub operation_id: String,
}

fn default_sync_scan_limit() -> u64 {
    200_000
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncProgress {
    pub operation_id: String,
    /// `running`, `done`, `failed` or `cancelled`.
    pub status: String,
    /// `source`, `target` or `finished`.
    pub phase: String,
    pub source: SyncSide,
    pub target: SyncSide,
    pub source_rows: u64,
    pub target_rows: u64,
    pub identical: u64,
    pub inserts: u64,
    pub updates: u64,
    pub deletes: u64,
    pub elapsed_ms: u64,
    pub plan_id: Option<String>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncOperation {
    /// Stable index into the cached plan; the apply request selects by this id.
    pub id: usize,
    /// `insert`, `update` or `delete`.
    pub kind: String,
    pub key: String,
    /// Changed column names for an update, otherwise empty.
    pub columns: Vec<String>,
    /// Literal rendering for review only — execution always binds parameters.
    pub preview_sql: String,
    pub selected: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncPlan {
    pub plan_id: String,
    pub source: SyncSide,
    pub target: SyncSide,
    pub key_columns: Vec<String>,
    pub copied_columns: Vec<String>,
    /// Columns that exist on only one side and therefore could not be compared.
    pub skipped_columns: Vec<String>,
    pub source_rows: u64,
    pub target_rows: u64,
    pub identical: u64,
    pub elapsed_ms: u64,
    pub operations: Vec<SyncOperation>,
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncApplyRequest {
    pub plan_id: String,
    pub operation_ids: Vec<usize>,
    pub operation_id: String,
    /// Required when the selection deletes rows.
    #[serde(default)]
    pub confirmed_dangerous: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncApplyResult {
    pub operation_id: String,
    pub applied: usize,
    pub inserts: usize,
    pub updates: usize,
    pub deletes: usize,
    pub rows_affected: u64,
    pub duration_ms: u64,
    pub committed: bool,
}

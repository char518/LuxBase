import { existsSync, readFileSync, statSync } from "node:fs";
import { resolve } from "node:path";
import { spawnSync } from "node:child_process";

const root = resolve(import.meta.dirname, "..");
const errors = [];
const warnings = [];

function exists(path) {
  return existsSync(resolve(root, path));
}

function read(path) {
  return readFileSync(resolve(root, path), "utf8");
}

function nonTrivial(path, minimum = 100) {
  if (!exists(path)) {
    errors.push(`${path} is missing`);
    return false;
  }
  if (statSync(resolve(root, path)).size < minimum) {
    errors.push(`${path} is unexpectedly small`);
    return false;
  }
  return true;
}

// Frontend integration
for (const file of ["src/main.tsx", "src/App.tsx", "src/styles.css", "ui/index.html", "ui/app.js", "ui/styles.css"]) {
  nonTrivial(file);
}
for (const file of ["ui/theme-preload.js", "ui/theme-dark.css", "ui/theme-dark-auto.css", "scripts/build-dark-theme.js", "ui/sql-lint.js", "ui/sql-lint.d.ts"]) {
  nonTrivial(file, 200);
}
if (exists("package.json")) {
  const pkg = JSON.parse(read("package.json"));
  for (const dependency of ["react", "react-dom", "monaco-editor", "tailwindcss", "@tauri-apps/api"]) {
    const present = pkg.dependencies?.[dependency] || pkg.devDependencies?.[dependency];
    if (!present) errors.push(`package.json is missing ${dependency}`);
  }
  if (!pkg.scripts?.["install:latest"]) errors.push("package.json must expose a clean latest-version installer");
}
if (nonTrivial("scripts/install-latest.js", 500)) {
  const installer = read("scripts/install-latest.js");
  for (const token of [
    "com.luxbase.app",
    'resolve(homedir(), "Applications")',
    "LUXBASE_INSTALL_DIR",
    "Refusing to remove",
    "scripts/build-macos-app.js",
    "codesign",
    "lsregister",
  ]) {
    if (!installer.includes(token)) errors.push(`Clean installer is missing ${token}`);
  }
}
if (exists("scripts/build-macos-app.js")) {
  const macBuild = read("scripts/build-macos-app.js");
  if (!macBuild.includes("const destination = resolve(root, appName);") || macBuild.includes("const destinations = [")) {
    errors.push("macOS builds must create one canonical application bundle");
  }
}
// The dependency-free fallback and React shell must expose the same core screens.
for (const file of ["ui/index.html", "ui/app.js"]) {
  if (exists(file)) {
    const body = read(file);
    for (const token of file.endsWith("index.html")
      ? ["view-sql", "view-redis", "view-compare", "view-console", "view-audit", "SCAN"]
      : ["SCAN", "FLUSHALL", "app-shell", "showWorkspace"]) {
      if (!body.includes(token)) warnings.push(`${file} does not contain ${token}`);
    }
  }
}

// Rust driver boundaries
const requiredRust = [
  "src-tauri/Cargo.toml",
  "src-tauri/tauri.conf.json",
  "src-tauri/src/main.rs",
  "src-tauri/src/lib.rs",
  "src-tauri/src/models.rs",
  "src-tauri/src/error.rs",
  "src-tauri/src/security.rs",
  "src-tauri/src/storage.rs",
  "src-tauri/src/app_state.rs",
  "src-tauri/src/commands.rs",
  "src-tauri/src/drivers/sql/trait.rs",
  "src-tauri/src/drivers/sql/sqlx_driver.rs",
  "src-tauri/src/drivers/sql/sqlite_driver.rs",
  "src-tauri/src/drivers/sql/import.rs",
  "src-tauri/src/drivers/sql/export.rs",
  "src-tauri/src/drivers/kv/trait.rs",
  "src-tauri/src/drivers/kv/redis_driver.rs",
];
for (const file of requiredRust) nonTrivial(file, 20);

if (exists("src-tauri/src/drivers/sql/trait.rs")) {
  const source = read("src-tauri/src/drivers/sql/trait.rs");
  for (const method of ["connect", "execute", "execute_batch", "fetch", "cancel", "metadata", "close", "stream_import", "export_snapshot"]) {
    if (!source.includes(`async fn ${method}`) && !source.includes(`fn ${method}`)) {
      errors.push(`SqlDriver trait is missing ${method}`);
    }
    if (method === "execute_batch" && !source.includes("async fn execute_batch")) {
      errors.push("SqlDriver trait must define executeBatch semantics safely");
    }
  }
}
if (exists("src-tauri/src/drivers/kv/trait.rs")) {
  const source = read("src-tauri/src/drivers/kv/trait.rs");
  for (const method of ["connect", "scan", "get", "set", "del", "ttl", "key_type", "info", "cancel", "close"]) {
    if (!source.includes(`async fn ${method}`) && !source.includes(`fn ${method}`)) {
      errors.push(`KvDriver trait is missing ${method}`);
    }
  }
}
if (exists("src-tauri/src/drivers/kv/redis_driver.rs")) {
  const source = read("src-tauri/src/drivers/kv/redis_driver.rs");
  if (/redis::cmd\(\s*"KEYS"/.test(source)) errors.push("Redis driver must not issue KEYS commands");
  for (const token of ["SCAN", "connection_manager", "ClusterClient", "SENTINEL", "FLUSHALL", "SHUTDOWN"]) {
    if (!source.includes(token)) warnings.push(`Redis driver does not contain ${token}`);
  }
  for (const token of ["run_cancellable", "register_client_id(operation_id)", "query_ttl", "query_key_type"]) {
    if (!source.includes(token)) errors.push(`Redis cancellation path is missing ${token}`);
  }
  for (const signature of [
    "async fn get(&self, key: &[u8], db: u32, operation_id: &str)",
    "async fn del(",
    "async fn ttl(&self, key: &[u8], db: u32, operation_id: &str)",
    "async fn key_type(&self, key: &[u8], db: u32, operation_id: &str)",
    "async fn dbsize(&self, db: u32, operation_id: &str)",
    "async fn info(&self, operation_id: &str)",
  ]) {
    if (!source.includes(signature)) errors.push(`Redis operation cancellation is missing ${signature}`);
  }
}
if (exists("src-tauri/src/drivers/sql/sqlite_driver.rs") && exists("src-tauri/src/drivers/sql/sqlx_driver.rs")) {
  const sqlite = read("src-tauri/src/drivers/sql/sqlite_driver.rs");
  const sqlx = read("src-tauri/src/drivers/sql/sqlx_driver.rs");
  if (!sqlite.includes("rusqlite")) errors.push("SQLite implementation does not use rusqlite");
  if (!sqlx.includes("sqlx")) errors.push("MySQL/PostgreSQL implementation does not use sqlx");
  if (!sqlite.includes("transaction") || !sqlx.includes("begin()")) warnings.push("Transaction safeguards need review");
  if (!sqlite.includes("token.cancel();") || !sqlite.includes("let _ = task.await;")) {
    errors.push("SQLite timeout/cancel must stop and reap the blocking worker");
  }
  for (const [name, source] of [["SQLite", sqlite], ["SQLx", sqlx]]) {
    if (!source.includes("async fn stream_import") || !source.includes("SqlImportSource::open") || !source.includes("next_batch(batch_size)")) {
      errors.push(`${name} driver must stream imports in bounded batches`);
    }
    if (!source.includes("transaction") || !source.includes(".commit()")) {
      errors.push(`${name} import must keep all batches in one transaction`);
    }
  }
}
if (exists("src-tauri/src/drivers/sql/import.rs")) {
  const importer = read("src-tauri/src/drivers/sql/import.rs");
  for (const token of ["BufReader", "read_line", "MAX_BIND_PARAMETERS", "safe_batch_size", "next_batch", "INSERT INTO", "quote_identifier"]) {
    if (!importer.includes(token)) errors.push(`Streaming SQL importer is missing ${token}`);
  }
}
if (exists("src-tauri/src/drivers/sql/export.rs")) {
  const exporter = read("src-tauri/src/drivers/sql/export.rs");
  for (const token of ["SqlSnapshotWriter", "database_scope", "begin_table", "write_row", "serde_json::to_writer", "INSERT INTO", "bytesBase64"]) {
    if (!exporter.includes(token)) errors.push(`Streaming SQL exporter is missing ${token}`);
  }
}
if (exists("src-tauri/src/drivers/sql/sqlx_driver.rs") && exists("src-tauri/src/drivers/sql/sqlite_driver.rs")) {
  const sqlx = read("src-tauri/src/drivers/sql/sqlx_driver.rs");
  const sqlite = read("src-tauri/src/drivers/sql/sqlite_driver.rs");
  if (!sqlx.includes("async fn export_snapshot") ||
      !sqlx.includes("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")) {
    errors.push("MySQL/PostgreSQL database export must run inside a repeatable-read snapshot");
  }
  if (!sqlite.includes("async fn export_snapshot") ||
      !sqlite.includes("transaction_with_behavior(TransactionBehavior::Deferred)")) {
    errors.push("SQLite database export must run inside one read transaction");
  }
}
if (exists("src-tauri/src/commands.rs")) {
  const commands = read("src-tauri/src/commands.rs");
  if ((commands.match(/pub struct IdentitySnapshot/g) || []).length !== 1) {
    errors.push("IdentitySnapshot must be declared exactly once");
  }
  if (!commands.includes('pub struct IdentitySnapshot') ||
      !commands.includes('#[derive(Debug, Clone, Serialize, Deserialize)]\n#[serde(rename_all = "camelCase")]\npub struct IdentitySnapshot')) {
    errors.push("IdentitySnapshot must have one clean derive declaration");
  }
  if (!commands.includes("reject_session_state_statement(&request.sql)?")) {
    errors.push("Single SQL entrypoints must reject pooled session/transaction state commands");
  }
  if (!commands.includes("pub async fn sql_drop_table") || !commands.includes("DROP TABLE {qualified}")) {
    errors.push("DROP TABLE must use a backend-controlled qualified identifier");
  }
  if (!commands.includes("summarize_redis_command") || !commands.includes("<redacted>")) {
    errors.push("Redis audit summaries must redact CONFIG and ACL secrets");
  }
  if (!commands.includes(".stream_import(") || !commands.includes("SqlImportRequest")) {
    errors.push("SQL import must delegate to the bounded-memory driver importer");
  }
  if (!commands.includes(".export_snapshot(") || !commands.includes("SqlExportRequest")) {
    errors.push("SQL export must delegate to the consistent-snapshot driver exporter");
  }
  if (commands.includes("SQL import files larger than 25 MB require the streaming importer")) {
    errors.push("SQL import must not keep the obsolete 25 MB fail-closed limit");
  }
  if (/\.get\(\s*&config,\s*password\.clone\(\),\s*&key\.key\s*\)/s.test(commands)) {
    errors.push("Redis export must pass an operation ID to the cancellation-aware KV get path");
  }
  if (!/\.get\(\s*&config,\s*password\.clone\(\),\s*&raw,\s*config\.redis_db,\s*&format!\(/s.test(commands)) {
    errors.push("Redis export key reads must use a cancellable operation ID");
  }
  for (const command of ["get_connection_config", "test_saved_connection", "delete_connection", "save_query_draft", "list_query_drafts", "sql_apply_ddl"]) {
    if (!commands.includes(`pub async fn ${command}`)) errors.push(`Backend command ${command} is missing`);
  }
  if (!commands.includes("MySQL DDL causes an implicit commit and cannot be atomic")) {
    errors.push("MySQL DDL must not be presented as an atomic transaction");
  }
}
if (exists("src-tauri/src/rowsync.rs")) {
  const source = read("src-tauri/src/rowsync.rs");
  for (const token of ["confirmed_dangerous", "MAX_PLAN_OPERATIONS", "wrap_in_transaction: true", "PLAN_TTL"]) {
    if (!source.includes(token)) errors.push(`Row sync safety is missing ${token}`);
  }
  if (!source.includes("if source_protocol != target_protocol")) {
    errors.push("Row sync must stay limited to same-engine comparisons");
  }
}
if (exists("src-tauri/src/drivers/kv/redis_driver.rs")) {
  const redis = read("src-tauri/src/drivers/kv/redis_driver.rs");
  for (const command of ["MULTI", "EXEC", "WATCH", "SELECT", "CLIENT", "SUBSCRIBE"]) {
    if (!redis.includes(`"${command}"`)) errors.push(`Redis raw console must isolate or reject session command ${command}`);
  }
  if (!redis.includes("Redis does not store empty hash, list, set or sorted-set keys")) {
    errors.push("Redis value replacement must reject silent deletion of empty collections");
  }
  if (!redis.includes("cluster_scan_urls") || !redis.includes("next_cluster_cursor")) {
    errors.push("Redis Cluster SCAN must traverse configured cluster nodes with opaque cursors");
  }
  if (!redis.includes("session_lock")) {
    errors.push("Redis shared SELECT/session state must be serialized");
  }
  if (!redis.includes("redis_command_requires_confirmation")) {
    errors.push("Redis dangerous commands must share a backend confirmation policy");
  }
  for (const command of ["EVAL", "RESTORE", "RENAME", "SWAPDB", "REPLICAOF", "MODULE"]) {
    if (!redis.includes(`"${command}"`)) errors.push(`Redis confirmation policy is missing ${command}`);
  }
}
if (exists("src-tauri/src/commands.rs")) {
  const commands = read("src-tauri/src/commands.rs");
  if (commands.includes("append_audit_event")) {
    errors.push("The renderer must not have a generic audit-append command");
  }
  for (const token of ["redact_sql_password_literals", "redacts_mysql_identity_passwords", "redacts_postgres_identity_passwords"]) {
    if (!commands.includes(token)) errors.push(`SQL audit redaction is missing ${token}`);
  }
  for (const token of [
    "pub fn save_diagnostics_report(",
    "MAX_DIAGNOSTICS_BYTES",
    "Diagnostics reports must be saved as a .json file",
    '"diagnostics_export"',
    "Exported redacted runtime diagnostics",
  ]) {
    if (!commands.includes(token)) errors.push(`Redacted diagnostics export is missing ${token}`);
  }
}
if (exists("src-tauri/src/storage.rs")) {
  const source = read("src-tauri/src/storage.rs");
  for (const token of ["audit_log_no_delete", "audit_log_no_update", "encrypted_config", "query_history"]) {
    if (!source.includes(token)) errors.push(`SQLite persistence is missing ${token}`);
  }
}
if (exists("src-tauri/src/lib.rs")) {
  const source = read("src-tauri/src/lib.rs");
  for (const token of ["--screenshot", "RunEvent::ExitRequested", "shutdown"]) {
    if (!source.includes(token)) errors.push(`Tauri lifecycle is missing ${token}`);
  }
  for (const command of [
    "transfer::transfer_start",
    "transfer::transfer_status",
    "transfer::transfer_cancel",
    "rowsync::sync_plan_start",
    "rowsync::sync_plan_status",
    "rowsync::sync_plan_cancel",
    "rowsync::sync_get_plan",
    "rowsync::sync_apply",
  ]) {
    if (!source.includes(command)) errors.push(`Tauri must register ${command}`);
  }
  if (exists("src-tauri/src/commands.rs")) {
    const commands = read("src-tauri/src/commands.rs");
    const registered = [...source.matchAll(/commands::([a-zA-Z0-9_]+)/g)].map((match) => match[1]);
    for (const command of registered) {
      if (!commands.includes(`pub async fn ${command}(`) && !commands.includes(`pub fn ${command}(`)) {
        errors.push(`Tauri registers missing command ${command}`);
      }
    }
  }
}
if (exists("src-tauri/tauri.conf.json") && exists("package.json")) {
  const conf = JSON.parse(read("src-tauri/tauri.conf.json"));
  const pkg = JSON.parse(read("package.json"));
  if (conf.build?.beforeBuildCommand !== "npm run build:production") {
    errors.push("Tauri packaging must use the strict production build command");
  }
  if (!pkg.scripts?.["build:production"]) {
    errors.push("package.json must expose a strict production build");
  }
}
if (exists("src-tauri/src/drivers/sql/sqlx_driver.rs")) {
  const sqlx = read("src-tauri/src/drivers/sql/sqlx_driver.rs");
  if (!sqlx.includes("paginate_sql(")) errors.push("SQLx pagination must push LIMIT/OFFSET to the server for SELECT queries");
}

// Runtime wiring and safety regressions. These checks deliberately target the
// contracts that used to be replaced by fake success toasts.
if (exists("ui/app.js")) {
  const source = read("ui/app.js");
  for (const token of [
    "__TAURI_INTERNALS__",
    'backendCall("test_connection"',
    'backendCall("save_connection"',
    'backendCall("sql_fetch"',
    'backendCall("sql_cancel"',
    'backendCall("redis_scan"',
    'backendCall("redis_get"',
    'backendCall("redis_set"',
    'backendCall("redis_command"',
    'backendCall("redis_info"',
    'backendCall("export_data"',
    'backendCall("import_data"',
    'backendCall("sql_drop_table"',
    'backendCall("get_connection_config"',
    'backendCall("test_saved_connection"',
    'backendCall("delete_connection"',
    'backendCall("save_query_draft"',
    'backendCall("sql_apply_ddl"',
    'backendCall("disconnect_connection"',
    'backendCall("save_diagnostics_report"',
    'plugin:dialog|save'
  ]) {
    if (!source.includes(token)) errors.push(`UI runtime wiring is missing ${token}`);
  }
  if (source.includes("function appendAudit(")) {
    errors.push("UI must not append fake audit rows locally");
  }
  if (!source.includes("confirmDangerous(") || !source.includes("confirmedDangerous: true")) {
    errors.push("Redis destructive commands must pass through the confirmed backend gate");
  }
  for (const token of ["renderGridBody", "virtual-spacer", "redisKeyRow", "renderRedisEditor", "loadGridPage", "sql_delete_rows", "showAppMenu", "runTransactionBlock", "designerPlan", "computeSchemaDiff", "sql_run_script", "renderResultTabs", "renderConsoleView", "buildIdentitySql", "mysqlLifecycleTarget", "postgresLifecycleRole", "redisAclUsername", "identity-create", "identity-reset-password", "identity-drop", "acl-apply", "acl-delete", "buildDiagnosticReport", "exportDiagnosticReport", "导出脱敏报告"]) {
    if (!source.includes(token)) errors.push(`UI runtime capability is missing ${token}`);
  }
  if (source.includes("诊断导出未接入") || source.includes("尚未实现脱敏诊断包")) {
    errors.push("The diagnostics export action must not remain an unimplemented stub");
  }
  if (!source.includes("密码已隐藏") || !source.includes("resetpass")) {
    errors.push("Identity lifecycle confirmations must hide passwords and use Redis password reset semantics");
  }
  for (const token of ["window-minimize", "window-maximize", "window-close", "focusSearch", "connection-search", "toggle-sidebar", "sql_table_ddl", "setObjectsScope", "sql_object_stats"]) {
    if (!source.includes(token)) errors.push(`UI interaction coverage is missing ${token}`);
  }
  if (source.includes("1,284") || source.includes("42.8k")) {
    errors.push("UI must not ship fabricated aggregate metrics");
  }
  if (/\bKEYS\s+\*/.test(source)) errors.push("UI must not send KEYS *");
  if (!source.includes('tab.kind !== "table"')) {
    errors.push("Grid edits and row deletion must be limited to table-preview results");
  }
  if (!source.includes("const key = contextRedisKey")) {
    errors.push("Redis delete must prefer the explicitly selected tree key");
  }
  if (!source.includes("operationId: op") ||
      !source.includes("if (activeRedisOperationId === op) activeRedisOperationId = null;") ||
      source.includes("当前 Redis 操作暂不支持取消")) {
    errors.push("Redis get/info/delete operations must be tied to the active cancellation ID");
  }
  for (const token of ["batchSize:", "#transfer-batch"]) {
    if (!source.includes(token)) errors.push(`SQL import UI must pass the bounded batch size to the importer (missing ${token})`);
  }
  // Data transfer and row sync are separate features with different semantics; both must stay wired.
  for (const token of [
    'backendCall("transfer_start"',
    'backendCall("transfer_status"',
    'backendCall("transfer_cancel"',
    'backendCall("sync_plan_start"',
    'backendCall("sync_plan_status"',
    'backendCall("sync_get_plan"',
    'backendCall("sync_apply"',
    "renderTransferView",
    "renderSyncView",
    "modeStripHtml",
  ]) {
    if (!source.includes(token)) errors.push(`Data transfer / row sync wiring is missing ${token}`);
  }
  if (!source.includes("数据传输") || !source.includes("数据同步") || !source.includes("结构比较")) {
    errors.push("The renderer must explain how data transfer, row sync and schema compare differ");
  }
  if (!source.includes("confirmedDangerous: true") || !/sync_apply[\s\S]{0,220}confirmedDangerous: true/.test(source)) {
    errors.push("Applying row deletes must pass the confirmed danger gate");
  }
  if (!source.includes('id="transfer-scope"') || !source.includes('exportScope === "database"')) {
    errors.push("SQL export UI must expose table and whole-database scope");
  }
  if (source.includes("$(\".breadcrumb strong\")?.textContent || \"当前表\"")) {
    errors.push("DROP TABLE must not derive its target from stale breadcrumb text");
  }
}
if (exists("src-tauri/src/commands.rs") && exists("src-tauri/src/drivers/sql/sqlx_driver.rs")) {
  const commands = read("src-tauri/src/commands.rs");
  const sqlx = read("src-tauri/src/drivers/sql/sqlx_driver.rs");
  const traits = read("src-tauri/src/drivers/sql/trait.rs");
  if (!traits.includes("async fn transaction")) errors.push("SqlDriver must expose transaction semantics");
  if (!commands.includes("sql_statement_count") || !commands.includes("Multiple SQL statements must use the transaction batch")) {
    errors.push("Single-statement SQL entrypoints must reject multi-statement scripts");
  }
  if (!sqlx.includes("cancel_backend_query") || !sqlx.includes("KILL QUERY") || !sqlx.includes("pg_cancel_backend")) {
    errors.push("SQL cancellation must terminate the server-side operation");
  }
  if (sqlx.includes(".fetch_all(pool)")) {
    errors.push("SQLx fetch path must not load the complete result set with fetch_all");
  }
}

// Appearance: dark mode is a generated mirror of ui/styles.css plus a curated token layer, and it
// must stay wired end to end (pre-paint script, stylesheet order, renderer, native window, menu).
{
  const shell = exists("ui/index.html") ? read("ui/index.html") : "";
  const styles = exists("ui/styles.css") ? read("ui/styles.css") : "";
  const preload = exists("ui/theme-preload.js") ? read("ui/theme-preload.js") : "";
  const darkTheme = exists("ui/theme-dark.css") ? read("ui/theme-dark.css") : "";
  const darkAuto = exists("ui/theme-dark-auto.css") ? read("ui/theme-dark-auto.css") : "";
  const entry = exists("src/main.tsx") ? read("src/main.tsx") : "";
  const editor = exists("src/App.tsx") ? read("src/App.tsx") : "";
  const renderer = exists("ui/app.js") ? read("ui/app.js") : "";
  const commands = exists("src-tauri/src/commands.rs") ? read("src-tauri/src/commands.rs") : "";
  const host = exists("src-tauri/src/lib.rs") ? read("src-tauri/src/lib.rs") : "";
  const menu = exists("src-tauri/src/menu.rs") ? read("src-tauri/src/menu.rs") : "";

  const keyOf = (text) => (text.match(/"(luxbase\.theme[^"]*)"/) || [])[1];
  if (!keyOf(renderer) || keyOf(renderer) !== keyOf(preload)) {
    errors.push("The pre-paint theme script and the renderer must read the same theme preference key");
  }
  // The pre-paint lives in its own file because CSP forbids inline scripts; the inline <style> that
  // covers the empty-body flash only works because inline styles remain allowed.
  const csp = (exists("src-tauri/tauri.conf.json")
    ? JSON.parse(read("src-tauri/tauri.conf.json"))?.app?.security?.csp || ""
    : "");
  if (!/style-src[^;]*'unsafe-inline'/.test(csp) || /script-src[^;]*'unsafe-inline'/.test(csp)) {
    errors.push("The theme preload relies on CSP allowing inline styles but not inline scripts");
  }
  // Tauri injects a nonce into style-src by default; per CSP a nonce makes browsers ignore
  // 'unsafe-inline', which silently strips Monaco's inline line positions and dynamic theme CSS
  // (overlapping lines, no caret, no selection, no token colours). Keep style-src unmodified.
  const cspModification = (exists("src-tauri/tauri.conf.json")
    ? JSON.parse(read("src-tauri/tauri.conf.json"))?.app?.security?.dangerousDisableAssetCspModification
    : undefined);
  const styleSrcUnmodified = cspModification === true
    || (Array.isArray(cspModification) && cspModification.includes("style-src"));
  if (!styleSrcUnmodified) {
    errors.push("dangerousDisableAssetCspModification must exempt style-src, otherwise the injected nonce disables Monaco's inline styles");
  }
  if (preload.includes("import ")) errors.push("The theme pre-paint script must stay import-free");
  if (!shell.includes('<script src="./theme-preload.js"></script>')) {
    errors.push("The file:// shell must pre-paint the theme before ui/app.js runs");
  }
  if (!entry.includes('import "../ui/theme-preload.js"')) {
    errors.push("The React entry must import the theme pre-paint script first");
  }
  const shellOrder = ["./styles.css", "./theme-dark-auto.css", "./theme-dark.css"].map((href) => shell.indexOf(`href="${href}"`));
  if (shellOrder.some((index) => index < 0) || shellOrder.some((index, position) => position > 0 && index < shellOrder[position - 1])) {
    errors.push("The dependency-free shell must load base, mirrored and curated dark styles in that order");
  }
  if (!entry.includes('"../ui/theme-dark-auto.css"') || !entry.includes('"../ui/theme-dark.css"')) {
    errors.push("The React build must bundle both dark stylesheets");
  }
  for (const documentFile of ["index.html", "ui/index.html"]) {
    if (!exists(documentFile) || !/content="light dark"/.test(read(documentFile))) {
      errors.push(`${documentFile} must advertise both colour schemes so native controls follow the theme`);
    }
  }
  if (!darkAuto.includes('[data-theme="dark"]')) errors.push("ui/theme-dark-auto.css is not scoped to [data-theme=\"dark\"]");
  if (/[{;]\s*--[\w-]+\s*:/.test(darkAuto)) errors.push("The generated dark mirror must not redeclare custom properties; the curated layer owns them");
  for (const token of ["color-scheme: dark", "--icon-plate"]) {
    if (!darkTheme.includes(token)) errors.push(`The curated dark layer is missing ${token}`);
  }
  if (!darkTheme.includes("[data-theme=\"dark\"]")) errors.push("ui/theme-dark.css has no dark-scoped rules");
  for (const token of ["luxbase-dark", "__luxbaseSetEditorTheme"]) {
    if (!editor.includes(token)) errors.push(`The Monaco bridge is missing ${token}`);
  }
  for (const token of ["applyThemePreference", 'case "set-theme"', "set_window_appearance"]) {
    if (!renderer.includes(token)) errors.push(`Theme renderer wiring is missing ${token}`);
  }
  if (!renderer.includes('matchMedia("(prefers-color-scheme: dark)")')) {
    errors.push("The renderer must follow the system appearance when the preference is system");
  }
  for (const token of ["data-action=\"set-theme\"", "tb-theme-toggle", "外观"]) {
    if (!shell.includes(token)) errors.push(`The shell has no theme entry for ${token}`);
  }
  // One button in the opaque top-right cluster flips light ↔ dark and always shows the current
  // mode; the cluster owns its surface so dark mode cannot leave transparent glyphs over the panel.
  if (!/class="tb-right"/.test(shell)) errors.push("The toolbar right cluster must be a contained surface");
  if (!/class="tb-theme-toggle" data-action="cycle-theme"/.test(shell)) errors.push("The appearance control must be a single toolbar button");
  for (const token of ["id=\"theme-toggle-label\"", "id=\"theme-toggle-icon\""]) {
    if (!shell.includes(token)) errors.push(`The appearance button is missing ${token}`);
  }
  for (const mode of ["system", "light", "dark"]) {
    if (!shell.includes(`data-theme-preference="${mode}"`)) errors.push(`The settings appearance card is missing the ${mode} choice`);
  }
  for (const token of ["function cycleTheme", 'case "cycle-theme"', "#theme-toggle-label", "#theme-toggle-icon"]) {
    if (!renderer.includes(token)) errors.push(`The renderer must drive the single appearance button: ${token}`);
  }
  // Tab strip bulk-close affordances.
  for (const token of ["data-action=\"tab-menu\"", "tab-close-others", "tab-close-left", "tab-close-right", "tab-close-all"]) {
    if (!shell.includes(token) && !renderer.includes(token)) errors.push(`Tab bulk-close affordance missing: ${token}`);
  }
  for (const token of ["function closeTabBatch", "function performBatchClose", "batch-close-save", "batch-close-discard"]) {
    if (!renderer.includes(token)) errors.push(`Batch close wiring missing: ${token}`);
  }
  // Review-round guards: the 编辑器选项 button must open live controls, menus and splitters must be
  // keyboard surfaces, and the webview context menu must not leak onto non-editable chrome.
  for (const token of ["function openEditorOptions", "data-eo-switch", "data-eo-step", "function closeEditorOptions"]) {
    if (!renderer.includes(token)) errors.push(`Live editor options missing: ${token}`);
  }
  for (const token of ["__luxbaseEditorSetOptions", "luxbase.editorOpts.v1"]) {
    if (!editor.includes(token)) errors.push(`Monaco editor options bridge missing: ${token}`);
  }
  if (!shell.includes('role="menu"') || !renderer.includes("menuitem")) errors.push("Context menus must carry menu/menuitem semantics");
  if (!renderer.includes("ArrowDown: 1, ArrowUp: -1")) errors.push("Context menus must support arrow-key navigation");
  const separators = (shell.match(/data-panel-resize="[^"]+" tabindex="0" role="separator"/g) || []).length;
  if (separators !== 3) errors.push(`All three panel splitters must be keyboard separators, found ${separators}`);
  if (!renderer.includes('closest?.("input, textarea, [contenteditable=\'true\'], .monaco-editor")')) {
    errors.push("The native context menu must stay suppressed outside editable surfaces");
  }
  if (!styles.includes("::selection") || !darkTheme.includes("::selection")) errors.push("Text selection must be themed in both modes");
  if (!darkTheme.includes("#46536a")) errors.push("Dark scrollbar thumbs must stay visible on the dark track");
  // Immediate client-side SQL feedback: squiggles plus a jump-to-problem chip in the editor footer.
  const sqlLint = exists("ui/sql-lint.js") ? read("ui/sql-lint.js") : "";
  for (const token of ["export function lintSql", "export function tokenizeSql"]) {
    if (!sqlLint.includes(token)) errors.push(`The shared SQL lint is missing ${token}`);
  }
  for (const token of ["lintSql", "setModelMarkers", "luxbase-sql"]) {
    if (!editor.includes(token)) errors.push(`The editor lint wiring is missing ${token}`);
  }
  if (!editor.includes('../ui/sql-lint.js')) errors.push("The editor must use the shared, unit-tested SQL lint");
  if (!shell.includes('id="editor-problems"')) errors.push("The editor footer must expose the problem count");
  if (!editor.includes("editor-problems")) errors.push("The editor must update the problem chip");
  if (!commands.includes("pub async fn set_window_appearance")) errors.push("Backend command set_window_appearance is missing");
  if (!host.includes("commands::set_window_appearance")) errors.push("Tauri must register set_window_appearance");
  if (!menu.includes("set_theme") && !menu.includes("theme-system")) errors.push("The native menu must expose appearance choices");
  for (const action of ["theme-system", "theme-light", "theme-dark"]) {
    if (!menu.includes(`"${action}"`)) errors.push(`Native menu is missing ${action}`);
    if (!renderer.includes(`"${action}"`)) errors.push(`The renderer does not handle the native menu id ${action}`);
  }

  const fresh = spawnSync(process.execPath, [resolve(root, "scripts/build-dark-theme.js"), "--check"], { encoding: "utf8" });
  if (fresh.status !== 0) {
    errors.push(`ui/theme-dark-auto.css is stale: ${(fresh.stderr || fresh.stdout || "").trim().split("\n").pop() || "run node scripts/build-dark-theme.js"}`);
  }
}

// Tauri assets should be icon-complete for packaging.
for (const file of ["src-tauri/icons/32x32.png", "src-tauri/icons/128x128.png", "src-tauri/icons/icon.icns", "src-tauri/icons/icon.ico"]) {
  nonTrivial(file, 100);
}

// Screenshot artifact
const shot = resolve(root, "shots/baseline.png");
if (!existsSync(shot) || statSync(shot).size < 10_000) {
  errors.push("shots/baseline.png is missing or blank-sized");
} else {
  const signature = readFileSync(shot).subarray(0, 8);
  if (!signature.equals(Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]))) {
    errors.push("shots/baseline.png is not a PNG");
  }
}

const rustc = spawnSync("rustc", ["--version"], { encoding: "utf8" });
const cargo = spawnSync("cargo", ["--version"], { encoding: "utf8" });
const rustAvailable = rustc.status === 0 && cargo.status === 0;
if (!rustAvailable) {
  warnings.push("Rust toolchain is unavailable on this host; Rust compilation is pending an environment with rustup/Cargo access.");
}

const result = {
  ok: errors.length === 0,
  rustAvailable,
  errors,
  warnings,
  checkedFiles: requiredRust.length + 8,
};
console.log(JSON.stringify(result, null, 2));
if (errors.length) process.exit(1);

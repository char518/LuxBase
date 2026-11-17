import { readFileSync } from "node:fs";
import { resolve } from "node:path";

const root = resolve(import.meta.dirname, "..");
const source = readFileSync(resolve(root, "ui/app.js"), "utf8");
const shell = readFileSync(resolve(root, "ui/index.html"), "utf8");
const styles = readFileSync(resolve(root, "ui/styles.css"), "utf8");
const appSource = readFileSync(resolve(root, "src/App.tsx"), "utf8");
const tauriHost = readFileSync(resolve(root, "src-tauri/src/lib.rs"), "utf8");
const nativeMenu = readFileSync(resolve(root, "src-tauri/src/menu.rs"), "utf8");
const tauriConfig = JSON.parse(readFileSync(resolve(root, "src-tauri/tauri.conf.json"), "utf8"));
const capabilities = readFileSync(resolve(root, "src-tauri/capabilities/default.json"), "utf8");
const buildHost = readFileSync(resolve(root, "scripts/build-macos-app.js"), "utf8");
const failures = [];

function extractFunction(name) {
  const marker = `  function ${name}(`;
  const start = source.indexOf(marker);
  if (start < 0) throw new Error(`Function ${name} was not found`);
  const next = source.indexOf("\n  function ", start + marker.length);
  if (next < 0) throw new Error(`Could not find the end of ${name}`);
  return source.slice(start, next);
}

function loadFunction(name) {
  return new Function(`${extractFunction(name)}\nreturn ${name};`)();
}

const splitSqlStatements = loadFunction("splitSqlStatements");
const formatSqlText = loadFunction("formatSqlText");
const isDangerousRedisCommand = loadFunction("isDangerousRedisCommand");
const parameterCount = loadFunction("parameterCount");

function assert(name, condition, detail) {
  if (!condition) failures.push(`${name}: ${detail}`);
}

const ordinary = splitSqlStatements("SELECT 1; SELECT 'a;b'; -- trailing");
assert("ordinary SQL split", ordinary.length === 2, `expected 2 statements, received ${ordinary.length}`);
const hashComment = splitSqlStatements("SELECT 1; # semicolon ; in comment\nSELECT 2;");
assert("MySQL # comment split", hashComment.length === 2, `expected 2 statements, received ${hashComment.length}`);

const dollar = splitSqlStatements(
  "DO $$ BEGIN RAISE NOTICE 'inside;body'; END $$; SELECT $1;",
);
assert("dollar-quoted SQL split", dollar.length === 2, `expected 2 statements, received ${dollar.length}`);
assert("dollar body preserved", dollar[0].includes("inside;body"), "function body separator was lost");

const formatted = formatSqlText("select a,b from t where a='x  y' and b=2;");
assert("formatter keyword case", formatted.startsWith("SELECT"), formatted);
assert("formatter preserves literal", formatted.includes("'x  y'"), formatted);
assert("formatter adds clause breaks", formatted.includes("\nFROM") && formatted.includes("\nWHERE"), formatted);

assert("DEL is dangerous", isDangerousRedisCommand("DEL", ["key"]), "DEL was not guarded");
assert("FLUSHALL is dangerous", isDangerousRedisCommand("FLUSHALL", []), "FLUSHALL was not guarded");
assert("EVAL is dangerous", isDangerousRedisCommand("EVAL", ["return redis.call('FLUSHALL')", "0"]), "script execution bypassed the guard");
assert("ACL SETUSER is dangerous", isDangerousRedisCommand("acl", ["setuser", "app", "on", ">secret"]), "ACL changes bypassed the guard");
assert("SCRIPT FLUSH is dangerous", isDangerousRedisCommand("SCRIPT", ["FLUSH"]), "script-cache flush bypassed the guard");
assert("RESTORE is dangerous", isDangerousRedisCommand("RESTORE", ["key", "0", "payload"]), "key replacement bypassed the guard");
assert("ordinary GET is not dangerous", !isDangerousRedisCommand("GET", ["key"]), "read-only GET was incorrectly guarded");
assert("KEYS is not confused with DEL guard", !isDangerousRedisCommand("KEYS", ["*"]), "KEYS should be blocked by its own guard");
assert("parameter count ignores literals", parameterCount("SELECT '?' AS q, $$ $9 $$, $1 -- ?\n") === 1, "literal/comment placeholders were counted");
assert("parameter count handles multiple indexes", parameterCount("SELECT $2, ?, '?'") === 2, "numbered or positional parameters were miscounted");

const staticActions = [...new Set([...shell.matchAll(/data-action="([^"]+)"/g)].map((match) => match[1]))];
const missingActions = staticActions.filter((action) => !source.includes(`case "${action}"`) && !source.includes(`"${action}"`));
assert("all static actions are handled", missingActions.length === 0, `missing handlers: ${missingActions.join(", ")}`);

assert(
  "standalone shell loads a classic script",
  shell.includes('<script src="./app.js" defer></script>') && !shell.includes('type="module"'),
  "the file:// shell must not depend on an ES-module import",
);
assert(
  "Tauri window controls are wired",
  ["minimize_window", "toggle_maximize_window", "close_window"].every((command) => tauriHost.includes(`commands::${command}`))
    && ["core:window:allow-minimize", "core:window:allow-toggle-maximize", "core:window:allow-close"].every((permission) => capabilities.includes(permission)),
  "the frameless window needs minimize, maximize and close commands",
);
assert(
  "native traffic lights and draggable toolbar",
  tauriConfig.app.windows[0].decorations === true
    && tauriConfig.app.windows[0].titleBarStyle === "Overlay"
    && shell.includes('<header class="toolbar" data-tauri-drag-region>')
    && capabilities.includes("core:window:allow-start-dragging"),
  "overlay chrome needs native decorations and a draggable toolbar",
);
assert(
  "macOS bundle is produced by Tauri",
  buildHost.includes('"--bundles"') && buildHost.includes('withDmg ? "app,dmg" : "app"') && buildHost.includes("codesign"),
  "the canonical application must be the Tauri build with the Rust drivers",
);
assert(
  "native edit menu preserves standard text shortcuts",
  nativeMenu.includes('PredefinedMenuItem::select_all(app, Some("全选"))')
    && nativeMenu.includes('PredefinedMenuItem::copy(app, Some("拷贝"))')
    && source.includes('case "editor-select-all"'),
  "native text editing commands must remain available",
);
assert(
  "objects view and statistics are connected",
  shell.includes('id="view-objects"')
    && shell.includes('data-workspace="objects"')
    && source.includes('backendCall("sql_object_stats"')
    && tauriHost.includes("commands::sql_object_stats"),
  "the pinned objects view must load real catalog statistics",
);
assert(
  "editable and readable content can be selected",
  styles.includes('[contenteditable="true"], pre, code, table') && styles.includes("-webkit-user-select: text"),
  "WebKit must allow native copy and select-all in text surfaces",
);
assert(
  "Monaco supports native select-all and text insertion",
  appSource.includes('selectAll: "editor.action.selectAll"')
    && appSource.includes("window.__luxbaseEditorInsertText")
    && appSource.includes("window.__luxbaseEditorDeleteSelection"),
  "the production editor must expose select-all, paste and cut hooks",
);

const fakeMenu = { hidden: true, innerHTML: "", style: {} };
const fakeAnchor = {
  getBoundingClientRect: () => ({ left: 24, bottom: 70 }),
  getAttribute: () => "false",
  setAttribute: (name, value) => { fakeAnchor[name] = value; },
};
const showAppMenu = new Function(
  "appMenu",
  "window",
  "escapeHtml",
  `${extractFunction("menuItem")}\n${extractFunction("showAppMenu")}\nreturn showAppMenu;`,
)(
  fakeMenu,
  { innerWidth: 1600 },
  (value) => String(value).replace(/[&<>"']/g, (character) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#039;" }[character])),
);
showAppMenu("more", fakeAnchor);
assert("menu popover opens", fakeMenu.hidden === false, "menu remained hidden");
assert("menu commands render", fakeMenu.innerHTML.includes("连接诊断") && fakeMenu.innerHTML.includes("设置"), "expected menu items were missing");
assert("menu anchor marks expanded", fakeAnchor["aria-expanded"] === "true", "the menu button did not expose its expanded state");

function loadPure(names, invocation) {
  const definitions = names.map(extractFunction).join("\n");
  return new Function(`${definitions}\nreturn (${invocation});`)();
}

const helpers = [
  "protocolQuote",
  "protocolQualified",
  "quoteIdent",
  "base64ToHex",
  "sqlLiteral",
  "splitColumnType",
  "joinColumnType",
  "defaultExpression",
  "designerColumn",
  "mysqlColumnDefinition",
  "genericColumnDefinition",
  "pgColumnDefinition",
  "columnDefinitionFor",
  "normalizeColumnType",
  "normalizeDefault",
  "normalizeExtra",
  "columnChanges",
  "primaryKeyColumns",
  "isPrimaryIndex",
  "computeSchemaDiff",
];
const computeSchemaDiff = new Function(
  `const DESTRUCTIVE_DIFFS = new Set(["drop_table", "drop_view", "drop_column"]);
   const protocolMeta = { postgres: { label: "PostgreSQL" }, mysql: { label: "MySQL" } };
   ${helpers.map(extractFunction).join("\n")}
   return computeSchemaDiff;`,
)();
const sourceSchema = {
  name: "app",
  tables: [{
    schema: "app",
    name: "items",
    tableType: "BASE TABLE",
    columns: [
      { name: "id", dataType: "bigint", nullable: false, defaultValue: null, isPrimaryKey: true, ordinal: 1 },
      { name: "name", dataType: "text", nullable: false, defaultValue: null, isPrimaryKey: false, ordinal: 2 },
    ],
    indexes: [{ name: "items_pkey", columns: ["id"], unique: true }, { name: "idx_items_name", columns: ["name"], unique: false }],
    foreignKeys: [],
  }, {
    schema: "app",
    name: "tags",
    tableType: "BASE TABLE",
    columns: [{ name: "id", dataType: "integer", nullable: false, defaultValue: "nextval('tags_id_seq'::regclass)", isPrimaryKey: true, ordinal: 1 }],
    indexes: [{ name: "tags_pkey", columns: ["id"], unique: true }],
    foreignKeys: [],
  }],
};
const targetSchema = {
  name: "prod",
  tables: [{
    schema: "prod",
    name: "items",
    tableType: "BASE TABLE",
    columns: [
      { name: "id", dataType: "bigint", nullable: false, defaultValue: null, isPrimaryKey: true, ordinal: 1 },
      { name: "legacy", dataType: "text", nullable: true, defaultValue: null, isPrimaryKey: false, ordinal: 2 },
    ],
    indexes: [{ name: "items_pkey", columns: ["id"], unique: true }],
    foreignKeys: [],
  }],
};
const diff = computeSchemaDiff(sourceSchema, targetSchema, { protocol: "postgres", sourceProtocol: "postgres", schema: "prod" });
const allItems = diff.tables.flatMap((table) => table.items);
const checkedSql = allItems.filter((item) => item.checked).flatMap((item) => item.ops.map((op) => op.sql));
assert("schema diff matches tables by name across schemas", diff.tables.find((table) => table.name === "items")?.status === "changed", JSON.stringify(diff.tables));
assert("schema diff adds the missing column in the target schema", checkedSql.some((sql) => sql.includes('ALTER TABLE "prod"."items" ADD COLUMN "name"')), JSON.stringify(checkedSql));
assert("schema diff creates the missing index", checkedSql.some((sql) => sql.includes('CREATE INDEX "idx_items_name" ON "prod"."items"')), JSON.stringify(checkedSql));
assert("schema diff skips primary-key indexes", !allItems.some((item) => item.name === "items_pkey"), JSON.stringify(allItems));
assert("schema diff creates missing tables with serial keys", checkedSql.some((sql) => sql.includes('CREATE TABLE "prod"."tags"') && sql.includes("serial") && !sql.includes("nextval")), JSON.stringify(checkedSql));
const dropColumn = allItems.find((item) => item.kind === "drop_column");
assert("destructive changes are opt-in", dropColumn && !dropColumn.checked && dropColumn.ops.length === 1, JSON.stringify(dropColumn));

const createPlan = loadPure([...helpers, "validateDesigner", "createTablePlan"], "createTablePlan");
const createColumn = { name: "id", base: "TEXT", length: "", scale: "", suffix: "", notNull: true, pk: true, defaultValue: "", comment: "", extra: "", dropped: false };
const sqlitePlan = createPlan({ table: "test_table", protocol: "sqlite", create: true, columns: [createColumn], newIndexes: [] });
assert("SQLite text primary key preserves not-null", sqlitePlan.statements[0].includes('"id" TEXT PRIMARY KEY NOT NULL'), JSON.stringify(sqlitePlan));
let invalidIdentity = false;
try {
  createPlan({ table: "test_table", protocol: "postgres", create: true, columns: [{ ...createColumn, base: "uuid", extra: "identity" }], newIndexes: [] });
} catch (error) {
  invalidIdentity = error.message.includes("不支持自增");
}
assert("identity columns require integer types", invalidIdentity, "uuid identity must not generate executable SQL");
assert("native close shortcuts do not conflict", !nativeMenu.includes("PredefinedMenuItem::close_window") && nativeMenu.includes('"window-close", "关闭窗口", Some("CmdOrCtrl+Shift+W")'), "Cmd+W is reserved for close-tab");

const darkThemeSource = readFileSync(resolve(root, "ui/theme-dark.css"), "utf8");
const darkAutoSource = readFileSync(resolve(root, "ui/theme-dark-auto.css"), "utf8");

function loadThemeResolver(systemDark) {
  return new Function(
    `const THEME_MODES = ["system", "light", "dark"];
     const darkThemeQuery = { matches: ${systemDark} };
     const localStorage = { getItem: () => "system" };
     ${extractFunction("themePreference")}
     ${extractFunction("themeIsDark")}
     return themeIsDark;`,
  )();
}
const onLightOs = loadThemeResolver(false);
const onDarkOs = loadThemeResolver(true);
assert("explicit dark wins on a light system", onLightOs("dark") === true, "dark must resolve dark");
assert("explicit light wins on a dark system", onDarkOs("light") === false, "light must resolve light");
assert("system follows the macOS appearance", onLightOs("system") === false && onDarkOs("system") === true, "system must mirror prefers-color-scheme");
assert("an unknown preference falls back to system", onLightOs("neon") === false && onDarkOs("neon") === true, "invalid stored value must not leave the app unstyled");

const applyTheme = extractFunction("applyThemePreference");
for (const contract of [
  "dataset.theme",
  "dataset.themePreference",
  "window.__luxbaseSetEditorTheme",
  'backendCall("set_window_appearance", { mode })',
  'data-theme-preference',
  'aria-pressed',
  '#theme-toggle-label',
]) {
  assert("theme application covers one surface", applyTheme.includes(contract), `applyThemePreference does not touch ${contract}`);
}

// The derived mirror may not fight the curated token layer, and every colour token the light
// stylesheet declares has to have a dark value or the mirror would resolve back to light.
const lightColorTokens = [...styles.matchAll(/:root\s*\{([^}]*)\}/g)]
  .flatMap((block) => [...block[1].matchAll(/(--[\w-]+)\s*:\s*([^;]*#[0-9a-f]{3,8}[^;]*|[^;]*rgba?\([^;]*)/gi)])
  .map((match) => match[1]);
const unmirrored = lightColorTokens.filter((token) => !darkThemeSource.includes(`${token}:`));
assert("every colour token has a dark value", unmirrored.length === 0, `light-only tokens: ${unmirrored.join(", ")}`);
assert("dark mirror is fully scoped", darkAutoSource.split("\n").filter((line) => /\{$/.test(line) && !line.startsWith(" ") && !line.startsWith("@"))
  .every((line) => line.includes('[data-theme="dark"]')), "an unscoped dark rule would repaint light mode");
assert("dark mirror leaves tokens to the curated layer", !/[{;]\s*--[\w-]+\s*:/.test(darkAutoSource), "the generator must not redeclare custom properties");
const darkSurfaceCount = (darkAutoSource.match(/hsl\(/g) || []).length;
assert("dark mirror maps literals", darkSurfaceCount > 500, `only ${darkSurfaceCount} mapped colours found`);

// The editor's immediate SQL feedback must flag real writing defects and stay quiet on valid SQL,
// including literals, comments and dialect-specific quoting that hide punctuation from the scanner.
const { lintSql } = await import("../ui/sql-lint.js");
const lintClean = lintSql("SELECT id, name FROM t WHERE id = 1 AND name = 'a;b';", "mysql");
assert("valid SQL produces no problems", lintClean.length === 0, JSON.stringify(lintClean));
const lintMasked = lintSql("SELECT ';' AS s, '(' AS p FROM t -- ) ,;\n/* , ( */ SELECT 2;", "mysql");
assert("literals and comments mask punctuation", lintMasked.length === 0, JSON.stringify(lintMasked));
const lintUnclosed = lintSql("SELECT 1;\nSELECT 'abc", "mysql");
assert("unterminated string is flagged on its line", lintUnclosed.length === 1 && lintUnclosed[0].line === 2 && lintUnclosed[0].severity === "error", JSON.stringify(lintUnclosed));
const lintOpenBracket = lintSql("SELECT (id FROM t", "mysql");
assert("unbalanced left bracket is flagged", lintOpenBracket.some((p) => p.message.includes("左括号没有闭合")), JSON.stringify(lintOpenBracket));
const lintCloseBracket = lintSql("SELECT id) FROM t", "mysql");
assert("stray right bracket is flagged", lintCloseBracket.some((p) => p.message.includes("多余的右括号")), JSON.stringify(lintCloseBracket));
const lintStrayComma = lintSql("SELECT id, FROM t", "mysql");
assert("dangling comma before FROM is flagged", lintStrayComma.some((p) => p.message.includes("多余的逗号")), JSON.stringify(lintStrayComma));
const lintTruncated = lintSql("SELECT id FROM t WHERE", "mysql");
assert("truncated condition is flagged", lintTruncated.some((p) => p.message.includes("语句不完整")), JSON.stringify(lintTruncated));
const lintUnknownHead = lintSql("FOO bar;", "mysql");
assert("unknown statement head is a warning", lintUnknownHead.length === 1 && lintUnknownHead[0].severity === "warning", JSON.stringify(lintUnknownHead));
const lintDollar = lintSql("SELECT $$a;b$$ AS body;", "postgres");
assert("dollar-quoted body is opaque", lintDollar.length === 0, JSON.stringify(lintDollar));
const lintDollarOpen = lintSql("SELECT $tag$ unclosed", "postgres");
assert("unterminated dollar quote is flagged", lintDollarOpen.some((p) => p.message.includes("字符串没有闭合")), JSON.stringify(lintDollarOpen));
const lintMysqlEscape = lintSql("SELECT 'a\\'b';", "mysql");
assert("MySQL backslash escape keeps the string closed", lintMysqlEscape.length === 0, JSON.stringify(lintMysqlEscape));
const lintPgEscape = lintSql("SELECT 'a\\'b';", "postgres");
assert("PostgreSQL treats the same text as two literals", lintPgEscape.some((p) => p.severity === "error"), JSON.stringify(lintPgEscape));
const lintHashComment = lintSql("SELECT 1; # , (\nSELECT 2;", "mysql");
assert("MySQL hash comment masks punctuation", lintHashComment.length === 0, JSON.stringify(lintHashComment));

const result = { ok: failures.length === 0, failures };
console.log(JSON.stringify(result, null, 2));
if (failures.length) process.exit(1);

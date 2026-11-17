# LuxBase

A Navicat-style, multi-protocol database client for macOS — built with **Tauri 2, Rust, React and Monaco**, and extended with a local **MCP server** so AI agents can query your saved connections without ever seeing their passwords.

**Protocols:** MySQL · PostgreSQL · SQLite · Redis · Elasticsearch (7.x/8.x, OpenSearch)

## Highlights

- **SQL workbench** — Monaco editor, per-tab sessions, Navicat-style filters (mixed AND/OR groups), result export (CSV / JSON Lines), column resize everywhere.
- **Grid editing done right** — double-click a cell to edit (single-table results including aliased `FROM t AS x`), stage changes across rows and columns, then apply them as **one UPDATE per row inside a single transaction** with full rollback on failure. BLOB cells get HEX/text previews and binary editing.
- **Redis** — DB-aware keyspace with binary-safe keys/values (UTF-8 / GBK / hex / base64 views), type editors, TTL, embedded command console, big-DB friendly SCAN iteration.
- **Elasticsearch** — index CRUD, mappings, aliases, document browse/edit/delete, Query DSL, REST console, cluster info.
- **Transfer / sync / structure compare** — single-primary-key keyset pagination, batched transactional commits, per-table COUNT with rate + ETA, optional secondary-index drop & recreate and uniqueness/foreign-key check toggles for very large tables.
- **Safety by default** — AES-256-GCM local vault (no plaintext credentials, no server uploads), confirmed dangerous operations, append-only audit log, connection import/export with passphrase-sealed envelopes.
- **AI-agent ready** — ships `luxbase-mcp`, a stdio MCP server with read-only tools by default and an opt-in, fully audited write tool.

## Install (macOS, Apple Silicon)

1. Download `LuxBase_<version>_aarch64.dmg` and drag **LuxBase.app** into Applications.
2. The bundle is ad-hoc signed (not notarized). On first launch: right-click → Open, or run
   `xattr -dr com.apple.quarantine /Applications/LuxBase.app`.
3. Create a connection. Credentials are encrypted into the local vault at
   `~/Library/Application Support/com.luxbase.app/` — nothing leaves your machine.

The DMG also contains a Chinese install guide (`安装说明.txt`).

## Build from source

Requirements: macOS, Rust (stable), Node.js 20+.

```bash
npm install
npm run validate                     # UI checks + audit gates + JS/Rust guards
node scripts/build-macos-app.js --dmg  # release app + DMG (includes luxbase-mcp)
```

Rust tests (unit + SQLite live suites):

```bash
cd src-tauri && cargo test --lib
```

Optional live suites run only against **disposable** servers you point them at, and every object they create uses the `luxbase_qa_` / `luxbase_live` prefixes:

```bash
LUXBASE_LIVE_MYSQL="127.0.0.1|3306|user|password|testdb" cargo test --lib
LUXBASE_LIVE_POSTGRES="127.0.0.1|5432|user|password|testdb" cargo test --lib
LUXBASE_LIVE_REDIS="127.0.0.1|6379|password" cargo test --lib
```

## MCP: let AI agents use your connections

`luxbase-mcp` is installed inside the app bundle and speaks newline-delimited JSON-RPC 2.0 over stdio. It works without the LuxBase UI running.

```json
{
  "mcpServers": {
    "luxbase": {
      "command": "/Applications/LuxBase.app/Contents/MacOS/luxbase-mcp",
      "args": []
    }
  }
}
```

Read-only tools: `list_connections`, `list_tables`, `describe_table`, `query` (single SELECT/SHOW/DESCRIBE/EXPLAIN, 100 rows by default, 1000 max), `redis_scan`, `redis_get`, `es_search`.

Start with `"args": ["--allow-writes"]` to also expose `execute` (single write statement). Connections are referenced by id, passwords never leave the vault, and every agent statement is appended to the audit log. Use `--data-dir PATH` (or `LUXBASE_DATA_DIR`) to point at an alternate LuxBase data directory.

## Security & privacy

- All connection metadata, drafts and audit entries live in the local LuxBase data folder; credentials are sealed with AES-256-GCM. See [DATA-SECURITY.md](DATA-SECURITY.md).
- Dangerous operations (drops, flushes, bulk deletes) always require confirmation.
- Legacy Arcbase Studio data is migrated copy-only on first launch; the old folder is left untouched.
- Please report vulnerabilities privately to the maintainer instead of opening public issues with live credentials attached.

## License

[MIT](LICENSE) © 2026 charl — developed and built by charl.

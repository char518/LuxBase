use crate::error::{AppError, AppResult};
use crate::models::{
    AuditEntry, ConnectionConfig, LegacyImportReport, Protocol, QueryDraft, QueryHistoryEntry,
    SavedConnection, SecurityState,
};
use crate::security::SecretStore;
use chrono::Utc;
use parking_lot::Mutex;
use rusqlite::{params, Connection, OptionalExtension, Transaction};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::time::Duration;
use uuid::Uuid;

pub struct LocalStore {
    path: PathBuf,
    connection: Mutex<Connection>,
    secrets: SecretStore,
}

/// Vault accounts keep the keychain naming, so credentials saved by older builds migrate in place.
fn password_account(id: &str) -> String {
    format!("connection:{id}:password")
}

fn ssh_passphrase_account(id: &str) -> String {
    format!("connection:{id}:ssh-passphrase")
}

fn ssh_password_account(id: &str) -> String {
    format!("connection:{id}:ssh-password")
}

impl LocalStore {
    pub fn open(path: PathBuf, secrets: SecretStore) -> AppResult<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let connection = Connection::open(&path)?;
        connection.busy_timeout(Duration::from_secs(5))?;
        connection.pragma_update(None, "journal_mode", "WAL")?;
        connection.pragma_update(None, "synchronous", "FULL")?;
        connection.pragma_update(None, "foreign_keys", "ON")?;
        connection.pragma_update(None, "secure_delete", "ON")?;
        let store = Self {
            path,
            connection: Mutex::new(connection),
            secrets,
        };
        store.migrate()?;
        Ok(store)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn security_state(&self) -> SecurityState {
        SecurityState {
            local_vault_available: self.secrets.vault_available(),
            encryption_at_rest: true,
            audit_append_only: true,
            message: if self.secrets.vault_available() {
                "LuxBase keeps connection metadata, saved SQL, passwords and the vault key inside its own data folder, encrypted with AES-256-GCM. The system keychain is not read at launch; recovering credentials from an older version is a one-time action you can trigger here."
                    .into()
            } else {
                format!(
                    "The local vault key at {} is not readable or writable. Passwords and saved SQL cannot be persisted until the LuxBase data folder is usable again.",
                    self.secrets.vault_path().display()
                )
            },
        }
    }

    fn migrate(&self) -> AppResult<()> {
        let mut conn = self.connection.lock();
        let tx = conn.transaction()?;
        tx.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version INTEGER PRIMARY KEY,
                applied_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS saved_connections (
                id TEXT PRIMARY KEY NOT NULL,
                name TEXT NOT NULL,
                protocol TEXT NOT NULL,
                host_summary TEXT NOT NULL,
                database_name TEXT NOT NULL DEFAULT '',
                encrypted_config TEXT NOT NULL,
                has_ssh INTEGER NOT NULL DEFAULT 0,
                tls_enabled INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS app_secrets (
                account TEXT PRIMARY KEY NOT NULL,
                ciphertext TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS query_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                connection_id TEXT NOT NULL,
                sql_text TEXT NOT NULL,
                status TEXT NOT NULL,
                duration_ms INTEGER,
                rows_affected INTEGER,
                error_class TEXT,
                executed_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_query_history_connection_time
                ON query_history(connection_id, executed_at DESC);

            CREATE TABLE IF NOT EXISTS query_drafts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                connection_id TEXT NOT NULL,
                name TEXT NOT NULL,
                sql_text TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_query_drafts_connection
                ON query_drafts(connection_id, updated_at DESC);

            CREATE TABLE IF NOT EXISTS audit_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                occurred_at TEXT NOT NULL,
                connection_id TEXT,
                connection_label TEXT,
                protocol TEXT,
                event_kind TEXT NOT NULL,
                command_summary TEXT NOT NULL,
                affected_rows INTEGER,
                outcome TEXT NOT NULL,
                metadata_json TEXT,
                previous_hash TEXT NOT NULL,
                entry_hash TEXT NOT NULL UNIQUE
            );

            CREATE TRIGGER IF NOT EXISTS audit_log_no_update
            BEFORE UPDATE ON audit_log
            BEGIN
                SELECT RAISE(ABORT, 'audit_log is append-only');
            END;

            CREATE TRIGGER IF NOT EXISTS audit_log_no_delete
            BEFORE DELETE ON audit_log
            BEGIN
                SELECT RAISE(ABORT, 'audit_log is append-only');
            END;
            "#,
        )?;
        tx.execute(
            "INSERT OR IGNORE INTO schema_migrations(version, applied_at) VALUES (1, ?1)",
            [Utc::now().to_rfc3339()],
        )?;
        tx.commit()?;

        let encrypted_queries = conn
            .query_row("SELECT 1 FROM schema_migrations WHERE version = 2", [], |row| row.get::<_, i64>(0))
            .optional()?
            .is_some();
        if !encrypted_queries && self.secrets.vault_available() {
            if let Err(error) = self.encrypt_existing_sql_rows(&mut conn) {
                // An unusable vault key must not lock the user out of the app. The rows stay
                // plaintext and version 2 remains unset, so the next launch retries.
                tracing::warn!(%error, "deferred the saved-SQL encryption migration");
            }
        }
        Ok(())
    }

    fn encrypt_existing_sql_rows(&self, conn: &mut Connection) -> AppResult<()> {
        let history = {
            let mut statement = conn.prepare("SELECT id, sql_text FROM query_history")?;
            let rows = statement
                .query_map([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)))?
                .collect::<Result<Vec<_>, _>>()?;
            rows
        };
        let drafts = {
            let mut statement = conn.prepare("SELECT id, sql_text FROM query_drafts")?;
            let rows = statement
                .query_map([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)))?
                .collect::<Result<Vec<_>, _>>()?;
            rows
        };
        let encrypted_history = history
            .into_iter()
            .map(|(id, sql)| Ok((id, self.secrets.encrypt_json(&sql)?)))
            .collect::<AppResult<Vec<_>>>()?;
        let encrypted_drafts = drafts
            .into_iter()
            .map(|(id, sql)| Ok((id, self.secrets.encrypt_json(&sql)?)))
            .collect::<AppResult<Vec<_>>>()?;
        let migration = conn.transaction()?;
        for (id, sql) in encrypted_history {
            migration.execute("UPDATE query_history SET sql_text = ?1 WHERE id = ?2", params![sql, id])?;
        }
        for (id, sql) in encrypted_drafts {
            migration.execute("UPDATE query_drafts SET sql_text = ?1 WHERE id = ?2", params![sql, id])?;
        }
        migration.execute(
            "INSERT INTO schema_migrations(version, applied_at) VALUES (2, ?1)",
            [Utc::now().to_rfc3339()],
        )?;
        migration.commit()?;
        Ok(())
    }

    fn reveal_sql(&self, stored: &str) -> String {
        decode_stored_sql(&self.secrets, stored)
    }

    fn store_secret(&self, account: &str, value: &str) -> AppResult<()> {
        let ciphertext = self.secrets.encrypt_json(&value)?;
        let conn = self.connection.lock();
        conn.execute(
            r#"
            INSERT INTO app_secrets (account, ciphertext, updated_at) VALUES (?1, ?2, ?3)
            ON CONFLICT(account) DO UPDATE SET
                ciphertext = excluded.ciphertext,
                updated_at = excluded.updated_at
            "#,
            params![account, ciphertext, Utc::now().to_rfc3339()],
        )?;
        Ok(())
    }

    /// Reads a credential from the local vault. A missing row means "no password saved"; the OS
    /// keychain is never consulted here, because that read asks macOS for permission on every
    /// launch once the app has been re-signed. Recovery of pre-vault credentials happens only
    /// through [`LocalStore::import_legacy_keychain_secrets`], which the user triggers.
    fn read_secret(&self, account: &str) -> AppResult<Option<String>> {
        let stored: Option<String> = {
            let conn = self.connection.lock();
            conn.query_row(
                "SELECT ciphertext FROM app_secrets WHERE account = ?1",
                [account],
                |row| row.get(0),
            )
            .optional()?
        };
        stored.map(|ciphertext| self.secrets.decrypt_json(&ciphertext)).transpose()
    }

    fn remove_secret(&self, account: &str) -> AppResult<()> {
        let conn = self.connection.lock();
        conn.execute("DELETE FROM app_secrets WHERE account = ?1", [account])?;
        Ok(())
    }

    fn secret_is_stored(&self, account: &str) -> AppResult<bool> {
        let conn = self.connection.lock();
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM app_secrets WHERE account = ?1",
            [account],
            |row| row.get(0),
        )?;
        Ok(count > 0)
    }

    /// Every vault account the saved connections could have a credential under.
    fn legacy_candidate_accounts(&self) -> AppResult<Vec<String>> {
        let ids: Vec<String> = {
            let conn = self.connection.lock();
            let mut statement = conn.prepare("SELECT id FROM saved_connections")?;
            let rows = statement
                .query_map([], |row| row.get::<_, String>(0))?
                .collect::<Result<Vec<_>, _>>()?;
            rows
        };
        Ok(ids
            .iter()
            .flat_map(|id| {
                [
                    password_account(id),
                    ssh_passphrase_account(id),
                    ssh_password_account(id),
                ]
            })
            .collect())
    }

    /// One-time recovery of the credentials an older build kept in the OS keychain.
    ///
    /// `legacy` decides whether the keychain is really read: only a store built with
    /// [`SecretStore::with_legacy_import`] reports anything, so the hermetic tests pass an ordinary
    /// store and prove the vault is left untouched. Existing vault rows always win, and an imported
    /// item is deleted from the keychain so nothing can offer it twice.
    pub fn import_legacy_secrets_from(&self, legacy: &SecretStore) -> AppResult<LegacyImportReport> {
        let mut report = LegacyImportReport::default();
        report.config_key_imported = legacy.install_legacy_config_key()?;
        for account in self.legacy_candidate_accounts()? {
            if self.secret_is_stored(&account)? {
                report.kept_existing += 1;
                continue;
            }
            match legacy.legacy_get(&account) {
                Ok(Some(value)) => {
                    self.store_secret(&account, &value)?;
                    report.imported += 1;
                    if let Err(error) = legacy.legacy_delete(&account) {
                        tracing::warn!(%error, account, "imported a legacy credential but left the keychain copy in place");
                        report.keychain_cleanup_failed += 1;
                    }
                }
                Ok(None) => report.missing += 1,
                Err(error) => {
                    tracing::warn!(%error, account, "the keychain refused a legacy credential read");
                    report.access_denied += 1;
                }
            }
        }
        self.append_audit_inner(
            None,
            None,
            None,
            "legacy_secret_import",
            &format!(
                "Keychain recovery imported {} credential(s); {} already in the vault, {} not found, {} refused",
                report.imported, report.kept_existing, report.missing, report.access_denied
            ),
            Some(report.imported as i64),
            "success",
            None,
        )?;
        Ok(report)
    }

    /// The command-facing entry point: reads the OS keychain exactly once, on user request.
    pub fn import_legacy_keychain_secrets(&self) -> AppResult<LegacyImportReport> {
        let legacy = SecretStore::with_legacy_import(self.secrets.vault_path().to_path_buf());
        self.import_legacy_secrets_from(&legacy)
    }

    pub fn save_connection(
        &self,
        config: &ConnectionConfig,
        password: Option<&str>,
        ssh_passphrase: Option<&str>,
        ssh_password: Option<&str>,
    ) -> AppResult<SavedConnection> {
        if config.name.trim().is_empty() {
            return Err(AppError::validation("Connection name is required."));
        }
        if matches!(config.protocol, Protocol::Mysql | Protocol::Postgres)
            && config.host.trim().is_empty()
        {
            return Err(AppError::validation("Host is required for this SQL protocol."));
        }
        if config.protocol == Protocol::Sqlite && config.file_path.as_deref().unwrap_or("").is_empty() {
            return Err(AppError::validation("SQLite database file is required."));
        }
        if config.protocol == Protocol::Redis {
            match config.redis_mode.unwrap_or(crate::models::RedisMode::Standalone) {
                crate::models::RedisMode::Standalone if config.host.trim().is_empty() => {
                    return Err(AppError::validation("Host is required for standalone Redis."));
                }
                crate::models::RedisMode::Sentinel => {
                    if config.sentinels.is_empty() {
                        return Err(AppError::validation(
                            "At least one Redis Sentinel node is required.",
                        ));
                    }
                    if config
                        .sentinel_master_name
                        .as_deref()
                        .unwrap_or("")
                        .trim()
                        .is_empty()
                    {
                        return Err(AppError::validation(
                            "Redis Sentinel master name is required.",
                        ));
                    }
                }
                crate::models::RedisMode::Cluster if config.cluster_nodes.is_empty() => {
                    return Err(AppError::validation(
                        "At least one Redis Cluster node is required.",
                    ));
                }
                _ => {}
            }
        }
        if config.protocol == Protocol::Elasticsearch && config.host.trim().is_empty() {
            return Err(AppError::validation("Host is required for Elasticsearch."));
        }
        if config.pool.max_size == 0 {
            return Err(AppError::validation("Pool max_size must be at least 1."));
        }
        if config.pool.min_idle > config.pool.max_size {
            return Err(AppError::validation("Pool min_idle cannot exceed max_size."));
        }
        if config.query_timeout_ms < 1_000 {
            return Err(AppError::validation(
                "Query timeout must be at least 1000 milliseconds.",
            ));
        }

        if let Some(password) = password {
            self.store_secret(&password_account(&config.id), password)?;
        }
        if let Some(passphrase) = ssh_passphrase {
            self.store_secret(&ssh_passphrase_account(&config.id), passphrase)?;
        }
        if let Some(ssh_password) = ssh_password {
            self.store_secret(&ssh_password_account(&config.id), ssh_password)?;
        }

        let encrypted = self.secrets.encrypt_json(config)?;
        let now = Utc::now().to_rfc3339();
        let host_summary = match config.protocol {
            Protocol::Sqlite => config.file_path.clone().unwrap_or_default(),
            Protocol::Redis if config.redis_mode == Some(crate::models::RedisMode::Cluster) => {
                format!("{} cluster nodes", config.cluster_nodes.len())
            }
            Protocol::Redis if config.redis_mode == Some(crate::models::RedisMode::Sentinel) => {
                format!("{} sentinels", config.sentinels.len())
            }
            _ => format!("{}:{}", config.host, config.port),
        };
        let saved = SavedConnection {
            id: config.id.clone(),
            name: config.name.clone(),
            protocol: config.protocol,
            host_summary,
            database: config.database.clone(),
            updated_at: now.clone(),
            has_ssh: config.ssh.is_some(),
            tls_enabled: !matches!(
                config.tls_mode.unwrap_or(crate::models::TlsMode::Disabled),
                crate::models::TlsMode::Disabled
            ),
            redis_db: config.redis_db,
            redis_mode: config.redis_mode,
        };

        let conn = self.connection.lock();
        conn.execute(
            r#"
            INSERT INTO saved_connections (
                id, name, protocol, host_summary, database_name, encrypted_config,
                has_ssh, tls_enabled, created_at, updated_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?9)
            ON CONFLICT(id) DO UPDATE SET
                name = excluded.name,
                protocol = excluded.protocol,
                host_summary = excluded.host_summary,
                database_name = excluded.database_name,
                encrypted_config = excluded.encrypted_config,
                has_ssh = excluded.has_ssh,
                tls_enabled = excluded.tls_enabled,
                updated_at = excluded.updated_at
            "#,
            params![
                saved.id,
                saved.name,
                saved.protocol.as_str(),
                saved.host_summary,
                saved.database,
                encrypted,
                saved.has_ssh as i64,
                saved.tls_enabled as i64,
                now
            ],
        )?;
        drop(conn);
        self.append_audit_inner(
            Some(&config.id),
            Some(&config.name),
            Some(config.protocol.as_str()),
            "connection_saved",
            &format!("Saved {} connection", config.protocol.as_str()),
            None,
            "success",
            None,
        )?;
        Ok(saved)
    }

    pub fn load_connection_config(&self, id: &str) -> AppResult<ConnectionConfig> {
        let conn = self.connection.lock();
        let encrypted: Option<String> = conn
            .query_row(
                "SELECT encrypted_config FROM saved_connections WHERE id = ?1",
                [id],
                |row| row.get(0),
            )
            .optional()?;
        let encrypted = encrypted.ok_or_else(|| {
            AppError::new(
                crate::models::ErrorClass::Validation,
                format!("Connection '{id}' does not exist."),
            )
        })?;
        drop(conn);
        self.secrets.decrypt_json(&encrypted)
    }

    pub fn connection_password(&self, id: &str) -> AppResult<Option<String>> {
        self.read_secret(&password_account(id))
    }

    pub fn ssh_passphrase(&self, id: &str) -> AppResult<Option<String>> {
        self.read_secret(&ssh_passphrase_account(id))
    }

    pub fn ssh_password(&self, id: &str) -> AppResult<Option<String>> {
        self.read_secret(&ssh_password_account(id))
    }

    pub fn delete_connection(&self, id: &str) -> AppResult<()> {
        let conn = self.connection.lock();
        let deleted = conn.execute("DELETE FROM saved_connections WHERE id = ?1", [id])?;
        if deleted == 0 {
            return Err(AppError::validation("The connection does not exist."));
        }
        drop(conn);
        self.remove_secret(&password_account(id))?;
        self.remove_secret(&ssh_passphrase_account(id))?;
        self.remove_secret(&ssh_password_account(id))?;
        Ok(())
    }

    pub fn list_connections(&self) -> AppResult<Vec<SavedConnection>> {
        let conn = self.connection.lock();
        let mut statement = conn.prepare(
            r#"
            SELECT id, name, protocol, host_summary, database_name, updated_at, has_ssh, tls_enabled, encrypted_config
            FROM saved_connections
            ORDER BY updated_at DESC
            "#,
        )?;
        let rows = statement
            .query_map([], |row| {
                let protocol: String = row.get(2)?;
                Ok((
                    SavedConnection {
                        id: row.get(0)?,
                        name: row.get(1)?,
                        protocol: match protocol.as_str() {
                            "mysql" => Protocol::Mysql,
                            "postgres" => Protocol::Postgres,
                            "sqlite" => Protocol::Sqlite,
                            "elasticsearch" => Protocol::Elasticsearch,
                            _ => Protocol::Redis,
                        },
                        host_summary: row.get(3)?,
                        database: row.get(4)?,
                        updated_at: row.get(5)?,
                        has_ssh: row.get::<_, i64>(6)? != 0,
                        tls_enabled: row.get::<_, i64>(7)? != 0,
                        redis_db: 0,
                        redis_mode: None,
                    },
                    row.get::<_, String>(8)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        drop(statement);
        drop(conn);
        rows.into_iter()
            .map(|(mut saved, encrypted)| {
                let config: ConnectionConfig = self.secrets.decrypt_json(&encrypted)?;
                saved.redis_db = config.redis_db;
                saved.redis_mode = config.redis_mode;
                Ok(saved)
            })
            .collect()
    }

    pub fn record_query(
        &self,
        connection_id: &str,
        sql_text: &str,
        status: &str,
        duration_ms: Option<u64>,
        rows_affected: Option<u64>,
        error_class: Option<&str>,
    ) -> AppResult<()> {
        let encrypted_sql = self.secrets.encrypt_json(&sql_text)?;
        let conn = self.connection.lock();
        conn.execute(
            r#"
            INSERT INTO query_history(
                connection_id, sql_text, status, duration_ms, rows_affected, error_class, executed_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
            "#,
            params![
                connection_id,
                encrypted_sql,
                status,
                duration_ms.map(|value| value as i64),
                rows_affected.map(|value| value as i64),
                error_class,
                Utc::now().to_rfc3339()
            ],
        )?;
        Ok(())
    }

    pub fn save_query_draft(
        &self,
        connection_id: &str,
        name: &str,
        sql_text: &str,
        draft_id: Option<i64>,
    ) -> AppResult<QueryDraft> {
        let name = name.trim();
        if name.is_empty() {
            return Err(AppError::validation("Query draft name is required."));
        }
        if sql_text.trim().is_empty() {
            return Err(AppError::validation("A query draft cannot be empty."));
        }
        let now = Utc::now().to_rfc3339();
        let encrypted_sql = self.secrets.encrypt_json(&sql_text)?;
        let conn = self.connection.lock();
        let id = if let Some(id) = draft_id {
            let changed = conn.execute(
                "UPDATE query_drafts SET name = ?1, sql_text = ?2, updated_at = ?3 WHERE id = ?4 AND connection_id = ?5",
                params![name, encrypted_sql, now, id, connection_id],
            )?;
            if changed != 1 { return Err(AppError::validation("The saved query no longer exists in this connection.")); }
            id
        } else {
            conn.execute(
                "INSERT INTO query_drafts(connection_id, name, sql_text, created_at, updated_at) VALUES (?1, ?2, ?3, ?4, ?4)",
                params![connection_id, name, encrypted_sql, now],
            )?;
            conn.last_insert_rowid()
        };
        drop(conn);
        self.list_query_drafts(connection_id)?
            .into_iter()
            .find(|draft| draft.id == id)
            .ok_or_else(|| AppError::validation("The saved query no longer exists in this connection."))
    }

    pub fn rename_query_draft(&self, connection_id: &str, id: i64, name: &str) -> AppResult<QueryDraft> {
        let name = name.trim();
        if name.is_empty() { return Err(AppError::validation("Query name cannot be empty.")); }
        let changed = self.connection.lock().execute(
            "UPDATE query_drafts SET name = ?1, updated_at = ?2 WHERE id = ?3 AND connection_id = ?4",
            params![name, Utc::now().to_rfc3339(), id, connection_id],
        )?;
        if changed != 1 { return Err(AppError::validation("The saved query no longer exists.")); }
        self.list_query_drafts(connection_id)?.into_iter().find(|draft| draft.id == id)
            .ok_or_else(|| AppError::validation("The saved query no longer exists."))
    }

    pub fn delete_query_draft(&self, connection_id: &str, id: i64) -> AppResult<()> {
        let changed = self.connection.lock().execute(
            "DELETE FROM query_drafts WHERE id = ?1 AND connection_id = ?2", params![id, connection_id],
        )?;
        if changed != 1 { return Err(AppError::validation("The saved query no longer exists.")); }
        Ok(())
    }

    pub fn list_query_drafts(&self, connection_id: &str) -> AppResult<Vec<QueryDraft>> {
        let conn = self.connection.lock();
        let mut statement = conn.prepare(
            r#"
            SELECT id, connection_id, name, sql_text, created_at, updated_at
            FROM query_drafts WHERE connection_id = ?1
            ORDER BY updated_at DESC, id DESC
            "#,
        )?;
        let rows = statement
            .query_map([connection_id], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        drop(statement);
        drop(conn);
        rows.into_iter()
            .map(|(id, connection_id, name, encrypted_sql, created_at, updated_at)| {
                Ok(QueryDraft {
                    id,
                    connection_id,
                    name,
                    sql_text: self.reveal_sql(&encrypted_sql),
                    created_at,
                    updated_at,
                })
            })
            .collect()
    }

    pub fn recent_history(&self, limit: usize) -> AppResult<Vec<QueryHistoryEntry>> {
        let conn = self.connection.lock();
        let mut statement = conn.prepare(
            r#"
            SELECT id, connection_id, sql_text, status, duration_ms, rows_affected, error_class, executed_at
            FROM query_history ORDER BY id DESC LIMIT ?1
            "#,
        )?;
        let rows = statement
            .query_map([limit as i64], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, Option<i64>>(4)?,
                    row.get::<_, Option<i64>>(5)?,
                    row.get::<_, Option<String>>(6)?,
                    row.get::<_, String>(7)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        drop(statement);
        drop(conn);
        rows.into_iter()
            .map(|(id, connection_id, encrypted_sql, status, duration_ms, rows_affected, error_class, executed_at)| {
                Ok(QueryHistoryEntry {
                    id,
                    connection_id,
                    sql_text: self.reveal_sql(&encrypted_sql),
                    status,
                    duration_ms,
                    rows_affected,
                    error_class,
                    executed_at,
                })
            })
            .collect()
    }

    pub fn append_audit(
        &self,
        connection_id: Option<&str>,
        connection_label: Option<&str>,
        protocol: Option<&str>,
        event_kind: &str,
        command_summary: &str,
        affected_rows: Option<i64>,
        outcome: &str,
        metadata_json: Option<&str>,
    ) -> AppResult<i64> {
        self.append_audit_inner(
            connection_id,
            connection_label,
            protocol,
            event_kind,
            command_summary,
            affected_rows,
            outcome,
            metadata_json,
        )
    }

    fn append_audit_inner(
        &self,
        connection_id: Option<&str>,
        connection_label: Option<&str>,
        protocol: Option<&str>,
        event_kind: &str,
        command_summary: &str,
        affected_rows: Option<i64>,
        outcome: &str,
        metadata_json: Option<&str>,
    ) -> AppResult<i64> {
        let mut conn = self.connection.lock();
        let tx = conn.transaction()?;
        Self::append_audit_tx(
            &tx,
            connection_id,
            connection_label,
            protocol,
            event_kind,
            command_summary,
            affected_rows,
            outcome,
            metadata_json,
        )?;
        let id = tx.last_insert_rowid();
        tx.commit()?;
        Ok(id)
    }

    #[allow(clippy::too_many_arguments)]
    fn append_audit_tx(
        tx: &Transaction<'_>,
        connection_id: Option<&str>,
        connection_label: Option<&str>,
        protocol: Option<&str>,
        event_kind: &str,
        command_summary: &str,
        affected_rows: Option<i64>,
        outcome: &str,
        metadata_json: Option<&str>,
    ) -> AppResult<()> {
        let previous_hash: String = tx
            .query_row(
                "SELECT entry_hash FROM audit_log ORDER BY id DESC LIMIT 1",
                [],
                |row| row.get(0),
            )
            .optional()?
            .unwrap_or_else(|| "GENESIS".to_string());
        let occurred_at = Utc::now().to_rfc3339();
        let payload = format!(
            "{previous_hash}|{occurred_at}|{}|{}|{}|{}|{}|{}|{}|{}",
            connection_id.unwrap_or(""),
            connection_label.unwrap_or(""),
            protocol.unwrap_or(""),
            event_kind,
            command_summary,
            affected_rows.map(|value| value.to_string()).unwrap_or_default(),
            outcome,
            metadata_json.unwrap_or("")
        );
        let entry_hash = hex::encode(Sha256::digest(payload.as_bytes()));
        tx.execute(
            r#"
            INSERT INTO audit_log(
                occurred_at, connection_id, connection_label, protocol, event_kind,
                command_summary, affected_rows, outcome, metadata_json, previous_hash, entry_hash
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
            "#,
            params![
                occurred_at,
                connection_id,
                connection_label,
                protocol,
                event_kind,
                command_summary,
                affected_rows,
                outcome,
                metadata_json,
                previous_hash,
                entry_hash
            ],
        )?;
        Ok(())
    }

    pub fn recent_audit(&self, limit: usize) -> AppResult<Vec<AuditEntry>> {
        let conn = self.connection.lock();
        let mut statement = conn.prepare(
            r#"
            SELECT id, occurred_at, connection_id, connection_label, protocol,
                   event_kind, command_summary, affected_rows, outcome, metadata_json
            FROM audit_log ORDER BY id DESC LIMIT ?1
            "#,
        )?;
        let rows = statement.query_map([limit as i64], |row| {
            Ok(AuditEntry {
                id: row.get(0)?,
                occurred_at: row.get(1)?,
                connection_id: row.get(2)?,
                connection_label: row.get(3)?,
                protocol: row.get(4)?,
                event_kind: row.get(5)?,
                command_summary: row.get(6)?,
                affected_rows: row.get(7)?,
                outcome: row.get(8)?,
                metadata_json: row.get(9)?,
            })
        })?;
        rows.collect::<Result<Vec<_>, _>>().map_err(AppError::from)
    }

    pub fn verify_audit_chain(&self) -> AppResult<bool> {
        let conn = self.connection.lock();
        let mut statement = conn.prepare(
            r#"
            SELECT occurred_at, connection_id, connection_label, protocol, event_kind,
                   command_summary, affected_rows, outcome, metadata_json, previous_hash, entry_hash
            FROM audit_log ORDER BY id ASC
            "#,
        )?;
        let mut rows = statement.query([])?;
        let mut expected_previous = "GENESIS".to_string();
        while let Some(row) = rows.next()? {
            let previous_hash: String = row.get(9)?;
            if previous_hash != expected_previous {
                return Ok(false);
            }
            let occurred_at: String = row.get(0)?;
            let connection_id: Option<String> = row.get(1)?;
            let connection_label: Option<String> = row.get(2)?;
            let protocol: Option<String> = row.get(3)?;
            let event_kind: String = row.get(4)?;
            let command_summary: String = row.get(5)?;
            let affected_rows: Option<i64> = row.get(6)?;
            let outcome: String = row.get(7)?;
            let metadata_json: Option<String> = row.get(8)?;
            let entry_hash: String = row.get(10)?;
            let payload = format!(
                "{previous_hash}|{occurred_at}|{}|{}|{}|{}|{}|{}|{}|{}",
                connection_id.unwrap_or_default(),
                connection_label.unwrap_or_default(),
                protocol.unwrap_or_default(),
                event_kind,
                command_summary,
                affected_rows.map(|value| value.to_string()).unwrap_or_default(),
                outcome,
                metadata_json.unwrap_or_default()
            );
            let computed = hex::encode(Sha256::digest(payload.as_bytes()));
            if computed != entry_hash {
                return Ok(false);
            }
            expected_previous = entry_hash;
        }
        Ok(true)
    }
}

/// One-time, copy-only migration from the pre-rename Arcbase Studio data folder. The legacy
/// folder is left completely untouched so the rename stays reversible; nothing is moved or deleted.
pub fn migrate_legacy_data_dir(new_dir: &Path) -> std::io::Result<bool> {
    if new_dir.join("luxbase.sqlite3").exists() {
        return Ok(false);
    }
    let legacy_dir = match new_dir.parent() {
        Some(parent) => parent.join("com.arcbase.studio"),
        None => return Ok(false),
    };
    let legacy_db = legacy_dir.join("arcbase-studio.sqlite3");
    if !legacy_db.exists() {
        return Ok(false);
    }
    std::fs::create_dir_all(new_dir)?;
    std::fs::copy(&legacy_db, new_dir.join("luxbase.sqlite3"))?;
    for suffix in ["-wal", "-shm"] {
        let sidecar = legacy_dir.join(format!("arcbase-studio.sqlite3{suffix}"));
        if sidecar.exists() {
            std::fs::copy(&sidecar, new_dir.join(format!("luxbase.sqlite3{suffix}")))?;
        }
    }
    let legacy_key = legacy_dir.join("vault.key");
    if legacy_key.exists() {
        std::fs::copy(&legacy_key, new_dir.join("vault.key"))?;
    }
    Ok(true)
}

pub fn new_connection_id() -> String {
    Uuid::new_v4().to_string()
}

fn decode_stored_sql(secrets: &SecretStore, stored: &str) -> String {
    if !stored.starts_with("aes-gcm-v1:") {
        return stored.to_string();
    }
    match secrets.decrypt_json::<String>(stored) {
        Ok(sql) => sql,
        Err(_) => "[本地历史内容无法解密]".to_string(),
    }
}

#[cfg(test)]
mod sql_storage_tests {
    use super::decode_stored_sql;

    fn test_secrets() -> crate::security::SecretStore {
        crate::security::SecretStore::new(
            std::env::temp_dir().join(format!("luxbase-storage-test-{}", std::process::id())),
        )
    }

    #[test]
    fn rows_written_before_the_encryption_migration_stay_readable() {
        let secrets = test_secrets();
        assert_eq!(decode_stored_sql(&secrets, "SELECT 1;"), "SELECT 1;");
    }

    #[test]
    fn undecryptable_envelopes_degrade_to_a_placeholder_instead_of_failing_the_list() {
        let secrets = test_secrets();
        assert_eq!(
            decode_stored_sql(&secrets, "aes-gcm-v1:AAAA"),
            "[本地历史内容无法解密]"
        );
    }
}

#[cfg(test)]
mod local_vault_tests {
    use super::*;
    use serde_json::json;

    struct TempVaultDir(PathBuf);

    impl TempVaultDir {
        fn new(tag: &str) -> Self {
            let dir = std::env::temp_dir().join(format!("luxbase-store-{}-{tag}", std::process::id()));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).unwrap();
            Self(dir)
        }

        fn secrets(&self) -> SecretStore {
            SecretStore::new(self.0.join("vault.key"))
        }

        fn open(&self) -> LocalStore {
            LocalStore::open(self.0.join("store.sqlite3"), self.secrets()).unwrap()
        }
    }

    impl Drop for TempVaultDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn sqlite_config(id: &str, file: &Path) -> ConnectionConfig {
        serde_json::from_value(json!({
            "id": id,
            "name": id,
            "protocol": "sqlite",
            "filePath": file.to_string_lossy()
        }))
        .expect("valid sqlite config")
    }

    #[test]
    fn rename_migration_copies_legacy_data_and_never_touches_the_legacy_folder() {
        let parent = std::env::temp_dir().join(format!("luxbase-migrate-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&parent);
        let legacy = parent.join("com.arcbase.studio");
        std::fs::create_dir_all(&legacy).unwrap();
        std::fs::write(legacy.join("arcbase-studio.sqlite3"), b"legacy-db").unwrap();
        std::fs::write(legacy.join("arcbase-studio.sqlite3-wal"), b"legacy-wal").unwrap();
        std::fs::write(legacy.join("vault.key"), b"legacy-key").unwrap();
        let new_dir = parent.join("com.luxbase.app");
        assert!(migrate_legacy_data_dir(&new_dir).unwrap());
        assert_eq!(std::fs::read(new_dir.join("luxbase.sqlite3")).unwrap(), b"legacy-db");
        assert_eq!(std::fs::read(new_dir.join("luxbase.sqlite3-wal")).unwrap(), b"legacy-wal");
        assert_eq!(std::fs::read(new_dir.join("vault.key")).unwrap(), b"legacy-key");
        assert!(legacy.join("arcbase-studio.sqlite3").exists(), "the legacy folder stays intact");
        assert!(!migrate_legacy_data_dir(&new_dir).unwrap(), "migration is idempotent");
        let _ = std::fs::remove_dir_all(&parent);
    }

    #[test]
    fn credentials_round_trip_through_the_local_vault_and_survive_a_restart() {
        let dir = TempVaultDir::new("roundtrip");
        let store = dir.open();
        let config = sqlite_config("vault-roundtrip", &dir.0.join("data.sqlite3"));
        store
            .save_connection(
                &config,
                Some("db-secret"),
                Some("key-passphrase"),
                Some("ssh-secret"),
            )
            .unwrap();

        assert_eq!(store.connection_password("vault-roundtrip").unwrap().as_deref(), Some("db-secret"));
        assert_eq!(store.ssh_passphrase("vault-roundtrip").unwrap().as_deref(), Some("key-passphrase"));
        assert_eq!(store.ssh_password("vault-roundtrip").unwrap().as_deref(), Some("ssh-secret"));
        assert!(dir.0.join("vault.key").exists(), "the master key belongs to LuxBase");

        let reopened = dir.open();
        assert_eq!(
            reopened.connection_password("vault-roundtrip").unwrap().as_deref(),
            Some("db-secret"),
            "a restarted app must read the same vault key"
        );

        reopened.delete_connection("vault-roundtrip").unwrap();
        assert_eq!(reopened.connection_password("vault-roundtrip").unwrap(), None);
        assert_eq!(reopened.ssh_passphrase("vault-roundtrip").unwrap(), None);
        assert_eq!(reopened.ssh_password("vault-roundtrip").unwrap(), None);
    }

    #[test]
    fn vault_rows_never_store_the_credential_as_plaintext() {
        let dir = TempVaultDir::new("ciphertext");
        let store = dir.open();
        let config = sqlite_config("vault-ciphertext", &dir.0.join("data.sqlite3"));
        store.save_connection(&config, Some("do-not-leak-me"), None, None).unwrap();
        store.record_query("vault-ciphertext", "SELECT 1;", "success", None, None, None).unwrap();

        let mut stored: Vec<u8> = Vec::new();
        for name in ["store.sqlite3", "store.sqlite3-wal"] {
            if let Ok(bytes) = std::fs::read(dir.0.join(name)) {
                stored.extend_from_slice(&bytes);
            }
        }
        let secret = b"do-not-leak-me";
        assert!(
            !stored.windows(secret.len()).any(|window| window == secret),
            "the local database holds the credential as plaintext"
        );
    }

    #[test]
    fn saving_without_a_password_leaves_the_stored_credential_untouched() {
        let dir = TempVaultDir::new("keep");
        let store = dir.open();
        let mut config = sqlite_config("vault-keep", &dir.0.join("data.sqlite3"));
        store.save_connection(&config, Some("first"), None, None).unwrap();
        config.name = "renamed".into();
        store.save_connection(&config, None, None, None).unwrap();
        assert_eq!(store.connection_password("vault-keep").unwrap().as_deref(), Some("first"));
    }

    #[test]
    fn recovery_imports_nothing_without_a_keychain_reader_and_keeps_vault_rows() {
        let dir = TempVaultDir::new("recovery");
        let store = dir.open();
        let config = sqlite_config("vault-recovery", &dir.0.join("data.sqlite3"));
        store.save_connection(&config, Some("vault-only"), None, None).unwrap();

        // Only a `with_legacy_import` store can see the OS keychain, so recovery through an
        // ordinary store has to behave like an empty keychain: import nothing and touch nothing.
        let legacy = SecretStore::new(dir.0.join("other-vault.key"));
        let report = store.import_legacy_secrets_from(&legacy).unwrap();
        assert!(!report.config_key_imported, "a reader that cannot ask the keychain seeds no master key");
        assert_eq!(report.imported, 0);
        assert_eq!(report.kept_existing, 1, "the password already inside LuxBase wins");
        assert_eq!(report.missing, 2, "the two SSH accounts have no vault row and no legacy item");
        assert_eq!(report.access_denied, 0);
        assert!(
            !dir.0.join("other-vault.key").exists(),
            "recovery must not create a second vault the user would never back up"
        );
        assert_eq!(
            store.connection_password("vault-recovery").unwrap().as_deref(),
            Some("vault-only"),
            "the saved credential must survive a recovery attempt"
        );

        let reopened = dir.open();
        assert_eq!(
            reopened.connection_password("vault-recovery").unwrap().as_deref(),
            Some("vault-only"),
            "a restarted app reads the same vault"
        );
    }
}

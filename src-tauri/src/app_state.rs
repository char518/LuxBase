use crate::drivers::kv::KvConnectionManager;
use crate::drivers::search::SearchConnectionManager;
use crate::drivers::sql::SqlConnectionManager;
use crate::error::{AppError, AppResult};
use crate::jobs::JobTable;
use crate::models::{ConnectionConfig, SyncProgress, TransferProgress};
use crate::rowsync::PlanCache;
use crate::storage::LocalStore;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

pub struct AppState {
    pub store: Arc<LocalStore>,
    pub sql: SqlConnectionManager,
    pub kv: KvConnectionManager,
    pub search: SearchConnectionManager,
    /// Data transfer and row-diff work run on these so the UI can poll progress and stop them.
    pub transfer_jobs: JobTable<TransferProgress>,
    pub sync_jobs: JobTable<SyncProgress>,
    pub sync_plans: PlanCache,
    shutdown: AtomicBool,
    // Every command resolves its connection; decrypting the config and reading the vault
    // (a SQLite query plus an AES-GCM open) each time dominated small metadata requests.
    configs: Mutex<HashMap<String, ConnectionConfig>>,
    passwords: Mutex<HashMap<String, Option<String>>>,
}

impl AppState {
    pub fn new(store: LocalStore) -> Self {
        Self {
            store: Arc::new(store),
            sql: SqlConnectionManager::default(),
            kv: KvConnectionManager::default(),
            search: SearchConnectionManager::default(),
            transfer_jobs: JobTable::default(),
            sync_jobs: JobTable::default(),
            sync_plans: PlanCache::default(),
            shutdown: AtomicBool::new(false),
            configs: Mutex::new(HashMap::new()),
            passwords: Mutex::new(HashMap::new()),
        }
    }

    pub fn connection(&self, connection_id: &str) -> AppResult<ConnectionConfig> {
        if let Some(config) = self.configs.lock().get(connection_id) {
            return Ok(config.clone());
        }
        let mut config = self.store.load_connection_config(connection_id)?;
        config.ssh_passphrase = if config.ssh.is_some() {
            self.store.ssh_passphrase(&config.id)?
        } else {
            None
        };
        config.ssh_password = if config.ssh.is_some() {
            self.store.ssh_password(&config.id)?
        } else {
            None
        };
        self.configs
            .lock()
            .insert(connection_id.to_string(), config.clone());
        Ok(config)
    }

    pub fn connection_password(&self, config: &ConnectionConfig) -> AppResult<Option<String>> {
        if let Some(password) = self.passwords.lock().get(&config.id) {
            return Ok(password.clone());
        }
        let password = self.store.connection_password(&config.id)?;
        self.passwords
            .lock()
            .insert(config.id.clone(), password.clone());
        Ok(password)
    }

    pub fn forget_connection(&self, connection_id: &str) {
        self.configs.lock().remove(connection_id);
        self.passwords.lock().remove(connection_id);
    }

    pub async fn shutdown(&self) {
        if self.shutdown.swap(true, Ordering::SeqCst) {
            return;
        }
        self.sql.close_all().await;
        self.kv.close_all().await;
        self.search.close_all().await;
        let _ = self.store.append_audit(
            None,
            None,
            None,
            "application_shutdown",
            "Closed all SQL pools and Redis connections",
            None,
            "success",
            None,
        );
    }

    pub fn ensure_running(&self) -> AppResult<()> {
        if self.shutdown.load(Ordering::Relaxed) {
            Err(AppError::connection("The application is shutting down."))
        } else {
            Ok(())
        }
    }
}

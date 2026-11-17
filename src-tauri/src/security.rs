use crate::error::{AppError, AppResult};
use crate::models::ErrorClass;
use aes_gcm::{
    aead::{Aead, KeyInit},
    Aes256Gcm, Nonce,
};
use base64::{engine::general_purpose::STANDARD as B64, Engine};
use keyring::Entry;
use parking_lot::RwLock;
use rand::{rngs::OsRng, RngCore};
use serde::{de::DeserializeOwned, Serialize};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use zeroize::Zeroizing;

// Kept under the pre-rename service id so the one-time keychain import still finds
// credentials saved by Arcbase Studio builds. New secrets never use the OS keychain.
const SERVICE: &str = "com.arcbase.studio";
const CONFIG_KEY_ACCOUNT: &str = "local-config-encryption-key";

/// Seals the vault key in an owner-only file and keeps the AES-256-GCM helpers.
///
/// Nothing here opens the OS keychain unless the store was built with
/// [`SecretStore::with_legacy_import`], which the explicit one-time import action creates. A
/// keychain read costs an authorization dialog every launch once the app is re-signed, so the
/// normal vault path must stay independent of it.
#[derive(Clone)]
pub struct SecretStore {
    vault_path: PathBuf,
    import_from_keychain: bool,
    cached_config_key: Arc<RwLock<Option<Zeroizing<Vec<u8>>>>>,
}

impl SecretStore {
    /// The store the app runs on. It reads and writes only files inside the LuxBase data folder.
    pub fn new(vault_path: PathBuf) -> Self {
        Self::build(vault_path, false)
    }

    /// A store allowed to read the previous keychain items, for the user-triggered import only.
    pub fn with_legacy_import(vault_path: PathBuf) -> Self {
        Self::build(vault_path, true)
    }

    fn build(vault_path: PathBuf, import_from_keychain: bool) -> Self {
        Self {
            vault_path,
            import_from_keychain,
            cached_config_key: Arc::new(RwLock::new(None)),
        }
    }

    pub fn vault_path(&self) -> &Path {
        &self.vault_path
    }

    fn entry(account: &str) -> AppResult<Entry> {
        Entry::new(SERVICE, account).map_err(AppError::from)
    }

    /// True when the vault key is already on disk, or can still be created and read back.
    pub fn vault_available(&self) -> bool {
        if let Some(key) = self.cached_config_key.read().as_ref() {
            return key.len() == 32;
        }
        match self.read_key_file() {
            Ok(Some(bytes)) => matches!(Self::decode_key(&bytes), Ok(key) if key.len() == 32),
            Ok(None) => {
                let parent = self.vault_path.parent().unwrap_or(Path::new("."));
                fs::metadata(parent)
                    .map(|meta| meta.is_dir() && !meta.permissions().readonly())
                    .unwrap_or(false)
            }
            Err(_) => false,
        }
    }

    /// Reads a credential that a pre-rename Arcbase Studio build stored in the OS keychain.
    ///
    /// Only reachable through [`SecretStore::with_legacy_import`]; the normal store reports the
    /// item as absent without touching macOS. `Ok(None)` means the item never existed; an error
    /// means the item is there but macOS refused the read, which must not be mistaken for "no
    /// password saved".
    pub fn legacy_get(&self, account: &str) -> AppResult<Option<String>> {
        if !self.import_from_keychain {
            return Ok(None);
        }
        let entry = Self::entry(account)?;
        match entry.get_password() {
            Ok(value) => Ok(Some(value)),
            Err(keyring::Error::NoEntry) => Ok(None),
            Err(error) => Err(AppError::new(
                ErrorClass::Internal,
                format!(
                    "LuxBase needs one-time access to the previous keychain item '{account}' to import it into the local vault. macOS refused the read ({error}); try again and choose Always Allow, or enter the password once in the connection form."
                ),
            )),
        }
    }

    /// Removes a keychain item that has already been imported. A failure here is harmless — the
    /// vault row now wins on every later read — but the caller reports it.
    pub fn legacy_delete(&self, account: &str) -> AppResult<()> {
        if !self.import_from_keychain {
            return Ok(());
        }
        let entry = Self::entry(account)?;
        match entry.delete_credential() {
            Ok(()) | Err(keyring::Error::NoEntry) => Ok(()),
            Err(error) => Err(AppError::new(
                ErrorClass::Internal,
                format!("Imported the keychain item '{account}' but macOS refused to delete it ({error})."),
            )),
        }
    }

    fn read_key_file(&self) -> AppResult<Option<Vec<u8>>> {
        match fs::read(&self.vault_path) {
            Ok(bytes) => {
                #[cfg(unix)]
                tighten_key_file_permissions(&self.vault_path);
                Ok(Some(bytes))
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(AppError::new(
                ErrorClass::Internal,
                format!(
                    "Unable to read the local vault key at {}: {error}",
                    self.vault_path.display()
                ),
            )),
        }
    }

    /// Writes the key only into a directory that already exists, so an unusable data folder fails
    /// closed instead of silently creating a second vault the user would never back up.
    fn write_key_file(&self, encoded: &str) -> AppResult<()> {
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&self.vault_path).map_err(|error| {
            AppError::new(
                ErrorClass::Internal,
                format!(
                    "Unable to create the local vault key at {}: {error}",
                    self.vault_path.display()
                ),
            )
        })?;
        file.write_all(encoded.as_bytes())?;
        file.flush()?;
        #[cfg(unix)]
        tighten_key_file_permissions(&self.vault_path);
        Ok(())
    }

    /// Copies the master key an older build kept in the keychain, but only while the local vault
    /// key is still missing — an existing vault must never be replaced by a foreign key.
    pub fn install_legacy_config_key(&self) -> AppResult<bool> {
        if self.read_key_file()?.is_some() {
            return Ok(false);
        }
        let Some(encoded) = self.legacy_get(CONFIG_KEY_ACCOUNT)? else {
            return Ok(false);
        };
        Self::decode_key(encoded.as_bytes())?;
        self.write_key_file(&encoded)?;
        Ok(true)
    }

    fn decode_key(bytes: &[u8]) -> AppResult<Vec<u8>> {
        let encoded = String::from_utf8_lossy(bytes).trim().to_string();
        let key = B64
            .decode(encoded.as_bytes())
            .map_err(|error| AppError::validation(format!("Local vault key is invalid: {error}")))?;
        if key.len() != 32 {
            return Err(AppError::validation(
                "Local vault key must contain exactly 32 bytes.",
            ));
        }
        Ok(key)
    }

    fn load_or_create_config_key(&self) -> AppResult<Zeroizing<Vec<u8>>> {
        if let Some(key) = self.cached_config_key.read().as_ref() {
            return Ok(Zeroizing::new(key.to_vec()));
        }

        let encoded = match self.read_key_file()? {
            Some(existing) => existing,
            None => {
                // A keychain-enabled store (the one-time import action) recovers the previous
                // master key so the envelopes that build already wrote stay readable. The normal
                // store sees `Ok(None)` here and starts a fresh vault without asking macOS anything.
                let key = match self.legacy_get(CONFIG_KEY_ACCOUNT)? {
                    Some(existing) => existing,
                    None => {
                        let mut raw = [0u8; 32];
                        OsRng.fill_bytes(&mut raw);
                        B64.encode(raw)
                    }
                };
                self.write_key_file(&key)?;
                key.into_bytes()
            }
        };

        let key = Self::decode_key(&encoded)?;
        *self.cached_config_key.write() = Some(Zeroizing::new(key.clone()));
        Ok(Zeroizing::new(key))
    }

    pub fn encrypt_json<T: Serialize>(&self, value: &T) -> AppResult<String> {
        let plaintext = Zeroizing::new(serde_json::to_vec(value)?);
        self.encrypt_bytes(&plaintext)
    }

    pub fn decrypt_json<T: DeserializeOwned>(&self, encrypted: &str) -> AppResult<T> {
        let plaintext = self.decrypt_bytes(encrypted)?;
        Ok(serde_json::from_slice(&plaintext)?)
    }

    fn encrypt_bytes(&self, plaintext: &[u8]) -> AppResult<String> {
        let key = self.load_or_create_config_key()?;
        let cipher = Aes256Gcm::new_from_slice(key.as_ref())
            .map_err(|_| AppError::new(ErrorClass::Internal, "Invalid AES key length."))?;
        let mut nonce_bytes = [0u8; 12];
        OsRng.fill_bytes(&mut nonce_bytes);
        let nonce = Nonce::from_slice(&nonce_bytes);
        let ciphertext = cipher
            .encrypt(nonce, plaintext)
            .map_err(|_| AppError::new(ErrorClass::Internal, "Configuration encryption failed."))?;
        let mut envelope = Vec::with_capacity(12 + ciphertext.len());
        envelope.extend_from_slice(&nonce_bytes);
        envelope.extend_from_slice(&ciphertext);
        Ok(format!("aes-gcm-v1:{}", B64.encode(envelope)))
    }

    fn decrypt_bytes(&self, encrypted: &str) -> AppResult<Zeroizing<Vec<u8>>> {
        let payload = encrypted
            .strip_prefix("aes-gcm-v1:")
            .ok_or_else(|| AppError::validation("Unsupported encrypted configuration envelope."))?;
        let envelope = B64
            .decode(payload.as_bytes())
            .map_err(|error| AppError::validation(format!("Invalid encrypted configuration: {error}")))?;
        if envelope.len() <= 12 {
            return Err(AppError::validation("Encrypted configuration is truncated."));
        }
        let key = self.load_or_create_config_key()?;
        let cipher = Aes256Gcm::new_from_slice(key.as_ref())
            .map_err(|_| AppError::new(ErrorClass::Internal, "Invalid AES key length."))?;
        let (nonce_bytes, ciphertext) = envelope.split_at(12);
        let plaintext = cipher
            .decrypt(Nonce::from_slice(nonce_bytes), ciphertext)
            .map_err(|_| AppError::new(ErrorClass::Internal, "Configuration decryption failed."))?;
        Ok(Zeroizing::new(plaintext))
    }
}

#[cfg(unix)]
fn tighten_key_file_permissions(path: &Path) {
    use std::os::unix::fs::PermissionsExt;
    if let Ok(meta) = fs::metadata(path) {
        if meta.permissions().mode() & 0o777 != 0o600 {
            let _ = fs::set_permissions(path, fs::Permissions::from_mode(0o600));
        }
    }
}

/// Passphrase-derived envelope for connection export files, independent of the local vault so
/// an export can be opened on another machine with the same passphrase.
/// Layout: `aes-gcm-pw-v1:` + base64(salt[16] || nonce[12] || ciphertext).
pub fn encrypt_with_passphrase(plaintext: &[u8], passphrase: &str) -> AppResult<String> {
    use sha2::{Digest, Sha256};
    let mut salt = [0u8; 16];
    OsRng.fill_bytes(&mut salt);
    let key = Sha256::digest([salt.as_slice(), passphrase.as_bytes()].concat());
    let cipher = Aes256Gcm::new_from_slice(&key)
        .map_err(|_| AppError::new(ErrorClass::Internal, "Invalid passphrase-derived key length."))?;
    let mut nonce_bytes = [0u8; 12];
    OsRng.fill_bytes(&mut nonce_bytes);
    let ciphertext = cipher
        .encrypt(Nonce::from_slice(&nonce_bytes), plaintext)
        .map_err(|_| AppError::new(ErrorClass::Internal, "Export encryption failed."))?;
    let mut envelope = Vec::with_capacity(16 + 12 + ciphertext.len());
    envelope.extend_from_slice(&salt);
    envelope.extend_from_slice(&nonce_bytes);
    envelope.extend_from_slice(&ciphertext);
    Ok(format!("aes-gcm-pw-v1:{}", B64.encode(envelope)))
}

pub fn decrypt_with_passphrase(envelope: &str, passphrase: &str) -> AppResult<Zeroizing<Vec<u8>>> {
    use sha2::{Digest, Sha256};
    let payload = envelope
        .strip_prefix("aes-gcm-pw-v1:")
        .ok_or_else(|| AppError::validation("Unsupported export envelope."))?;
    let bytes = B64
        .decode(payload.as_bytes())
        .map_err(|error| AppError::validation(format!("Invalid export envelope: {error}")))?;
    if bytes.len() <= 28 {
        return Err(AppError::validation("Export envelope is truncated."));
    }
    let (salt, rest) = bytes.split_at(16);
    let (nonce_bytes, ciphertext) = rest.split_at(12);
    let key = Sha256::digest([salt, passphrase.as_bytes()].concat());
    let cipher = Aes256Gcm::new_from_slice(&key)
        .map_err(|_| AppError::new(ErrorClass::Internal, "Invalid passphrase-derived key length."))?;
    let plaintext = cipher
        .decrypt(Nonce::from_slice(nonce_bytes), ciphertext)
        .map_err(|_| AppError::validation("导出文件口令不正确或文件已损坏。"))?;
    Ok(Zeroizing::new(plaintext))
}

#[cfg(test)]
mod vault_tests {
    use super::*;

    struct TempVaultDir(PathBuf);

    impl TempVaultDir {
        fn new(tag: &str) -> Self {
            let dir = std::env::temp_dir().join(format!("luxbase-vault-{}-{tag}", std::process::id()));
            let _ = fs::remove_dir_all(&dir);
            fs::create_dir_all(&dir).unwrap();
            Self(dir)
        }
    }

    impl Drop for TempVaultDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn passphrase_envelope_round_trips_and_rejects_wrong_passphrase() {
        let sealed = encrypt_with_passphrase(b"secret-config", "pw123").unwrap();
        assert!(sealed.starts_with("aes-gcm-pw-v1:"));
        let opened = decrypt_with_passphrase(&sealed, "pw123").unwrap();
        assert_eq!(opened.as_slice(), b"secret-config");
        assert!(decrypt_with_passphrase(&sealed, "wrong").is_err());
    }

    #[test]
    fn the_vault_key_is_created_once_and_reused_across_restarts() {
        let dir = TempVaultDir::new("reuse");
        let path = dir.0.join("vault.key");
        let first = SecretStore::new(path.clone());
        let sealed = first.encrypt_json(&"secret".to_string()).unwrap();
        assert!(path.exists(), "the vault key must live inside LuxBase");
        assert_eq!(
            first.decrypt_json::<String>(&sealed).unwrap(),
            "secret",
            "the same store must keep using the key it just wrote"
        );
        let reopened = SecretStore::new(path.clone());
        assert_eq!(
            reopened.decrypt_json::<String>(&sealed).unwrap(),
            "secret",
            "a restarted app must read the same key file"
        );
    }

    #[test]
    fn an_unwritable_vault_path_fails_closed_instead_of_holding_secrets_in_memory() {
        let dir = TempVaultDir::new("unwritable");
        let blocked = dir.0.join("missing-parent").join("vault.key");
        let store = SecretStore::new(blocked);
        assert!(
            !store.vault_available(),
            "an unwritable vault path is not a usable vault"
        );
        assert!(
            store.encrypt_json(&"secret".to_string()).is_err(),
            "encryption must fail closed rather than keep an ephemeral key"
        );
    }

    #[test]
    fn the_store_the_app_runs_never_opens_the_keychain() {
        let dir = TempVaultDir::new("inert");
        let store = SecretStore::new(dir.0.join("vault.key"));
        assert!(
            store.legacy_get("connection:00000000-0000-4000-8000-000000000000:password").unwrap().is_none(),
            "a production store must report legacy items as absent"
        );
        assert!(store.legacy_delete("connection:00000000-0000-4000-8000-000000000000:password").is_ok());
        assert!(
            !store.install_legacy_config_key().unwrap(),
            "recovery cannot seed a vault key for a store that cannot read the keychain"
        );
        assert!(
            !dir.0.join("vault.key").exists(),
            "an inert legacy read must not leave a vault key behind"
        );
    }

    #[test]
    fn recovery_never_replaces_a_vault_key_that_already_exists() {
        let dir = TempVaultDir::new("no-overwrite");
        let path = dir.0.join("vault.key");
        let vault = SecretStore::new(path.clone());
        let sealed = vault.encrypt_json(&"kept".to_string()).unwrap();
        let importer = SecretStore::with_legacy_import(path.clone());
        assert!(
            !importer.install_legacy_config_key().unwrap(),
            "the key already inside LuxBase wins over anything the keychain offers"
        );
        assert_eq!(
            vault.decrypt_json::<String>(&sealed).unwrap(),
            "kept",
            "the vault key must survive a recovery attempt"
        );
    }

    #[cfg(unix)]
    #[test]
    fn the_vault_key_file_is_owner_readable_only() {
        use std::os::unix::fs::PermissionsExt;
        let dir = TempVaultDir::new("mode");
        let path = dir.0.join("vault.key");
        let store = SecretStore::new(path.clone());
        store.encrypt_json(&"seed".to_string()).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
        let reopened = SecretStore::new(path.clone());
        reopened.encrypt_json(&"again".to_string()).unwrap();
        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600,
            "the vault key must never be group or world readable"
        );
    }
}

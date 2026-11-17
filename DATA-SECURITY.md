# Runtime data

LuxBase stores everything in the platform application-data directory and never uploads it.
Nothing is written to the system keychain: the app used to keep its master key and every
credential there, and macOS re-authorized that access on each launch. The key and the
credentials now live inside LuxBase's own data folder, so launch is silent.

## Local vault

- `vault.key` holds the AES-256 master key, base64 encoded. On macOS and Linux it is created
  with owner-only permissions (`0600`) and re-tightened on every read; on Windows it inherits
  the per-user application-data ACLs.
- `app_secrets` inside `luxbase-studio.sqlite3` holds passwords, SSH passwords and private
  key passphrases as `aes-gcm-v1:<base64(nonce || ciphertext)>` envelopes.
- Connection configuration, query drafts and query history use the same envelope format, so
  no plaintext credential or SQL text remains at rest. Password literals inside SQL are still
  redacted before the text reaches storage.
- The vault key lives only on this machine. Losing the data folder means the encrypted rows
  cannot be recovered; there is no backdoor, reset flow or server-side copy.

### One-time import from an older install

Builds before the local vault stored the master key in the keychain under service
`com.luxbase.studio`, account `local-config-encryption-key`. On the first launch that needs a
key, LuxBase reads that item once, writes it to `vault.key` and keeps every envelope already
present in SQLite readable; per-connection credentials move into `app_secrets` the first time
each one is read. The original keychain items are left in place as an accidental backup and are
never read again after the key file exists, so the authorization prompt disappears from
subsequent launches.

If macOS refuses that single import read, LuxBase fails loudly instead of generating a fresh
key (a new key would silently lock every row already written). Relaunch and choose
**Always Allow**, or restore the data folder from backup.

Rows written by older builds stay plaintext until a key is available; the migration is retried
on the next launch rather than blocking startup.

## Local paths

- macOS: `~/Library/Application Support/com.luxbase.studio/`
  (`luxbase-studio.sqlite3`, `vault.key`)
- Windows: `%APPDATA%\com.luxbase.studio\` (`luxbase-studio.sqlite3`, `vault.key`)
- Linux: `~/.local/share/com.luxbase.studio/` (`luxbase-studio.sqlite3`, `vault.key`)

## Audit ledger

The `audit_log` table is append-only at the SQLite level. UPDATE and DELETE triggers
raise an error, and each row contains a SHA-256 chain hash derived from the previous
row and the current event payload. `verify_audit_chain` checks the complete chain.

use super::r#trait::KvDriver;
use crate::error::{AppError, AppResult};
use crate::models::{
    ConnectionConfig, IndexedString, RedisCommandRequest, RedisCommandResult, RedisInfo,
    RedisMode, RedisScanRequest, RedisScanResult, RedisValue, RedisValueBody, ScoredMember,
    StreamEntry, TlsMode,
};
use async_trait::async_trait;
use base64::{engine::general_purpose::STANDARD as B64, Engine};
use parking_lot::Mutex;
use redis::aio::{ConnectionManager, MultiplexedConnection};
use redis::cluster::ClusterClient;
use redis::cluster_async::ClusterConnection;
use redis::{Cmd, FromRedisValue, Value as RespValue};
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::Mutex as AsyncMutex;
use tokio_util::sync::CancellationToken;

enum RedisBackend {
    Single(ConnectionManager),
    Cluster(ClusterConnection),
}

#[derive(Clone)]
struct ActiveRedisOperation {
    token: CancellationToken,
    client_id: Option<u64>,
}

pub struct RedisDriver {
    connection_id: String,
    backend: Option<RedisBackend>,
    cancel_connection: Mutex<Option<MultiplexedConnection>>,
    cancellations: Mutex<HashMap<String, ActiveRedisOperation>>,
    command_timeout: Duration,
    session_lock: AsyncMutex<()>,
    selected_db: AtomicU32,
    cluster_scan_urls: Vec<String>,
    cluster_scan_state: Mutex<HashMap<u64, (usize, u64)>>,
    next_cluster_cursor: AtomicU64,
}

impl RedisDriver {
    pub fn new(connection_id: String) -> Self {
        Self {
            connection_id,
            backend: None,
            cancel_connection: Mutex::new(None),
            cancellations: Mutex::new(HashMap::new()),
            command_timeout: Duration::from_secs(30),
            session_lock: AsyncMutex::new(()),
            selected_db: AtomicU32::new(0),
            cluster_scan_urls: Vec::new(),
            cluster_scan_state: Mutex::new(HashMap::new()),
            next_cluster_cursor: AtomicU64::new(1),
        }
    }

    fn backend(&self) -> AppResult<&RedisBackend> {
        self.backend
            .as_ref()
            .ok_or_else(|| AppError::connection("The Redis connection is not initialized."))
    }

    fn token(&self, operation_id: &str) -> CancellationToken {
        let token = CancellationToken::new();
        self.cancellations.lock().insert(
            operation_id.to_string(),
            ActiveRedisOperation {
                token: token.clone(),
                client_id: None,
            },
        );
        token
    }

    fn finish(&self, operation_id: &str) {
        self.cancellations.lock().remove(operation_id);
    }

    fn operation_snapshot(&self, operation_id: &str) -> Option<ActiveRedisOperation> {
        self.cancellations.lock().get(operation_id).cloned()
    }

    async fn register_client_id(&self, operation_id: &str) -> AppResult<()> {
        if matches!(self.backend()?, RedisBackend::Cluster(_)) {
            return Ok(());
        }
        let mut connection = match self.backend()? {
            RedisBackend::Single(connection) => connection.clone(),
            RedisBackend::Cluster(_) => unreachable!(),
        };
        let client_id: u64 = redis::cmd("CLIENT")
            .arg("ID")
            .query_async(&mut connection)
            .await?;
        if let Some(operation) = self.cancellations.lock().get_mut(operation_id) {
            operation.client_id = Some(client_id);
        }
        Ok(())
    }

    async fn terminate_operation(&self, operation: &ActiveRedisOperation) -> AppResult<()> {
        operation.token.cancel();
        let Some(client_id) = operation.client_id else {
            return Ok(());
        };
        let mut connection = self
            .cancel_connection
            .lock()
            .clone()
            .ok_or_else(|| AppError::connection("The Redis cancellation channel is unavailable."))?;
        let killed: u64 = redis::cmd("CLIENT")
            .arg("KILL")
            .arg("ID")
            .arg(client_id)
            .query_async(&mut connection)
            .await?;
        if killed == 0 {
            return Err(AppError::new(
                crate::models::ErrorClass::ConnectionFailed,
                format!("Redis client {client_id} was no longer running."),
            ));
        }
        // Killing the client drops the shared socket; this probe absorbs the one failed call that
        // makes the connection manager reconnect, so the user's next command succeeds.
        let _ = self.query::<String>(&redis::cmd("PING")).await;
        Ok(())
    }

    async fn select_db(&self, db: u32) -> AppResult<()> {
        match self.backend()? {
            RedisBackend::Single(_) => {
                let mut select = redis::cmd("SELECT");
                select.arg(db);
                let _: String = self.query(&select).await?;
                self.selected_db.store(db, Ordering::Relaxed);
                Ok(())
            }
            RedisBackend::Cluster(_) if db != 0 => Err(AppError::unsupported(
                "Redis Cluster only supports DB 0.",
            )),
            RedisBackend::Cluster(_) => Ok(()),
        }
    }

    async fn query<T: FromRedisValue>(&self, command: &Cmd) -> AppResult<T> {
        match self.backend()? {
            RedisBackend::Single(connection) => {
                let mut connection = connection.clone();
                command
                    .query_async(&mut connection)
                    .await
                    .map_err(AppError::from)
            }
            RedisBackend::Cluster(connection) => {
                let mut connection = connection.clone();
                command
                    .query_async(&mut connection)
                    .await
                    .map_err(AppError::from)
            }
        }
    }

    async fn query_resp(&self, command: &Cmd) -> AppResult<RespValue> {
        self.query::<RespValue>(command).await
    }

    async fn pipeline(&self, pipeline: &redis::Pipeline) -> AppResult<Vec<RespValue>> {
        match self.backend()? {
            RedisBackend::Single(connection) => {
                let mut connection = connection.clone();
                pipeline
                    .query_async(&mut connection)
                    .await
                    .map_err(AppError::from)
            }
            RedisBackend::Cluster(connection) => {
                let mut connection = connection.clone();
                pipeline
                    .query_async(&mut connection)
                    .await
                    .map_err(AppError::from)
            }
        }
    }

    async fn with_timeout<F, T>(
        &self,
        operation_id: &str,
        token: CancellationToken,
        future: F,
    ) -> AppResult<T>
    where
        F: std::future::Future<Output = AppResult<T>>,
    {
        tokio::select! {
            biased;
            _ = token.cancelled() => Err(AppError::cancelled()),
            result = tokio::time::timeout(self.command_timeout, future) => {
                match result {
                    Ok(value) => value,
                    Err(_) => {
                        if let Some(operation) = self.operation_snapshot(operation_id) {
                            let _ = self.terminate_operation(&operation).await;
                        }
                        Err(AppError::timeout("The Redis command exceeded the configured timeout and was cancelled."))
                    }
                }
            }
        }
    }

    async fn run_cancellable<F, T>(&self, operation_id: &str, future: F) -> AppResult<T>
    where
        F: std::future::Future<Output = AppResult<T>>,
    {
        let token = self.token(operation_id);
        let result = self.with_timeout(operation_id, token, future).await;
        self.finish(operation_id);
        result
    }

    async fn query_ttl(&self, key: &[u8]) -> AppResult<i64> {
        let mut command = redis::cmd("TTL");
        command.arg(key);
        self.query(&command).await
    }

    async fn query_key_type(&self, key: &[u8]) -> AppResult<String> {
        let mut command = redis::cmd("TYPE");
        command.arg(key);
        self.query(&command).await
    }
}

#[async_trait]
impl KvDriver for RedisDriver {
    fn connection_id(&self) -> &str {
        &self.connection_id
    }

    async fn connect(
        &mut self,
        config: &ConnectionConfig,
        password: Option<String>,
    ) -> AppResult<()> {
        self.command_timeout = Duration::from_millis(config.query_timeout_ms.min(30_000));
        let mode = config.redis_mode.unwrap_or(RedisMode::Standalone);
        let tls = !matches!(
            config.tls_mode.unwrap_or(TlsMode::Disabled),
            TlsMode::Disabled
        );
        if tls
            && (config.ca_cert_path.as_deref().is_some_and(|value| !value.trim().is_empty())
                || config
                    .client_cert_path
                    .as_deref()
                    .is_some_and(|value| !value.trim().is_empty()))
        {
            return Err(AppError::unsupported(
                "Redis custom CA and client-certificate TLS are not available in this version. Use the system trust store or disable TLS explicitly.",
            ));
        }
        let scheme = if tls { "rediss" } else { "redis" };

        if mode == RedisMode::Cluster {
            let nodes = if config.cluster_nodes.is_empty() {
                vec![redis_url(
                    scheme,
                    &config.host,
                    config.port,
                    &config.username,
                    password.as_deref(),
                    None,
                )]
            } else {
                config
                    .cluster_nodes
                    .iter()
                    .map(|node| {
                        let mut endpoint = node.clone();
                        if !endpoint.contains("://") {
                            let auth = match (config.username.is_empty(), password.as_deref()) {
                                (false, Some(password)) => format!(
                                    "{}:{}@",
                                    percent_encode(&config.username),
                                    percent_encode(password)
                                ),
                                (true, Some(password)) => {
                                    format!(":{}@", percent_encode(password))
                                }
                                _ => String::new(),
                            };
                            endpoint = format!("{scheme}://{auth}{endpoint}");
                        }
                        endpoint
                    })
                    .collect()
            };
            let mut scan_urls = Vec::new();
            if let Some(seed) = nodes.first() {
                scan_urls = discover_cluster_masters(seed).await;
            }
            if scan_urls.is_empty() {
                scan_urls = nodes.clone();
            }
            self.cluster_scan_urls = scan_urls;
            let client = ClusterClient::new(nodes)?;
            let connection = client.get_async_connection().await?;
            self.backend = Some(RedisBackend::Cluster(connection));
        } else {
            let (host, port) = if mode == RedisMode::Sentinel {
                resolve_sentinel_master(config, password.as_deref()).await?
            } else {
                (config.host.clone(), config.port)
            };
            let url = redis_url(
                scheme,
                &host,
                port,
                &config.username,
                password.as_deref(),
                Some(config.redis_db),
            );
            let client = redis::Client::open(url)?;
            let cancel_connection = client.get_multiplexed_async_connection().await?;
            let mut connection = client.get_connection_manager().await?;
            let pong: String = redis::cmd("PING").query_async(&mut connection).await?;
            if pong != "PONG" {
                return Err(AppError::connection("Redis PING did not return PONG."));
            }
            *self.cancel_connection.lock() = Some(cancel_connection);
            self.selected_db.store(config.redis_db, Ordering::Relaxed);
            self.backend = Some(RedisBackend::Single(connection));
        }
        Ok(())
    }

    async fn scan(&self, request: RedisScanRequest) -> AppResult<RedisScanResult> {
        let token = self.token(&request.operation_id);
        let started = Instant::now();
        let result = self
            .with_timeout(&request.operation_id, token, async {
                let _session = self.session_lock.lock().await;
                self.register_client_id(&request.operation_id).await?;
                self.select_db(request.db).await?;
                // Keys are read as raw bytes: a keyspace may contain non-UTF-8 (binary) key
                // names, and converting the whole SCAN reply to String would fail the batch.
                let (cursor, keys): (u64, Vec<Vec<u8>>) = match self.backend()? {
                    RedisBackend::Single(_) => {
                        let mut command = redis::cmd("SCAN");
                        command
                            .arg(request.cursor)
                            .arg("MATCH")
                            .arg(&request.pattern)
                            .arg("COUNT")
                            .arg(request.count.max(1));
                        if let Some(filter) = &request.type_filter {
                            command.arg("TYPE").arg(filter);
                        }
                        self.query(&command).await?
                    }
                    RedisBackend::Cluster(_) => {
                        if self.cluster_scan_urls.is_empty() {
                            return Err(AppError::connection(
                                "Redis Cluster has no configured SCAN seed nodes.",
                            ));
                        }
                        let (node_index, cluster_cursor) = if request.cursor == 0 {
                            (0usize, 0u64)
                        } else {
                            self.cluster_scan_state
                                .lock()
                                .get(&request.cursor)
                                .copied()
                                .ok_or_else(|| {
                                    AppError::validation(
                                        "Redis Cluster SCAN cursor is no longer valid; restart SCAN.",
                                    )
                                })?
                        };
                        let node_index = node_index.min(self.cluster_scan_urls.len() - 1);
                        let url = self.cluster_scan_urls[node_index].clone();
                        let client = redis::Client::open(url)?;
                        let mut connection = client.get_multiplexed_async_connection().await?;
                        let mut command = redis::cmd("SCAN");
                        command
                            .arg(cluster_cursor)
                            .arg("MATCH")
                            .arg(&request.pattern)
                            .arg("COUNT")
                            .arg(request.count.max(1));
                        if let Some(filter) = &request.type_filter {
                            command.arg("TYPE").arg(filter);
                        }
                        let (next_node_cursor, node_keys): (u64, Vec<Vec<u8>>) =
                            command.query_async(&mut connection).await?;
                        let next_state = if next_node_cursor != 0 {
                            Some((node_index, next_node_cursor))
                        } else if node_index + 1 < self.cluster_scan_urls.len() {
                            Some((node_index + 1, 0))
                        } else {
                            None
                        };
                        let encoded_cursor = if let Some(state) = next_state {
                            let token = self.next_cluster_cursor.fetch_add(1, Ordering::Relaxed);
                            self.cluster_scan_state.lock().insert(token, state);
                            token
                        } else {
                            0
                        };
                        if request.cursor != 0 {
                            self.cluster_scan_state.lock().remove(&request.cursor);
                        }
                        (encoded_cursor, node_keys)
                    }
                };
                let key_details = self.load_key_summaries(&keys).await?;
                Ok(RedisScanResult {
                    cursor,
                    keys: key_details,
                    scanned: keys.len() as u64,
                    duration_ms: started.elapsed().as_millis() as u64,
                })
            })
            .await;
        self.finish(&request.operation_id);
        result
    }

    async fn get(&self, key: &[u8], db: u32, operation_id: &str) -> AppResult<RedisValue> {
        self.run_cancellable(operation_id, async {
            let _session = self.session_lock.lock().await;
            self.register_client_id(operation_id).await?;
            self.select_db(db).await?;
            let key_type = self.query_key_type(key).await?;
            if key_type == "none" {
                return Err(AppError::validation(format!(
                    "Redis key '{}' does not exist in DB {db}; it may have expired or been deleted. Refresh the key list and try again.",
                    String::from_utf8_lossy(key)
                )));
            }
            let guard_command = match key_type.as_str() {
                "string" => Some(("STRLEN", 5_000_000u64, "5 MB string")),
                "hash" => Some(("HLEN", 10_000, "10,000-field hash")),
                "list" => Some(("LLEN", 10_000, "10,000-item list")),
                "set" => Some(("SCARD", 10_000, "10,000-member set")),
                "zset" => Some(("ZCARD", 10_000, "10,000-member sorted set")),
                _ => None,
            };
            if let Some((length_command, limit, description)) = guard_command {
                let mut command = redis::cmd(length_command);
                command.arg(key);
                let cardinality: u64 = self.query(&command).await?;
                if cardinality > limit {
                    return Err(AppError::unsupported(format!(
                        "This {description} exceeds the safe typed-editor limit. Use the raw command console or export flow instead of loading the entire value."
                    )));
                }
            }
            let ttl_seconds = self.query_ttl(key).await?;
            let mut memory = redis::cmd("MEMORY");
            memory.arg("USAGE").arg(key);
            let size_bytes = self.query::<Option<u64>>(&memory).await.ok().flatten();
            let mut object_encoding = redis::cmd("OBJECT");
            object_encoding.arg("ENCODING").arg(key);
            let encoding = self
                .query::<Option<String>>(&object_encoding)
                .await
                .ok()
                .flatten();
            let mut raw_base64 = None;
            let value = match key_type.as_str() {
                "string" => {
                    let mut command = redis::cmd("GET");
                    command.arg(key);
                    let bytes: Option<Vec<u8>> = self.query(&command).await?;
                    let bytes = bytes.unwrap_or_default();
                    match String::from_utf8(bytes.clone()) {
                        Ok(text) => RedisValueBody::String(text),
                        Err(_) => {
                            raw_base64 = Some(B64.encode(&bytes));
                            RedisValueBody::String(String::from_utf8_lossy(&bytes).into_owned())
                        }
                    }
                }
                "hash" => {
                    let mut command = redis::cmd("HGETALL");
                    command.arg(key);
                    let map: HashMap<Vec<u8>, Vec<u8>> = self.query(&command).await?;
                    RedisValueBody::Hash(
                        map.into_iter()
                            .map(|(k, v)| (String::from_utf8_lossy(&k).into_owned(), String::from_utf8_lossy(&v).into_owned()))
                            .collect(),
                    )
                }
                "list" => {
                    let mut command = redis::cmd("LRANGE");
                    command.arg(key).arg(0).arg(-1);
                    let items: Vec<Vec<u8>> = self.query(&command).await?;
                    RedisValueBody::List(
                        items
                            .into_iter()
                            .enumerate()
                            .map(|(index, value)| IndexedString {
                                index: index as u64,
                                value: String::from_utf8_lossy(&value).into_owned(),
                            })
                            .collect(),
                    )
                }
                "set" => {
                    let mut command = redis::cmd("SMEMBERS");
                    command.arg(key);
                    let items: Vec<Vec<u8>> = self.query(&command).await?;
                    RedisValueBody::Set(items.into_iter().map(|v| String::from_utf8_lossy(&v).into_owned()).collect())
                }
                "zset" => {
                    let mut command = redis::cmd("ZRANGE");
                    command.arg(key).arg(0).arg(-1).arg("WITHSCORES");
                    let pairs: Vec<(Vec<u8>, f64)> = self.query(&command).await?;
                    RedisValueBody::ZSet(
                        pairs
                            .into_iter()
                            .map(|(member, score)| ScoredMember { member: String::from_utf8_lossy(&member).into_owned(), score })
                            .collect(),
                    )
                }
                "stream" => {
                    let mut command = redis::cmd("XRANGE");
                    command.arg(key).arg("-").arg("+").arg("COUNT").arg(100);
                    let response: RespValue = self.query(&command).await?;
                    RedisValueBody::Stream(parse_stream_entries(&response))
                }
                other => RedisValueBody::Unsupported(format!(
                    "Redis type '{other}' is not editable or previewable in this version."
                )),
            };
            Ok(RedisValue {
                key: String::from_utf8_lossy(key).into_owned(),
                key_type,
                ttl_seconds,
                encoding,
                size_bytes,
                raw_base64,
                value,
            })
        })
        .await
    }

    async fn set(&self, key: &[u8], db: u32, value: RedisValue, operation_id: &str) -> AppResult<()> {
        if matches!(&value.value, RedisValueBody::Stream(_) | RedisValueBody::Unsupported(_)) {
            return Err(AppError::unsupported(
                "Streams are read-only. Unsupported Redis types cannot be edited.",
            ));
        }
        if matches!(
            &value.value,
            RedisValueBody::Hash(fields) if fields.is_empty()
        ) || matches!(&value.value, RedisValueBody::List(items) if items.is_empty())
            || matches!(&value.value, RedisValueBody::Set(items) if items.is_empty())
            || matches!(&value.value, RedisValueBody::ZSet(items) if items.is_empty())
        {
            return Err(AppError::validation(
                "Redis does not store empty hash, list, set or sorted-set keys. Add at least one element or delete the key explicitly.",
            ));
        }
        self.run_cancellable(operation_id, async {
            let _session = self.session_lock.lock().await;
            self.register_client_id(operation_id).await?;
            self.select_db(db).await?;
            let mut pipeline = redis::pipe();
            pipeline.atomic();
            pipeline.cmd("DEL").arg(key);
            match value.value {
                RedisValueBody::String(text) => {
                    pipeline.cmd("SET").arg(key).arg(text);
                }
                RedisValueBody::Hash(fields) => {
                    for (field, field_value) in fields {
                        pipeline.cmd("HSET").arg(key).arg(field).arg(field_value);
                    }
                }
                RedisValueBody::List(items) => {
                    for item in items {
                        pipeline.cmd("RPUSH").arg(key).arg(item.value);
                    }
                }
                RedisValueBody::Set(items) => {
                    for item in items {
                        pipeline.cmd("SADD").arg(key).arg(item);
                    }
                }
                RedisValueBody::ZSet(items) => {
                    for item in items {
                        pipeline.cmd("ZADD").arg(key).arg(item.score).arg(item.member);
                    }
                }
                RedisValueBody::Stream(_) | RedisValueBody::Unsupported(_) => unreachable!(),
            }
            if value.ttl_seconds > 0 {
                pipeline.cmd("EXPIRE").arg(key).arg(value.ttl_seconds);
            }
            let responses = self.pipeline(&pipeline).await?;
            if responses.iter().any(|response| matches!(response, RespValue::Nil)) {
                return Err(AppError::new(
                    crate::models::ErrorClass::Internal,
                    "Redis atomic value replacement was rejected by the server.",
                ));
            }
            Ok(())
        })
        .await
    }

    async fn create(&self, db: u32, value: RedisValue, operation_id: &str) -> AppResult<()> {
        let command = create_command(&value)?;
        self.run_cancellable(operation_id, async {
            let _session = self.session_lock.lock().await;
            self.register_client_id(operation_id).await?;
            self.select_db(db).await?;
            match self.query::<i64>(&command).await? {
                1 => Ok(()),
                0 => Err(AppError::new(
                    crate::models::ErrorClass::Conflict,
                    "A Redis key with this name already exists. No value was changed.",
                )),
                -1 => Err(AppError::validation(
                    "The Redis TTL exceeds the server's supported expiration range.",
                )),
                _ => Err(AppError::new(
                    crate::models::ErrorClass::Internal,
                    "Redis atomic key creation returned an unexpected result.",
                )),
            }
        })
        .await
    }

    async fn del(
        &self,
        keys: &[Vec<u8>],
        db: u32,
        confirmed_dangerous: bool,
        operation_id: &str,
    ) -> AppResult<u64> {
        if keys.is_empty() {
            return Err(AppError::validation("DEL requires at least one key."));
        }
        if !confirmed_dangerous {
            return Err(AppError::validation(
                "DEL requires explicit second-step confirmation and an append-only audit entry.",
            ));
        }
        self.run_cancellable(operation_id, async {
            let _session = self.session_lock.lock().await;
            self.register_client_id(operation_id).await?;
            self.select_db(db).await?;
            let mut command = redis::cmd("DEL");
            for key in keys {
                command.arg(key.as_slice());
            }
            let deleted: u64 = self.query(&command).await?;
            Ok(deleted)
        })
        .await
    }

    async fn ttl(&self, key: &[u8], db: u32, operation_id: &str) -> AppResult<i64> {
        self.run_cancellable(operation_id, async {
            let _session = self.session_lock.lock().await;
            self.register_client_id(operation_id).await?;
            self.select_db(db).await?;
            self.query_ttl(key).await
        })
        .await
    }

    async fn key_type(&self, key: &[u8], db: u32, operation_id: &str) -> AppResult<String> {
        self.run_cancellable(operation_id, async {
            let _session = self.session_lock.lock().await;
            self.register_client_id(operation_id).await?;
            self.select_db(db).await?;
            self.query_key_type(key).await
        })
        .await
    }

    async fn dbsize(&self, db: u32, operation_id: &str) -> AppResult<u64> {
        self.run_cancellable(operation_id, async {
            let _session = self.session_lock.lock().await;
            self.register_client_id(operation_id).await?;
            self.select_db(db).await?;
            let command = redis::cmd("DBSIZE");
            self.query(&command).await
        })
        .await
    }

    async fn info(&self, operation_id: &str) -> AppResult<RedisInfo> {
        self.run_cancellable(operation_id, async {
            let _session = self.session_lock.lock().await;
            self.register_client_id(operation_id).await?;
            self.select_db(self.selected_db.load(Ordering::Relaxed)).await?;
            let mut command = redis::cmd("INFO");
            command.arg("ALL");
            let raw: String = self.query(&command).await?;
            Ok(parse_redis_info(raw))
        })
        .await
    }

    async fn health_check(&self) -> AppResult<()> {
        let _session = self.session_lock.lock().await;
        let pong: String = self.query(&redis::cmd("PING")).await?;
        if pong == "PONG" {
            Ok(())
        } else {
            Err(AppError::connection(
                "Redis health check returned an unexpected PING response.",
            ))
        }
    }

    async fn execute_raw(&self, request: RedisCommandRequest) -> AppResult<RedisCommandResult> {
        let command_name = request.command.trim().to_ascii_uppercase();
        let first_arg = request
            .args
            .first()
            .map(|value| value.trim().to_ascii_uppercase())
            .unwrap_or_default();
        if command_name == "KEYS" {
            return Err(AppError::unsupported(
                "KEYS is disabled because it blocks Redis; use cursor-based SCAN in the key browser.",
            ));
        }
        if matches!(
            command_name.as_str(),
            "MULTI"
                | "EXEC"
                | "DISCARD"
                | "WATCH"
                | "UNWATCH"
                | "SELECT"
                | "AUTH"
                | "HELLO"
                | "CLIENT"
                | "SUBSCRIBE"
                | "PSUBSCRIBE"
                | "SSUBSCRIBE"
                | "MONITOR"
                | "READONLY"
                | "READWRITE"
        ) {
            return Err(AppError::unsupported(
                "This command changes shared connection session state and is not available in the raw console. Use the dedicated connection, database and transaction controls.",
            ));
        }
        let dangerous = redis_command_requires_confirmation(&command_name, &first_arg);
        if dangerous && !request.confirmed_dangerous {
            return Err(AppError::validation(
                "This Redis command requires explicit second-step confirmation.",
            ));
        }
        let started = Instant::now();
        let token = self.token(&request.operation_id);
        let result = self
            .with_timeout(&request.operation_id, token, async {
                let _session = self.session_lock.lock().await;
                self.register_client_id(&request.operation_id).await?;
                self.select_db(self.selected_db.load(Ordering::Relaxed)).await?;
                let mut command = redis::cmd(&request.command);
                for arg in &request.args {
                    command.arg(arg);
                }
                let value = self.query_resp(&command).await?;
                Ok(RedisCommandResult {
                    operation_id: request.operation_id.clone(),
                    protocol: "RESP".into(),
                    value: resp_to_json(&value),
                    duration_ms: started.elapsed().as_millis() as u64,
                })
            })
            .await;
        self.finish(&request.operation_id);
        result
    }

    async fn cancel(&self, operation_id: &str) -> AppResult<()> {
        let operation = self.operation_snapshot(operation_id).ok_or_else(|| {
            AppError::validation(format!(
                "Redis command '{operation_id}' is not running or has already completed."
            ))
        })?;
        self.terminate_operation(&operation).await
    }

    async fn close(&self) -> AppResult<()> {
        for (_, operation) in self.cancellations.lock().drain() {
            operation.token.cancel();
        }
        let _ = self.cancel_connection.lock().take();
        Ok(())
    }
}

impl RedisDriver {
    async fn load_key_summaries(
        &self,
        keys: &[Vec<u8>],
    ) -> AppResult<Vec<crate::models::RedisKeySummary>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let mut pipeline = redis::pipe();
        for key in keys {
            pipeline.cmd("TYPE").arg(key.as_slice());
            pipeline.cmd("TTL").arg(key.as_slice());
            pipeline.cmd("MEMORY").arg("USAGE").arg(key.as_slice());
        }
        let response = self.pipeline(&pipeline).await?;
        let mut summaries = Vec::with_capacity(keys.len());
        let mut length_pipeline = redis::pipe();
        let mut length_indexes = Vec::new();
        for (index, key) in keys.iter().enumerate() {
            let response_index = index * 3;
            let key_type = response
                .get(response_index)
                .map(resp_string)
                .unwrap_or_else(|| "unknown".into());
            let ttl_seconds = response
                .get(response_index + 1)
                .and_then(resp_i64)
                .unwrap_or(-2);
            let size_bytes = response
                .get(response_index + 2)
                .and_then(resp_u64);
            let length_command = match key_type.as_str() {
                "string" => Some("STRLEN"),
                "hash" => Some("HLEN"),
                "list" => Some("LLEN"),
                "set" => Some("SCARD"),
                "zset" => Some("ZCARD"),
                "stream" => Some("XLEN"),
                _ => None,
            };
            if let Some(command) = length_command {
                length_pipeline.cmd(command).arg(key.as_slice());
                length_indexes.push(index);
            }
            summaries.push(crate::models::RedisKeySummary {
                key: String::from_utf8_lossy(key).into_owned(),
                key_base64: if std::str::from_utf8(key).is_ok() { None } else { Some(B64.encode(key)) },
                key_type,
                ttl_seconds,
                size_bytes,
                length: None,
            });
        }
        if !length_indexes.is_empty() {
            let lengths = self.pipeline(&length_pipeline).await?;
            for (length, summary_index) in lengths.into_iter().zip(length_indexes) {
                summaries[summary_index].length = resp_u64(&length);
            }
        }
        Ok(summaries)
    }
}

async fn discover_cluster_masters(seed_url: &str) -> Vec<String> {
    let Ok(client) = redis::Client::open(seed_url.to_string()) else {
        return Vec::new();
    };
    let Ok(mut connection) = client.get_multiplexed_async_connection().await else {
        return Vec::new();
    };
    let raw: Result<String, redis::RedisError> = redis::cmd("CLUSTER")
        .arg("NODES")
        .query_async(&mut connection)
        .await;
    let Ok(raw) = raw else {
        return Vec::new();
    };
    let (scheme, _) = seed_url.split_once("://").unwrap_or(("redis", seed_url));
    let authority_prefix = seed_url
        .rsplit_once('@')
        .map(|(prefix, _)| prefix.to_string());
    let mut masters = Vec::new();
    for line in raw.lines() {
        let fields = line.split_whitespace().collect::<Vec<_>>();
        if fields.len() < 3 || !fields[2].split(',').any(|flag| flag == "master") {
            continue;
        }
        let endpoint = fields[1].split('@').next().unwrap_or(fields[1]);
        if endpoint.is_empty() {
            continue;
        }
        let url = match &authority_prefix {
            Some(prefix) => format!("{prefix}@{endpoint}"),
            None => format!("{scheme}://{endpoint}"),
        };
        if !masters.contains(&url) {
            masters.push(url);
        }
    }
    masters
}

async fn resolve_sentinel_master(
    config: &ConnectionConfig,
    password: Option<&str>,
) -> AppResult<(String, u16)> {
    let master_name = config
        .sentinel_master_name
        .as_deref()
        .ok_or_else(|| AppError::validation("Sentinel master name is required."))?;
    let mut last_error = None;
    let scheme = if matches!(
        config.tls_mode.unwrap_or(TlsMode::Disabled),
        TlsMode::Disabled
    ) {
        "redis"
    } else {
        "rediss"
    };
    for sentinel in &config.sentinels {
        let url = redis_url(
            scheme,
            &sentinel.host,
            sentinel.port,
            &config.username,
            password,
            None,
        );
        let result = async {
            let client = redis::Client::open(url)?;
            let mut connection = client.get_multiplexed_async_connection().await?;
            let mut command = redis::cmd("SENTINEL");
            command.arg("get-master-addr-by-name").arg(master_name);
            let address: Vec<String> = command.query_async(&mut connection).await?;
            let host = address
                .first()
                .cloned()
                .ok_or_else(|| AppError::connection("Sentinel returned an empty master address."))?;
            let port = address
                .get(1)
                .and_then(|value| value.parse::<u16>().ok())
                .ok_or_else(|| AppError::connection("Sentinel returned an invalid master port."))?;
            Ok::<_, AppError>((host, port))
        }
        .await;
        match result {
            Ok(value) => return Ok(value),
            Err(error) => last_error = Some(error),
        }
    }
    Err(last_error.unwrap_or_else(|| {
        AppError::connection("No Redis Sentinel nodes were configured.")
    }))
}

fn redis_url(
    scheme: &str,
    host: &str,
    port: u16,
    username: &str,
    password: Option<&str>,
    db: Option<u32>,
) -> String {
    let auth = match (username.is_empty(), password) {
        (false, Some(password)) => format!(
            "{}:{}@",
            percent_encode(username),
            percent_encode(password)
        ),
        (true, Some(password)) => format!(":{}@", percent_encode(password)),
        _ => String::new(),
    };
    let db = db.map(|value| format!("/{value}")).unwrap_or_default();
    format!("{scheme}://{auth}{host}:{port}{db}")
}

fn percent_encode(value: &str) -> String {
    url::form_urlencoded::byte_serialize(value.as_bytes()).collect()
}

// This fixed, single-key script is only used by the typed create operation, not the raw console.
// Redis scripts do not roll back on errors, so expiration is checked before the first value write.
const CREATE_KEY_SCRIPT: &str = r#"
if redis.call('EXISTS', KEYS[1]) ~= 0 then
    return 0
end

local kind = ARGV[1]
local expires_at = nil
if ARGV[2] ~= '-1' then
    local now = redis.call('TIME')
    local milliseconds = now[1] .. string.format('%03d', math.floor(tonumber(now[2]) / 1000))
    local duration = ARGV[2] .. '000'
    -- Decimal addition avoids Lua double rounding near Redis's signed 64-bit timestamp limit.
    local digits = {}
    local carry = 0
    local i, j = #milliseconds, #duration
    while i > 0 or j > 0 or carry > 0 do
        local a = i > 0 and tonumber(string.sub(milliseconds, i, i)) or 0
        local b = j > 0 and tonumber(string.sub(duration, j, j)) or 0
        local sum = a + b + carry
        table.insert(digits, 1, tostring(sum % 10))
        carry = math.floor(sum / 10)
        i, j = i - 1, j - 1
    end
    expires_at = table.concat(digits)
    if #expires_at > 19 or (#expires_at == 19 and expires_at > '9223372036854775807') then
        return -1
    end
    -- The key is absent: this also checks native expiration permissions before creating it.
    redis.call('PEXPIREAT', KEYS[1], expires_at)
end

if kind == 'string' then
    redis.call('SET', KEYS[1], ARGV[3])
elseif kind == 'hash' then
    for i = 3, #ARGV, 2 do
        redis.call('HSET', KEYS[1], ARGV[i], ARGV[i + 1])
    end
elseif kind == 'list' then
    for i = 3, #ARGV do
        redis.call('RPUSH', KEYS[1], ARGV[i])
    end
elseif kind == 'set' then
    for i = 3, #ARGV do
        redis.call('SADD', KEYS[1], ARGV[i])
    end
elseif kind == 'zset' then
    for i = 3, #ARGV, 2 do
        redis.call('ZADD', KEYS[1], ARGV[i], ARGV[i + 1])
    end
end
if expires_at then
    redis.call('PEXPIREAT', KEYS[1], expires_at)
end
return 1
"#;

fn validate_create_string(value: &str) -> AppResult<()> {
    if value.len() > 512 * 1024 * 1024 {
        return Err(AppError::validation(
            "Redis keys, fields, members and string values must not exceed 512 MiB.",
        ));
    }
    Ok(())
}

fn create_command(value: &RedisValue) -> AppResult<Cmd> {
    if value.key.is_empty() {
        return Err(AppError::validation("A Redis key name must not be empty."));
    }
    validate_create_string(&value.key)?;
    if value.ttl_seconds != -1
        && (value.ttl_seconds <= 0 || value.ttl_seconds.checked_mul(1000).is_none())
    {
        return Err(AppError::validation(
            "Redis TTL must be -1 (permanent) or positive seconds within the supported expiration range.",
        ));
    }

    // Validate the entire body before sending any writes, including later sorted-set scores.
    let (kind, args): (&str, Vec<String>) = match &value.value {
        RedisValueBody::String(text) => {
            validate_create_string(text)?;
            ("string", vec![text.clone()])
        }
        RedisValueBody::Hash(fields) => {
            let mut args = Vec::new();
            for (field, text) in fields {
                validate_create_string(field)?;
                validate_create_string(text)?;
                args.push(field.clone());
                args.push(text.clone());
            }
            ("hash", args)
        }
        RedisValueBody::List(items) => {
            let mut args = Vec::new();
            for item in items {
                validate_create_string(&item.value)?;
                args.push(item.value.clone());
            }
            ("list", args)
        }
        RedisValueBody::Set(items) => {
            for item in items {
                validate_create_string(item)?;
            }
            ("set", items.clone())
        }
        RedisValueBody::ZSet(items) => {
            let mut args = Vec::new();
            for item in items {
                if !item.score.is_finite() {
                    return Err(AppError::validation("Sorted-set scores must be finite numbers."));
                }
                validate_create_string(&item.member)?;
                args.push(item.score.to_string());
                args.push(item.member.clone());
            }
            ("zset", args)
        }
        RedisValueBody::Stream(_) | RedisValueBody::Unsupported(_) => {
            return Err(AppError::unsupported(
                "Only string, hash, list, set and sorted-set keys can be created. Streams and unsupported Redis types are read-only.",
            ));
        }
    };
    if kind != "string" && args.is_empty() {
        return Err(AppError::validation(
            "Redis does not store empty hash, list, set or sorted-set keys. Add at least one element.",
        ));
    }
    if value.key_type != kind {
        return Err(AppError::validation(
            "The Redis key type must match the value body type.",
        ));
    }
    let mut command = redis::cmd("EVAL");
    command
        .arg(CREATE_KEY_SCRIPT)
        .arg(1)
        .arg(&value.key)
        .arg(kind)
        .arg(value.ttl_seconds)
        .arg(args);
    Ok(command)
}

fn redis_command_requires_confirmation(command: &str, subcommand: &str) -> bool {
    if matches!(
        command,
        "FLUSHALL"
            | "FLUSHDB"
            | "SHUTDOWN"
            | "SWAPDB"
            | "MOVE"
            | "RESTORE"
            | "MIGRATE"
            | "REPLICAOF"
            | "SLAVEOF"
            | "FAILOVER"
            | "RENAME"
            | "GETDEL"
            | "EVAL"
            | "EVALSHA"
            | "FCALL"
            | "DEL"
            | "UNLINK"
    ) {
        return true;
    }
    match command {
        "CONFIG" => matches!(subcommand, "SET" | "REWRITE"),
        "ACL" => matches!(subcommand, "SETUSER" | "DELUSER" | "LOAD" | "SAVE"),
        "SCRIPT" => subcommand == "FLUSH",
        "FUNCTION" => matches!(subcommand, "FLUSH" | "DELETE" | "LOAD" | "RESTORE"),
        "MODULE" => matches!(subcommand, "LOAD" | "UNLOAD"),
        "XGROUP" => subcommand == "DESTROY",
        _ => false,
    }
}

fn resp_string(value: &RespValue) -> String {
    match value {
        RespValue::SimpleString(value) => value.clone(),
        RespValue::BulkString(bytes) => String::from_utf8_lossy(bytes).to_string(),
        RespValue::VerbatimString { text, .. } => text.clone(),
        RespValue::Int(value) => value.to_string(),
        RespValue::Double(value) => value.to_string(),
        RespValue::Boolean(value) => value.to_string(),
        RespValue::BigNumber(value) => value.to_string(),
        RespValue::Okay => "OK".into(),
        RespValue::Nil => String::new(),
        _ => format!("{value:?}"),
    }
}

fn resp_i64(value: &RespValue) -> Option<i64> {
    match value {
        RespValue::Int(value) => Some(*value),
        RespValue::BulkString(bytes) => String::from_utf8_lossy(bytes).parse().ok(),
        RespValue::SimpleString(value) => value.parse().ok(),
        _ => None,
    }
}

fn resp_u64(value: &RespValue) -> Option<u64> {
    resp_i64(value).and_then(|value| value.try_into().ok())
}

fn resp_to_json(value: &RespValue) -> Value {
    match value {
        RespValue::Nil => Value::Null,
        RespValue::SimpleString(value) => Value::String(value.clone()),
        RespValue::BulkString(bytes) => Value::String(String::from_utf8_lossy(bytes).to_string()),
        RespValue::VerbatimString { text, .. } => Value::String(text.clone()),
        RespValue::Int(value) => Value::from(*value),
        RespValue::Double(value) => Value::from(*value),
        RespValue::Boolean(value) => Value::Bool(*value),
        RespValue::BigNumber(value) => Value::String(value.to_string()),
        RespValue::Okay => Value::String("OK".into()),
        RespValue::Array(items) => Value::Array(items.iter().map(resp_to_json).collect()),
        RespValue::Set(items) => Value::Array(items.iter().map(resp_to_json).collect()),
        RespValue::Map(items) => {
            let mut map = Map::new();
            for (key, value) in items {
                map.insert(resp_string(key), resp_to_json(value));
            }
            Value::Object(map)
        }
        RespValue::Attribute { data, attributes } => json!({
            "data": resp_to_json(data),
            "attributes": attributes.iter().map(|(key, value)| (resp_string(key), resp_to_json(value))).collect::<Map<_, _>>()
        }),
        RespValue::Push { kind, data } => json!({
            "push": format!("{kind:?}"),
            "data": data.iter().map(resp_to_json).collect::<Vec<_>>()
        }),
        RespValue::ServerError(error) => json!({ "error": format!("{error:?}") }),
    }
}

fn parse_stream_entries(value: &RespValue) -> Vec<StreamEntry> {
    let RespValue::Array(entries) = value else {
        return Vec::new();
    };
    entries
        .iter()
        .filter_map(|entry| {
            let RespValue::Array(parts) = entry else {
                return None;
            };
            let id = parts.first().map(resp_string).unwrap_or_default();
            let mut fields = BTreeMap::new();
            if let Some(RespValue::Array(field_values)) = parts.get(1) {
                for pair in field_values.chunks(2) {
                    if let [field, value] = pair {
                        fields.insert(resp_string(field), resp_string(value));
                    }
                }
            }
            Some(StreamEntry { id, fields })
        })
        .collect()
}

fn parse_redis_info(raw: String) -> RedisInfo {
    let mut values = HashMap::new();
    for line in raw.lines() {
        if line.starts_with('#') || line.trim().is_empty() {
            continue;
        }
        if let Some((key, value)) = line.split_once(':') {
            values.insert(key.to_ascii_lowercase(), value.to_string());
        }
    }
    let hits = values
        .get("keyspace_hits")
        .and_then(|value| value.parse::<f64>().ok())
        .unwrap_or(0.0);
    let misses = values
        .get("keyspace_misses")
        .and_then(|value| value.parse::<f64>().ok())
        .unwrap_or(0.0);
    let hit_rate = if hits + misses > 0.0 {
        Some(hits / (hits + misses))
    } else {
        None
    };
    let mut keyspace = Vec::new();
    for (key, value) in &values {
        let Some(db) = key.strip_prefix("db").and_then(|index| index.parse::<u32>().ok()) else {
            continue;
        };
        let mut keys = 0u64;
        let mut expires = 0u64;
        for part in value.split(',') {
            if let Some(number) = part.strip_prefix("keys=") {
                keys = number.parse().unwrap_or(0);
            } else if let Some(number) = part.strip_prefix("expires=") {
                expires = number.parse().unwrap_or(0);
            }
        }
        keyspace.push(crate::models::RedisKeyspaceEntry { db, keys, expires });
    }
    keyspace.sort_by_key(|entry| entry.db);
    RedisInfo {
        memory_used_bytes: values.get("used_memory").and_then(|value| value.parse().ok()),
        memory_peak_bytes: values
            .get("used_memory_peak")
            .and_then(|value| value.parse().ok()),
        connected_clients: values
            .get("connected_clients")
            .and_then(|value| value.parse().ok()),
        hit_rate,
        replication_role: values.get("role").cloned(),
        connected_replicas: values
            .get("connected_slaves")
            .and_then(|value| value.parse().ok()),
        cluster_enabled: values
            .get("cluster_enabled")
            .map(|value| value == "1")
            .unwrap_or(false),
        keyspace,
        raw,
    }
}

#[cfg(test)]
mod create_tests {
    use super::*;
    use crate::models::ErrorClass;

    fn value(kind: &str, body: RedisValueBody) -> RedisValue {
        RedisValue {
            key: "new:{key}".into(),
            key_type: kind.into(),
            ttl_seconds: -1,
            encoding: None,
            size_bytes: None,
            raw_base64: None,
            value: body,
        }
    }

    fn command_args(value: &RedisValue) -> Vec<String> {
        let command = create_command(value).unwrap();
        let response = redis::parse_redis_value(&command.get_packed_command()).unwrap();
        let RespValue::Array(args) = response else {
            panic!("Expected a RESP command array");
        };
        args.iter().map(resp_string).collect()
    }

    fn assert_validation(value: RedisValue) {
        let error = create_command(&value).err().expect("Expected invalid input");
        assert_eq!(error.class, ErrorClass::Validation);
    }

    #[test]
    fn binds_values_as_arguments_and_uses_only_one_key() {
        let payload = "'); redis.call('DEL', 'victim'); --\n\0你好";
        let mut input = value("string", RedisValueBody::String(payload.into()));
        input.key = "key'\n\0{slot}".into();
        input.ttl_seconds = 60;
        assert_eq!(
            command_args(&input),
            vec![
                "EVAL", CREATE_KEY_SCRIPT, "1", &input.key, "string", "60", payload,
            ]
        );
        assert!(!CREATE_KEY_SCRIPT.contains(payload));
        assert!(!CREATE_KEY_SCRIPT.contains("redis.call('DEL'"));
        assert!(!CREATE_KEY_SCRIPT.contains("KEYS[2]"));
    }

    #[test]
    fn generates_arguments_for_all_writable_types() {
        let cases = vec![
            (value("string", RedisValueBody::String(String::new())), vec![""]),
            (
                value("hash", RedisValueBody::Hash(BTreeMap::from([
                    ("b".into(), "second".into()),
                    ("a".into(), String::new()),
                ]))),
                vec!["a", "", "b", "second"],
            ),
            (
                value("list", RedisValueBody::List(vec![
                    IndexedString { index: 7, value: "first".into() },
                    IndexedString { index: 0, value: String::new() },
                ])),
                vec!["first", ""],
            ),
            (value("set", RedisValueBody::Set(vec![String::new(), "member".into()])), vec!["", "member"]),
            (
                value("zset", RedisValueBody::ZSet(vec![
                    ScoredMember { member: "a".into(), score: -1.25 },
                    ScoredMember { member: String::new(), score: 0.0 },
                ])),
                vec!["-1.25", "a", "0", ""],
            ),
        ];
        for (input, expected) in cases {
            let args = command_args(&input);
            assert_eq!(args[4], input.key_type);
            assert_eq!(&args[6..], expected);
        }
    }

    #[test]
    fn rejects_empty_keys_and_empty_collection_bodies() {
        let mut input = value("string", RedisValueBody::String(String::new()));
        input.key.clear();
        assert_validation(input);
        for (kind, body) in [
            ("hash", RedisValueBody::Hash(BTreeMap::new())),
            ("list", RedisValueBody::List(Vec::new())),
            ("set", RedisValueBody::Set(Vec::new())),
            ("zset", RedisValueBody::ZSet(Vec::new())),
        ] {
            assert_validation(value(kind, body));
        }
    }

    #[test]
    fn validates_ttl_before_building_a_command() {
        for ttl in [i64::MIN, -2, 0, i64::MAX / 1000 + 1, i64::MAX] {
            let mut input = value("string", RedisValueBody::String(String::new()));
            input.ttl_seconds = ttl;
            assert_validation(input);
        }
        for ttl in [-1, 1, 60, i64::MAX / 1000] {
            let mut input = value("string", RedisValueBody::String(String::new()));
            input.ttl_seconds = ttl;
            assert_eq!(command_args(&input)[5], ttl.to_string());
        }
        // The server-clock-dependent absolute timestamp limit is checked inside the script.
        let overflow_guard = CREATE_KEY_SCRIPT.find("if #expires_at > 19").unwrap();
        let expiry_probe = CREATE_KEY_SCRIPT.find("redis.call('PEXPIREAT'").unwrap();
        let first_write = CREATE_KEY_SCRIPT.find("redis.call('SET'").unwrap();
        assert!(overflow_guard < expiry_probe && expiry_probe < first_write);
        assert!(CREATE_KEY_SCRIPT.find("redis.call('EXISTS'").unwrap() < overflow_guard);
    }

    #[test]
    fn rejects_nonfinite_scores_even_after_valid_members() {
        for score in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            assert_validation(value("zset", RedisValueBody::ZSet(vec![
                ScoredMember { member: "valid".into(), score: 1.0 },
                ScoredMember { member: "invalid".into(), score },
            ])));
        }
        for score in [f64::MIN, f64::MAX, 0.0] {
            let input = value("zset", RedisValueBody::ZSet(vec![
                ScoredMember { member: "finite".into(), score },
            ]));
            assert!(create_command(&input).is_ok());
        }
    }

    #[test]
    fn rejects_read_only_bodies_and_mismatched_metadata() {
        for body in [
            RedisValueBody::Stream(Vec::new()),
            RedisValueBody::Unsupported("unknown".into()),
        ] {
            let error = create_command(&value("stream", body)).err().unwrap();
            assert_eq!(error.class, ErrorClass::ProtocolUnsupported);
        }
        assert_validation(value("hash", RedisValueBody::String("text".into())));
    }

    #[test]
    fn does_not_weaken_raw_eval_confirmation() {
        assert!(redis_command_requires_confirmation("EVAL", ""));
        assert!(redis_command_requires_confirmation("EVALSHA", ""));
    }
}

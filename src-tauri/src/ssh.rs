use crate::error::{AppError, AppResult};
use crate::models::SshConfig;
use ssh2::Session;
use std::io::{ErrorKind, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

pub struct SshTunnel {
    local_port: u16,
    shutdown: Arc<AtomicBool>,
    worker: Option<thread::JoinHandle<()>>,
}

impl SshTunnel {
    pub fn local_port(&self) -> u16 {
        self.local_port
    }

    pub fn start(
        config: &SshConfig,
        password: Option<String>,
        passphrase: Option<String>,
        target_host: &str,
        target_port: u16,
    ) -> AppResult<Self> {
        // Authenticate once up front so bad credentials or host keys surface as SSH errors
        // instead of an opaque "connection reset" from the database driver.
        drop(connect_session(config, password.as_deref(), passphrase.as_deref())?);
        let listener = TcpListener::bind(("127.0.0.1", 0))?;
        listener.set_nonblocking(true)?;
        let local_port = listener.local_addr()?.port();
        let shutdown = Arc::new(AtomicBool::new(false));
        let worker_shutdown = Arc::clone(&shutdown);
        let ssh = config.clone();
        let target_host = target_host.to_string();

        let worker = thread::spawn(move || {
            while !worker_shutdown.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        let ssh = ssh.clone();
                        let password = password.clone();
                        let passphrase = passphrase.clone();
                        let target_host = target_host.clone();
                        thread::spawn(move || {
                            if let Err(error) = forward_connection(
                                stream,
                                &ssh,
                                password.as_deref(),
                                passphrase.as_deref(),
                                &target_host,
                                target_port,
                            ) {
                                tracing::warn!(%error, "SSH tunnel forwarding failed");
                            }
                        });
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(40));
                    }
                    Err(error) => {
                        tracing::error!(%error, "SSH tunnel listener failed");
                        break;
                    }
                }
            }
        });

        Ok(Self {
            local_port,
            shutdown,
            worker: Some(worker),
        })
    }
}

impl Drop for SshTunnel {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

fn connect_session(
    config: &SshConfig,
    password: Option<&str>,
    passphrase: Option<&str>,
) -> AppResult<Session> {
    let address = format!("{}:{}", config.host, config.port);
    let tcp = TcpStream::connect_timeout(
        &address
            .parse()
            .map_err(|error| AppError::validation(format!("Invalid SSH address: {error}")))?,
        Duration::from_secs(10),
    )?;
    tcp.set_read_timeout(Some(Duration::from_secs(30)))?;
    tcp.set_write_timeout(Some(Duration::from_secs(30)))?;
    let mut session = Session::new()?;
    session.set_tcp_stream(tcp);
    session.handshake()?;

    if config.strict_host_key_checking {
        let known_hosts_path = config
            .known_hosts_path
            .as_deref()
            .map(std::path::PathBuf::from)
            .or_else(|| dirs::home_dir().map(|home| home.join(".ssh/known_hosts")))
            .ok_or_else(|| AppError::validation("known_hosts path is unavailable."))?;
        if !known_hosts_path.exists() {
            return Err(AppError::connection(format!(
                "Strict SSH host key checking is enabled but {} does not exist.",
                known_hosts_path.display()
            )));
        }
        let mut known_hosts = session.known_hosts()?;
        known_hosts.read_file(&known_hosts_path, ssh2::KnownHostFileKind::OpenSSH)?;
        let (host_key, _) = session
            .host_key()
            .ok_or_else(|| AppError::connection("The SSH server did not present a host key."))?;
        match known_hosts.check_port(&config.host, config.port, host_key) {
            ssh2::CheckResult::Match => {}
            ssh2::CheckResult::Mismatch => {
                return Err(AppError::connection(
                    "SSH host key does not match the pinned known_hosts entry.",
                ))
            }
            ssh2::CheckResult::NotFound => {
                return Err(AppError::connection(
                    "SSH host key is not present in known_hosts; strict checking refused the connection.",
                ))
            }
            ssh2::CheckResult::Failure => {
                return Err(AppError::connection("SSH host key verification failed."))
            }
        }
    }

    if let Some(private_key_path) = &config.private_key_path {
        session.userauth_pubkey_file(
            &config.username,
            None,
            std::path::Path::new(private_key_path),
            passphrase,
        )?;
    } else if let Some(password) = password {
        session.userauth_password(&config.username, password)?;
    } else {
        return Err(AppError::validation(
            "SSH authentication requires a key or password.",
        ));
    }

    if !session.authenticated() {
        return Err(AppError::new(
            crate::models::ErrorClass::PermissionDenied,
            "SSH authentication failed.",
        ));
    }
    Ok(session)
}

// libssh2 serialises every call on the session lock, so a blocking reader thread would starve
// the writer. Pump both directions from one thread with non-blocking I/O instead.
fn forward_connection(
    mut local: TcpStream,
    config: &SshConfig,
    password: Option<&str>,
    passphrase: Option<&str>,
    target_host: &str,
    target_port: u16,
) -> AppResult<()> {
    let session = connect_session(config, password, passphrase)?;
    let mut channel = session.channel_direct_tcpip(target_host, target_port, None)?;
    session.set_blocking(false);
    local.set_nonblocking(true)?;

    let mut upstream: Vec<u8> = Vec::new();
    let mut downstream: Vec<u8> = Vec::new();
    let mut buffer = [0u8; 32 * 1024];
    let mut local_open = true;
    loop {
        let mut progressed = false;

        if local_open && upstream.is_empty() {
            match local.read(&mut buffer) {
                Ok(0) => local_open = false,
                Ok(size) => {
                    upstream.extend_from_slice(&buffer[..size]);
                    progressed = true;
                }
                Err(error) if error.kind() == ErrorKind::WouldBlock => {}
                Err(_) => break,
            }
        }
        if !upstream.is_empty() {
            match channel.write(&upstream) {
                Ok(size) => {
                    upstream.drain(..size);
                    progressed = true;
                }
                Err(error) if error.kind() == ErrorKind::WouldBlock => {}
                Err(_) => break,
            }
        }
        if downstream.is_empty() {
            match channel.read(&mut buffer) {
                Ok(0) if channel.eof() => break,
                Ok(0) => {}
                Ok(size) => {
                    downstream.extend_from_slice(&buffer[..size]);
                    progressed = true;
                }
                Err(error) if error.kind() == ErrorKind::WouldBlock => {}
                Err(_) => break,
            }
        }
        if !downstream.is_empty() {
            match local.write(&downstream) {
                Ok(size) => {
                    downstream.drain(..size);
                    progressed = true;
                }
                Err(error) if error.kind() == ErrorKind::WouldBlock => {}
                Err(_) => break,
            }
        }

        if !local_open && upstream.is_empty() {
            break;
        }
        if !progressed {
            thread::sleep(Duration::from_millis(2));
        }
    }
    let _ = channel.close();
    Ok(())
}

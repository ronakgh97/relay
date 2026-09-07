use anyhow::Result;
use clap::{Parser, Subcommand};
use dashmap::DashMap;
use relay::START_TIME;
use relay::log::LOG_LEVEL;
use relay::log::Level;
use relay::rate_limit::IpRateLimiter;
use relay::{debug, error, info, warn};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::Duration;
use tokio::io::{AsyncReadExt, copy_bidirectional_with_sizes};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;
use tokio::time::sleep;

#[derive(Parser)]
#[command(
    name = "relay",
    version = "v1.0.0",
    about = "High-performance Duplex relay server over TCP",
    long_about = "High-performance Duplex relay server over TCP"
)]
struct Cli {
    #[command(subcommand)]
    command: CliArgs,
}
#[derive(Subcommand)]
enum CliArgs {
    /// Run the relay server
    Start {
        /// Address of the server to bind to (format: ip:port)
        #[arg(long, default_value = "0.0.0.0:8787")]
        server_addr: String,

        /// Maximum number of concurrent connections
        #[arg(long, default_value = "1024")]
        max_connections: usize,

        /// Maximum number of new connections per IP per rate
        #[arg(long, default_value = "60")]
        max_requests_per_ip: u32,

        /// Rate window in seconds for per-IP limiting
        #[arg(long, default_value = "60")]
        rate_window_secs: u64,

        /// Log level
        #[arg(long, default_value = "info")]
        log_level: Level,
    },
    /// Show server protocol information
    Protocol {},
}

/// Second arriver will notify the first arriver `Arc<Notify>` to start piping to the `TcpStream`
/// by taking ownership of waiting client, and then both will be removed from the map.
struct WaitingClient {
    stream: TcpStream,
    notify: Arc<Notify>,
}

/// Global session map to store waiting clients by their pairing key.
static SESSION_MAP: LazyLock<DashMap<[u8; 32], WaitingClient>> =
    LazyLock::new(|| DashMap::with_capacity(1 << 20));

#[tokio::main]
async fn main() -> Result<()> {
    let args = Cli::parse();

    match args.command {
        CliArgs::Start {
            server_addr,
            max_connections,
            max_requests_per_ip,
            rate_window_secs,
            log_level,
        } => {
            LOG_LEVEL.set(log_level).expect("Failed to set log level");
            run_server(
                server_addr,
                max_connections,
                max_requests_per_ip,
                rate_window_secs,
            )
            .await?;
        }
        CliArgs::Protocol { .. } => {
            println!("1. Client connects to the relay server and sends a 32-byte pairing key.");
            println!(
                "2. The first client to send a pairing key will be stored in the session map."
            );
            println!(
                "3. The second client to send the same pairing key will be paired with the first client."
            );
            println!("4. Both clients will then start piping data between each other.");
            println!(
                "5. If no second client arrives within 60 seconds, the first client will be removed from the session map."
            );
        }
    }

    Ok(())
}

pub async fn run_server(
    server_addr: String,
    max_connections: usize,
    max_requests_per_ip: u32,
    rate_window_secs: u64,
) -> Result<()> {
    let listener = TcpListener::bind(&server_addr).await?;
    let addr = listener.local_addr()?;

    START_TIME
        .set(chrono::Local::now())
        .expect("Failed to set start time");
    info!(
        "Relay server started on {} with max connections: {}, rate limit: {}/{}s per IP",
        addr, max_connections, max_requests_per_ip, rate_window_secs
    );

    let current_connections = Arc::new(AtomicUsize::new(0));
    let shutdown = Arc::new(Notify::new());
    let mut rate_limiter = IpRateLimiter::init(
        max_requests_per_ip,
        Duration::from_secs(rate_window_secs.max(1)),
    );

    loop {
        // Wait for either a new connection or a shutdown signal
        tokio::select! {
            _ = ctrl_c_handler() => {
                info!("Shutdown signal received, stopping server...");
                shutdown.notify_waiters();
                break;
            }
            res = listener.accept() => {
                match res {
                    Ok((socket, addr)) => {
                        let client_ip = addr.ip();
                        if !rate_limiter.check(client_ip) {
                            warn!("Rate limit exceeded for {}, rejecting", client_ip);
                            drop(socket);
                            continue;
                        }
                        if current_connections.fetch_add(1, Ordering::AcqRel) >= max_connections {
                            current_connections.fetch_sub(1, Ordering::AcqRel);
                            warn!("Maximum number of connections reached, rejecting {}", addr);
                            drop(socket);
                            continue;
                        }
                        debug!("Accepted connections from {}", addr);
                        let active_connections = Arc::clone(&current_connections);
                        let shutdown_signal = Arc::clone(&shutdown);
                        tokio::spawn(async move {
                            if let Err(e) = handle_client(socket, shutdown_signal).await {
                                error!("Error handling client (ip: {}): {:?}", addr, e);
                            };
                            active_connections.fetch_sub(1, Ordering::AcqRel);
                        });
                    }
                    Err(e) => {
                        error!("Error accepting connection from ({}): {:?}", addr, e);
                    }
                }
            }
        }
    }

    drop(listener);
    info!(
        "Waiting for {} active connections to complete...",
        current_connections.load(Ordering::Acquire)
    );
    // wait for all active connections to finish before shutting down (returning from main)
    while current_connections.load(Ordering::Acquire) != 0 {
        tokio::task::yield_now().await;
    }
    info!("Server shutdown complete.");

    Ok(())
}

/// Buffer size for A -> B
const AB: usize = 2 << 20;
/// Buffer size for B -> A
const BA: usize = 2 << 20;

#[inline(always)]
pub async fn handle_client(mut stream: TcpStream, shutdown_signal: Arc<Notify>) -> Result<()> {
    stream.set_nodelay(true)?;

    let mut pairing_key = [0u8; 32];
    stream.read_exact(&mut pairing_key).await?;

    // second arriver, take waiter's socket and start piping
    if let Some((_, waiting_client)) = SESSION_MAP.remove(&pairing_key) {
        let mut waiting_stream = waiting_client.stream;
        waiting_client.notify.notify_one();
        info!(
            "Clients paired: {} <--> {}",
            stream.peer_addr()?,
            waiting_stream.peer_addr()?
        );
        let (a, b) =
            copy_bidirectional_with_sizes(&mut stream, &mut waiting_stream, AB, BA).await?;
        info!(
            "Bytes transferred: ({}) {} <--> ({}) {}",
            stream.peer_addr()?,
            a,
            waiting_stream.peer_addr()?,
            b
        );
        return Ok(());
    }

    // first arriver, store socket, park until paired or timeout or shutdown
    let waiter_signal = Arc::new(Notify::new());
    let waiting_client = WaitingClient {
        stream,
        notify: Arc::clone(&waiter_signal),
    };
    SESSION_MAP.insert(pairing_key, waiting_client);

    // wait for second arriver (notify the waiter), timeout, or shutdown
    // shutdown disconnects waiter and remove entry (drops parked stream) and exit
    tokio::select! {
        _ = waiter_signal.notified() => {}
        _ = sleep(Duration::from_secs(60)) => {
            SESSION_MAP.remove(&pairing_key);
        }
        _ = shutdown_signal.notified() => {
            SESSION_MAP.remove(&pairing_key);
        }
    }

    Ok(())
}

/// Cross-platform Ctrl+C handler that also handles SIGTERM on Unix systems.
async fn ctrl_c_handler() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("Failed Ctrl+C handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}

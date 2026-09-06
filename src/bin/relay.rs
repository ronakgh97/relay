use anyhow::Result;
use clap::{Parser, Subcommand};
use dashmap::DashMap;
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
    version = "1.0.0",
    about = "High-performance Duplex relay server over TCP",
    long_about = "None"
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

static SESSION_MAP: LazyLock<DashMap<[u8; 32], WaitingClient>> =
    LazyLock::new(|| DashMap::with_capacity(1024));

#[tokio::main]
async fn main() -> Result<()> {
    let args = Cli::parse();

    match args.command {
        CliArgs::Start {
            server_addr,
            max_connections,
        } => {
            run_server(server_addr, max_connections).await?;
        }
        CliArgs::Protocol { .. } => {
            println!("SERVER PROTOCOL:");
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
pub async fn run_server(server_addr: String, max_connections: usize) -> Result<()> {
    let listener = TcpListener::bind(&server_addr).await?;
    let addr = listener.local_addr()?;

    println!("Relay server listening on {}", addr);

    let cuurent_connections = Arc::new(AtomicUsize::new(0));
    loop {
        if cuurent_connections.fetch_add(1, Ordering::Relaxed) >= max_connections {
            cuurent_connections.fetch_sub(1, Ordering::Relaxed);
            println!("Maximum number of connections reached");
            sleep(Duration::from_secs(3)).await;
            continue;
        }

        match listener.accept().await {
            Ok((socket, addr)) => {
                println!("Accepted client from {}", addr);
                let active_connections = Arc::clone(&cuurent_connections);
                tokio::spawn(async move {
                    if let Err(e) = handle_client(socket).await {
                        println!("Error handling client (ip: {}): {:?}", addr, e);
                    };
                    active_connections.fetch_sub(1, Ordering::Relaxed);
                });
            }
            Err(e) => {
                println!("Error accepting connection: {:?}", e);
                cuurent_connections.fetch_sub(1, Ordering::Relaxed);
            }
        }
    }
}

#[inline(always)]
pub async fn handle_client(mut stream: TcpStream) -> Result<()> {
    stream.set_nodelay(true)?;

    let mut pairing_key = [0u8; 32];
    stream.read_exact(&mut pairing_key).await?;

    // second arriver, take waiter's socket and start piping.
    if let Some((_, waiting)) = SESSION_MAP.remove(&pairing_key) {
        let mut waiting_stream = waiting.stream;
        waiting.notify.notify_one();
        println!(
            "Paired client: {} <--> {}",
            stream.peer_addr()?,
            waiting_stream.peer_addr()?
        );
        let (a, b) =
            copy_bidirectional_with_sizes(&mut stream, &mut waiting_stream, 16384, 16384).await?;
        println!(
            "Bytes transferred: ({}) {} <--> ({}) {}",
            stream.peer_addr()?,
            a,
            waiting_stream.peer_addr()?,
            b
        );
        return Ok(());
    }

    // first arriver, store socket, park until paired or timeout.
    let notify = Arc::new(Notify::new());
    let waiting_client = WaitingClient {
        stream,
        notify: Arc::clone(&notify),
    };
    SESSION_MAP.insert(pairing_key, waiting_client);

    // wait for second arriver (notify the waiter) or timeout, then remove from map.
    tokio::select! {
        _ = notify.notified() => {}
        _ = sleep(Duration::from_secs(60)) => {
            SESSION_MAP.remove(&pairing_key);
        }
    }

    Ok(())
}

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
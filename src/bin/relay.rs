use anyhow::Result;
use dashmap::DashMap;
use std::sync::{Arc, LazyLock};
use tokio::io::{AsyncReadExt, copy_bidirectional_with_sizes};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;

static SESSION_MAP: LazyLock<DashMap<[u8; 32], WaitingClient>> =
    LazyLock::new(|| DashMap::with_capacity(1024));

struct WaitingClient {
    stream: TcpStream,
    notify: Arc<Notify>,
}

/*
Simple duplex relay server, accepts connections
react_exact() 32 bytes for matching clients (one-to-one) only
spawn task to handle them and using tokio-bidirectional copy until EOF or error, then close both sides
*/
#[tokio::main]
async fn main() -> Result<()> {
    let listener = TcpListener::bind("0.0.0.0:8787").await?;
    let addr = listener.local_addr()?;

    println!("Relay server listening on {}", addr);

    loop {
        let (socket, addr) = listener.accept().await?;
        println!("Accepted connection from {}", addr);
        tokio::spawn(async move {
            if let Err(e) = handle_connection(socket).await {
                println!("Error handling client connection (ip: {}): {:?}", addr, e);
            };
        });
    }
}

pub async fn handle_connection(mut stream: TcpStream) -> Result<()> {
    stream.set_nodelay(true)?;

    let mut key = [0u8; 32];
    stream.read_exact(&mut key).await?;

    if let Some((_, waiting)) = SESSION_MAP.remove(&key) {
        waiting.notify.notify_one();
        let mut peer_stream = waiting.stream;
        println!(
            "Paired client: {} <--> {}",
            stream.peer_addr()?,
            peer_stream.peer_addr()?
        );
        let (a, b) =
            copy_bidirectional_with_sizes(&mut stream, &mut peer_stream, 16384, 16384).await?;
        println!(
            "Bytes transferred: ({}) {} <--> ({}) {}",
            stream.peer_addr()?,
            a,
            peer_stream.peer_addr()?,
            b
        );
    } else {
        let notify = Arc::new(Notify::new());
        let waiting = WaitingClient {
            stream,
            notify: Arc::clone(&notify),
        };
        SESSION_MAP.insert(key, waiting);
        notify.notified().await;
    }

    Ok(())
}

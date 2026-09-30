//! Real OS TCP socket transport with length-prefixed message framing.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Mutex};
use graft_core::Message;

/// Transport network errors.
#[derive(Debug, Error)]
pub enum TransportError {
    /// Standard I/O socket error.
    #[error("TCP socket I/O error: {0}")]
    Io(#[from] std::io::Error),
    /// Message serialization or deserialization failure.
    #[error("message serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    /// Connection closed prematurely.
    #[error("remote connection closed")]
    ConnectionClosed,
}

/// Asynchronous TCP network transport managing peer connections.
pub struct TcpTransport {
    local_addr: SocketAddr,
    outbound_conns: Arc<Mutex<HashMap<SocketAddr, TcpStream>>>,
}

impl TcpTransport {
    /// Constructs a transport from an already-bound TCP listener.
    pub fn from_listener(listener: TcpListener) -> Result<(Self, mpsc::Receiver<Message>), TransportError> {
        let local_addr = listener.local_addr()?;
        let (inbound_tx, inbound_rx) = mpsc::channel(1024);

        tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                let tx = inbound_tx.clone();
                tokio::spawn(async move {
                    while let Ok(msg) = Self::read_frame(&mut socket).await {
                        if tx.send(msg).await.is_err() {
                            break;
                        }
                    }
                });
            }
        });

        Ok((
            Self {
                local_addr,
                outbound_conns: Arc::new(Mutex::new(HashMap::new())),
            },
            inbound_rx,
        ))
    }

    /// Binds a TCP listener on the specified local address, returning transport and inbound message channel.
    pub async fn bind(addr: SocketAddr) -> Result<(Self, mpsc::Receiver<Message>), TransportError> {
        let listener = TcpListener::bind(addr).await?;
        Self::from_listener(listener)
    }

    /// Returns the local bound address.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Sends a consensus message to a target address over TCP.
    pub async fn send_message(&self, target: SocketAddr, msg: &Message) -> Result<(), TransportError> {
        let payload = serde_json::to_vec(msg)?;
        let mut conns = self.outbound_conns.lock().await;

        if let Some(stream) = conns.get_mut(&target) {
            if Self::write_frame(stream, &payload).await.is_ok() {
                return Ok(());
            }
        }

        conns.remove(&target);
        let mut new_stream = TcpStream::connect(target).await?;
        Self::write_frame(&mut new_stream, &payload).await?;
        conns.insert(target, new_stream);
        Ok(())
    }

    async fn write_frame(stream: &mut TcpStream, data: &[u8]) -> Result<(), TransportError> {
        let len = data.len() as u32;
        stream.write_all(&len.to_be_bytes()).await?;
        stream.write_all(data).await?;
        stream.flush().await?;
        Ok(())
    }

    async fn read_frame(stream: &mut TcpStream) -> Result<Message, TransportError> {
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf).await?;
        let len = u32::from_be_bytes(len_buf) as usize;

        let mut buf = vec![0u8; len];
        stream.read_exact(&mut buf).await?;
        let msg: Message = serde_json::from_slice(&buf)?;
        Ok(msg)
    }
}

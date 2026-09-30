//! Production-grade consensus node server orchestrating transport, storage, health, and state machine.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;
use thiserror::Error;
use tokio::sync::Mutex;
use graft_blob::PayloadManager;
use graft_core::{CommitApplyPipeline, Index, NodeId, RaftConfig, RaftEngine, Role};
use graft_health::{HealthMonitor, HealthSignal, HealthThresholds, MonitorConfig};
use graft_storage::SharedWal;
use crate::transport::{TcpTransport, TransportError};

/// Server runtime errors.
#[derive(Debug, Error)]
pub enum ServerError {
    /// Standard I/O error.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    /// Network transport failure.
    #[error("transport error: {0}")]
    Transport(#[from] TransportError),
    /// WAL storage error.
    #[error("storage error: {0}")]
    Storage(#[from] graft_storage::WalError),
    /// Node is not the current leader.
    #[error("not current leader")]
    NotLeader,
}

/// Standalone consensus node server instance.
pub struct GraftServer {
    /// Node unique identifier.
    pub id: NodeId,
    engine: RaftEngine,
    wal: SharedWal,
    last_persisted: Index,
    health: HealthMonitor,
    transport: TcpTransport,
    payloads: PayloadManager,
    pipeline: CommitApplyPipeline,
    peer_addresses: HashMap<NodeId, SocketAddr>,
    state_machine: HashMap<Vec<u8>, Vec<u8>>,
    running: Arc<AtomicBool>,
}

impl GraftServer {
    /// Instantiates and initializes a new server.
    pub async fn start<P: AsRef<Path>>(
        config: RaftConfig,
        wal_path: P,
        listen_addr: SocketAddr,
        peer_addresses: HashMap<NodeId, SocketAddr>,
    ) -> Result<Arc<Mutex<Self>>, ServerError> {
        let listener = tokio::net::TcpListener::bind(listen_addr).await?;
        Self::start_with_listener(config, wal_path, listener, peer_addresses)
    }

    /// Instantiates a server from an already-bound TCP listener.
    pub fn start_with_listener<P: AsRef<Path>>(
        config: RaftConfig,
        wal_path: P,
        listener: tokio::net::TcpListener,
        peer_addresses: HashMap<NodeId, SocketAddr>,
    ) -> Result<Arc<Mutex<Self>>, ServerError> {
        let id = config.id;
        let wal = SharedWal::open(wal_path)?;
        let mut engine = RaftEngine::new(config);

        let recovered = wal.read_group(1);
        let mut last_persisted = 0;
        if !recovered.is_empty() {
            last_persisted = recovered.last().unwrap().index;
            let _ = engine.log.append(recovered);
        }

        let health = HealthMonitor::new(id, MonitorConfig::default(), HealthThresholds::default());
        let (transport, mut inbound_rx) = TcpTransport::from_listener(listener)?;
        let payloads = PayloadManager::new(64 * 1024);
        let pipeline = CommitApplyPipeline::new(0, 5000);
        let running = Arc::new(AtomicBool::new(true));

        let server = Arc::new(Mutex::new(Self {
            id,
            engine,
            wal,
            last_persisted,
            health,
            transport,
            payloads,
            pipeline,
            peer_addresses,
            state_machine: HashMap::new(),
            running: running.clone(),
        }));

        let loop_server = server.clone();
        let loop_running = running.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(tokio::time::Duration::from_millis(20));
            while loop_running.load(Ordering::Relaxed) {
                ticker.tick().await;
                let mut s = loop_server.lock().await;
                let _ = s.process_tick().await;
            }
        });

        let rx_server = server.clone();
        let rx_running = running.clone();
        tokio::spawn(async move {
            while let Some(msg) = inbound_rx.recv().await {
                if !rx_running.load(Ordering::Relaxed) {
                    break;
                }
                let mut s = rx_server.lock().await;
                s.engine.step(msg);
                s.flush_and_send().await;
            }
        });

        Ok(server)
    }

    /// Submits a key-value write proposal through this server.
    pub async fn propose_kv(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<Index, ServerError> {
        if self.engine.role != Role::Leader {
            return Err(ServerError::NotLeader);
        }

        let mut raw = Vec::with_capacity(8 + key.len() + value.len());
        raw.extend_from_slice(&(key.len() as u32).to_be_bytes());
        raw.extend_from_slice(&key);
        raw.extend_from_slice(&value);

        let prepared = self.payloads.prepare_proposal(raw);
        let index = self
            .engine
            .propose(prepared)
            .map_err(|_| ServerError::NotLeader)?;

        self.flush_and_send().await;
        Ok(index)
    }

    /// Reads a value from the local state machine.
    pub fn read_local(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.state_machine.get(key).cloned()
    }

    /// Returns the current consensus role.
    pub fn role(&self) -> Role {
        self.engine.role
    }

    /// Returns the current consensus term.
    pub fn term(&self) -> u64 {
        self.engine.current_term
    }

    /// Returns the commit index.
    pub fn commit_index(&self) -> Index {
        self.engine.commit_index
    }

    /// Returns a mutable reference to the health monitor.
    pub fn health_mut(&mut self) -> &mut HealthMonitor {
        &mut self.health
    }

    /// Gracefully stops the server runtime.
    pub fn stop(&mut self) {
        self.running.store(false, Ordering::Relaxed);
    }

    /// Manually triggers a logical consensus tick and message delivery.
    pub async fn process_tick(&mut self) -> Result<(), ServerError> {
        self.engine.tick();
        self.health.tick();

        if self.engine.role == Role::Leader {
            if let Some(target) = self.health.should_step_down() {
                let _ = self.engine.transfer_leadership(target);
            }
        }

        self.flush_and_send().await;
        Ok(())
    }

    async fn flush_and_send(&mut self) {
        let committed = self.engine.drain_committed_entries();
        if !committed.is_empty() {
            self.pipeline.enqueue_committed(committed);
            let to_apply = self.pipeline.pop_batch(100);

            for entry in to_apply {
                if let Ok(resolved) = self.payloads.resolve_entry(&entry.data) {
                    if resolved.len() >= 4 {
                        let klen = u32::from_be_bytes(resolved[0..4].try_into().unwrap()) as usize;
                        if resolved.len() >= 4 + klen {
                            let key = resolved[4..4 + klen].to_vec();
                            let val = resolved[4 + klen..].to_vec();
                            self.state_machine.insert(key, val);
                        }
                    }
                }
            }
        }

        let last_log = self.engine.log.last_index();
        if last_log > self.last_persisted {
            let start_idx = self.last_persisted + 1;
            let count = (last_log - self.last_persisted) as usize;
            let unpersisted = self.engine.log.slice(start_idx, count);

            if !unpersisted.is_empty() {
                let start = Instant::now();
                let _ = self.wal.append(1, &unpersisted);
                let _ = self.wal.sync();
                self.last_persisted = last_log;
                let elapsed_us = start.elapsed().as_micros() as u64;

                let signal = HealthSignal::new(
                    self.id,
                    elapsed_us,
                    0,
                    self.pipeline.pending_count(),
                    start.elapsed().as_millis() as u64,
                );
                self.health.record_local_sample(&signal);
            }
        }

        let outbox = self.engine.take_outbox();
        for msg in outbox {
            if let Some(&addr) = self.peer_addresses.get(&msg.to) {
                let _ = self.transport.send_message(addr, &msg).await;
            }
        }
    }
}

//! Health signal data structures.

use serde::{Deserialize, Serialize};
use graft_core::NodeId;

/// Snapshot of operational health metrics published by a cluster node.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct HealthSignal {
    /// Identifier of the reporting node.
    pub node_id: NodeId,
    /// Measured disk fsync latency in microseconds.
    pub fsync_latency_us: u64,
    /// Number of entries trailing behind the leader.
    pub replication_lag_entries: u64,
    /// Backlog of pending unapplied proposals.
    pub pending_queue_depth: usize,
    /// Timestamp in milliseconds when this sample was recorded.
    pub timestamp_ms: u64,
}

impl HealthSignal {
    /// Creates a new health signal snapshot.
    pub fn new(
        node_id: NodeId,
        fsync_latency_us: u64,
        replication_lag_entries: u64,
        pending_queue_depth: usize,
        timestamp_ms: u64,
    ) -> Self {
        Self {
            node_id,
            fsync_latency_us,
            replication_lag_entries,
            pending_queue_depth,
            timestamp_ms,
        }
    }
}

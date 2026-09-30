//! ReadIndex protocol implementation for zero-disk linearizable reads.

use std::collections::{HashMap, HashSet};
use graft_core::{Index, NodeId, QuorumModel};

/// State of an in-flight ReadIndex request waiting for quorum confirmation.
#[derive(Debug, Clone)]
pub struct PendingRead {
    /// Unique request identifier.
    pub request_id: u64,
    /// Commit index captured when the read was initiated.
    pub read_index: Index,
    /// Peers that acknowledged leadership for this read.
    pub acks: HashSet<NodeId>,
    /// Whether quorum confirmation has succeeded.
    pub confirmed: bool,
}

/// Manager tracking active ReadIndex requests.
pub struct ReadIndexManager {
    pending: HashMap<u64, PendingRead>,
}

impl ReadIndexManager {
    /// Creates a new ReadIndex manager.
    pub fn new() -> Self {
        Self {
            pending: HashMap::new(),
        }
    }

    /// Registers a new read request at the leader's current commit index.
    pub fn initiate_read(&mut self, request_id: u64, commit_index: Index, leader_id: NodeId) {
        let mut acks = HashSet::new();
        acks.insert(leader_id);
        self.pending.insert(
            request_id,
            PendingRead {
                request_id,
                read_index: commit_index,
                acks,
                confirmed: false,
            },
        );
    }

    /// Records a heartbeat acknowledgment from a peer for an active read.
    pub fn record_ack(
        &mut self,
        request_id: u64,
        from: NodeId,
        voters: &[NodeId],
        quorum_model: &QuorumModel,
    ) -> bool {
        if let Some(read) = self.pending.get_mut(&request_id) {
            read.acks.insert(from);
            if !read.confirmed && quorum_model.is_election_quorum(&read.acks, voters) {
                read.confirmed = true;
                return true;
            }
        }
        false
    }

    /// Returns request IDs that have both achieved quorum and been satisfied by `last_applied`.
    pub fn ready_reads(&mut self, last_applied: Index) -> Vec<u64> {
        let mut ready = Vec::new();
        self.pending.retain(|id, read| {
            if read.confirmed && last_applied >= read.read_index {
                ready.push(*id);
                false
            } else {
                true
            }
        });
        ready
    }

    /// Returns the number of currently pending read requests.
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }
}

impl Default for ReadIndexManager {
    fn default() -> Self {
        Self::new()
    }
}

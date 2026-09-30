//! Fundamental Raft data types and role definitions.

use serde::{Deserialize, Serialize};

/// Monotonically increasing consensus term.
pub type Term = u64;

/// Monotonically increasing consensus log index.
pub type Index = u64;

/// Unique cluster node identifier.
pub type NodeId = u64;

/// Node consensus role.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Role {
    /// Passive replica following the current leader.
    Follower,
    /// Speculative candidate testing quorum reachability.
    PreCandidate,
    /// Active candidate soliciting votes for a new term.
    Candidate,
    /// Authoritative cluster leader.
    Leader,
}

/// Category of log entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EntryType {
    /// Standard application command.
    Normal,
    /// Cluster membership change.
    Configuration,
    /// Empty entry committed at the start of a leader term.
    NoOp,
}

/// Single consensus log entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogEntry {
    /// Term in which this entry was created.
    pub term: Term,
    /// Monotonic log index of this entry.
    pub index: Index,
    /// Entry classification.
    pub entry_type: EntryType,
    /// Serialized payload data.
    pub data: Vec<u8>,
}

impl LogEntry {
    /// Creates a new normal log entry.
    pub fn new_normal(term: Term, index: Index, data: Vec<u8>) -> Self {
        Self {
            term,
            index,
            entry_type: EntryType::Normal,
            data,
        }
    }

    /// Creates a new no-op log entry.
    pub fn new_noop(term: Term, index: Index) -> Self {
        Self {
            term,
            index,
            entry_type: EntryType::NoOp,
            data: Vec::new(),
        }
    }
}

/// Durable consensus metadata that must be persisted before acknowledging messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct HardState {
    /// Current known term.
    pub term: Term,
    /// Node voted for in the current term.
    pub voted_for: Option<NodeId>,
    /// Highest log entry known to be committed.
    pub commit_index: Index,
}

//! Consensus protocol message definitions.

use serde::{Deserialize, Serialize};
use crate::types::{Index, LogEntry, NodeId, Term};

/// Raft consensus message envelope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Message {
    /// Originating node.
    pub from: NodeId,
    /// Destination node.
    pub to: NodeId,
    /// Consensus term associated with this message.
    pub term: Term,
    /// Specific payload.
    pub payload: MessagePayload,
}

/// Message payload variants for Raft consensus.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MessagePayload {
    /// Request pre-vote before initiating a formal election.
    PreVoteRequest {
        /// Candidate log last index.
        last_log_index: Index,
        /// Candidate log last term.
        last_log_term: Term,
    },
    /// Response to a pre-vote solicitation.
    PreVoteResponse {
        /// True if the respondent would vote for the candidate.
        vote_granted: bool,
    },
    /// Formal vote request for leadership election.
    VoteRequest {
        /// Candidate log last index.
        last_log_index: Index,
        /// Candidate log last term.
        last_log_term: Term,
    },
    /// Response to a formal vote solicitation.
    VoteResponse {
        /// True if vote was granted to the candidate.
        vote_granted: bool,
    },
    /// Log replication request from the leader.
    AppendEntriesRequest {
        /// Index of log entry immediately preceding new ones.
        prev_log_index: Index,
        /// Term of `prev_log_index` entry.
        prev_log_term: Term,
        /// Log entries to store.
        entries: Vec<LogEntry>,
        /// Leader's commit index.
        leader_commit: Index,
    },
    /// Response to log replication request.
    AppendEntriesResponse {
        /// True if follower contained entry matching `prev_log_index` and `prev_log_term`.
        success: bool,
        /// Highest log index replicated by the follower.
        match_index: Index,
    },
    /// Lightweight periodic heartbeat message from leader.
    HeartbeatRequest {
        /// Leader's commit index.
        commit_index: Index,
    },
    /// Acknowledgment of heartbeat.
    HeartbeatResponse,
    /// Immediate election trigger sent during leadership transfer.
    TimeoutNow,
}

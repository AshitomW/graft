//! Configuration options and validation for a Raft consensus node.

use std::collections::HashSet;
use thiserror::Error;
use crate::types::NodeId;

/// Configuration validation errors.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ConfigError {
    /// Node identifier cannot be 0.
    #[error("node id cannot be 0")]
    ZeroNodeId,
    /// Heartbeat interval must be greater than 0.
    #[error("heartbeat interval must be > 0")]
    ZeroHeartbeatTick,
    /// Election timeout must be strictly greater than heartbeat interval.
    #[error("election timeout ({election}) must be > heartbeat interval ({heartbeat})")]
    InvalidElectionTick {
        /// Configured election tick.
        election: u64,
        /// Configured heartbeat tick.
        heartbeat: u64,
    },
    /// Node was not present in the voting peers set.
    #[error("node {0} not present in voting peers")]
    MissingSelfInPeers(NodeId),
    /// Duplicate peer identifier found.
    #[error("duplicate peer detected: {0}")]
    DuplicatePeer(NodeId),
}

/// Configuration settings for a Raft node instance.
#[derive(Debug, Clone)]
pub struct RaftConfig {
    /// Unique identifier of this node.
    pub id: NodeId,
    /// Voting peer node identifiers including self.
    pub peers: Vec<NodeId>,
    /// Non-voting learner node identifiers.
    pub learners: Vec<NodeId>,
    /// Ticks before an election timeout fires.
    pub election_tick: u64,
    /// Ticks between leader heartbeat emissions.
    pub heartbeat_tick: u64,
    /// Enable pre-vote phase before elections.
    pub pre_vote_enabled: bool,
    /// Enable periodic quorum checks by the leader.
    pub check_quorum: bool,
}

impl RaftConfig {
    /// Validates configuration parameters.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.id == 0 {
            return Err(ConfigError::ZeroNodeId);
        }
        if self.heartbeat_tick == 0 {
            return Err(ConfigError::ZeroHeartbeatTick);
        }
        if self.election_tick <= self.heartbeat_tick {
            return Err(ConfigError::InvalidElectionTick {
                election: self.election_tick,
                heartbeat: self.heartbeat_tick,
            });
        }

        let mut seen = HashSet::new();
        for &peer in &self.peers {
            if !seen.insert(peer) {
                return Err(ConfigError::DuplicatePeer(peer));
            }
        }

        if !self.learners.contains(&self.id) && !seen.contains(&self.id) {
            return Err(ConfigError::MissingSelfInPeers(self.id));
        }

        Ok(())
    }
}

//! Flexible quorum models and replica classifications.

use std::collections::HashSet;
use thiserror::Error;
use crate::types::NodeId;

/// Quorum validation errors.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum QuorumError {
    /// Write quorum and read quorum do not intersect.
    #[error("quorum intersection violation: write quorum ({write_quorum}) + read quorum ({read_quorum}) <= total voters ({total_voters})")]
    DisjointQuorums {
        /// Configured write quorum size.
        write_quorum: usize,
        /// Configured read quorum size.
        read_quorum: usize,
        /// Total number of voting nodes.
        total_voters: usize,
    },
    /// Configured quorum size exceeds total voter population.
    #[error("quorum size {size} exceeds voter count {voter_count}")]
    QuorumSizeTooLarge {
        /// Quorum size.
        size: usize,
        /// Voter count.
        voter_count: usize,
    },
    /// Empty voters list provided.
    #[error("voter set cannot be empty")]
    EmptyVoters,
}

/// Node replica role in the consensus cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaRole {
    /// Full replica participating in elections, storing logs, and applying to state machine.
    Full,
    /// Witness node participating in quorums without storing full payloads or applying state.
    Witness,
    /// Log-only replica persisting logs for durability without applying state.
    LogOnly,
}

/// Flexible quorum specification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QuorumModel {
    /// Classical simple majority: size > N / 2.
    Majority,
    /// Flexible quorum with separate write and election/read sizes satisfying Q_w + Q_r > N.
    Flexible {
        /// Minimum nodes required to acknowledge a write.
        write_quorum: usize,
        /// Minimum nodes required to elect a leader or perform read quorum.
        read_quorum: usize,
    },
}

impl QuorumModel {
    /// Validates that all possible write and read quorums intersect.
    pub fn validate(&self, voters: &[NodeId]) -> Result<(), QuorumError> {
        let n = voters.len();
        if n == 0 {
            return Err(QuorumError::EmptyVoters);
        }

        match *self {
            QuorumModel::Majority => Ok(()),
            QuorumModel::Flexible {
                write_quorum,
                read_quorum,
            } => {
                if write_quorum > n {
                    return Err(QuorumError::QuorumSizeTooLarge {
                        size: write_quorum,
                        voter_count: n,
                    });
                }
                if read_quorum > n {
                    return Err(QuorumError::QuorumSizeTooLarge {
                        size: read_quorum,
                        voter_count: n,
                    });
                }
                if write_quorum + read_quorum <= n {
                    return Err(QuorumError::DisjointQuorums {
                        write_quorum,
                        read_quorum,
                        total_voters: n,
                    });
                }
                Ok(())
            }
        }
    }

    /// Evaluates if the set of acknowledging nodes satisfies the write quorum.
    pub fn is_write_quorum(&self, acks: &HashSet<NodeId>, voters: &[NodeId]) -> bool {
        let voter_set: HashSet<NodeId> = voters.iter().copied().collect();
        let valid_acks = acks.iter().filter(|id| voter_set.contains(id)).count();
        let n = voters.len();

        match *self {
            QuorumModel::Majority => valid_acks > n / 2,
            QuorumModel::Flexible { write_quorum, .. } => valid_acks >= write_quorum,
        }
    }

    /// Evaluates if the set of voting nodes satisfies the election/read quorum.
    pub fn is_election_quorum(&self, votes: &HashSet<NodeId>, voters: &[NodeId]) -> bool {
        let voter_set: HashSet<NodeId> = voters.iter().copied().collect();
        let valid_votes = votes.iter().filter(|id| voter_set.contains(id)).count();
        let n = voters.len();

        match *self {
            QuorumModel::Majority => valid_votes > n / 2,
            QuorumModel::Flexible { read_quorum, .. } => valid_votes >= read_quorum,
        }
    }
}

impl Default for QuorumModel {
    fn default() -> Self {
        QuorumModel::Majority
    }
}

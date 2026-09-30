//! Range metadata management and key-to-group routing (Range 0 coordination).

use serde::{Deserialize, Serialize};
use thiserror::Error;
use graft_core::NodeId;
use crate::group::GroupId;

/// Errors during range operations.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RangeError {
    /// Specified range identifier was not found.
    #[error("range {0} not found")]
    NotFound(GroupId),
    /// Split key is outside range boundaries.
    #[error("split key outside range boundaries")]
    InvalidSplitKey,
    /// Non-adjacent ranges cannot be merged.
    #[error("cannot merge non-adjacent ranges {left} and {right}")]
    NonAdjacentMerge {
        /// Left range.
        left: GroupId,
        /// Right range.
        right: GroupId,
    },
}

/// Contiguous key range descriptor hosted by a consensus group.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RangeDescriptor {
    /// Unique range identifier.
    pub range_id: GroupId,
    /// Inclusive lower key bound.
    pub start_key: Vec<u8>,
    /// Exclusive upper key bound (empty represents positive infinity).
    pub end_key: Vec<u8>,
    /// Replica node placements.
    pub replicas: Vec<NodeId>,
}

impl RangeDescriptor {
    /// Returns true if the key falls within this range [start_key, end_key).
    pub fn contains_key(&self, key: &[u8]) -> bool {
        if key < self.start_key.as_slice() {
            return false;
        }
        if !self.end_key.is_empty() && key >= self.end_key.as_slice() {
            return false;
        }
        true
    }
}

/// Routing table maintained by the metadata group (Range 0).
#[derive(Debug, Clone, Default)]
pub struct MetadataRouter {
    ranges: Vec<RangeDescriptor>,
}

impl MetadataRouter {
    /// Creates a new router with an initial root range covering all keys ["" .. \inf).
    pub fn new_with_root(root_group_id: GroupId, initial_replicas: Vec<NodeId>) -> Self {
        Self {
            ranges: vec![RangeDescriptor {
                range_id: root_group_id,
                start_key: Vec::new(),
                end_key: Vec::new(),
                replicas: initial_replicas,
            }],
        }
    }

    /// Finds the consensus group responsible for the specified key.
    pub fn find_group(&self, key: &[u8]) -> Option<GroupId> {
        self.ranges
            .iter()
            .find(|r| r.contains_key(key))
            .map(|r| r.range_id)
    }

    /// Splits an existing range into two adjacent ranges at `split_key`.
    pub fn split(
        &mut self,
        range_id: GroupId,
        split_key: Vec<u8>,
        new_range_id: GroupId,
    ) -> Result<(), RangeError> {
        let idx = self
            .ranges
            .iter()
            .position(|r| r.range_id == range_id)
            .ok_or(RangeError::NotFound(range_id))?;

        let current = &self.ranges[idx];
        if split_key <= current.start_key
            || (!current.end_key.is_empty() && split_key >= current.end_key)
        {
            return Err(RangeError::InvalidSplitKey);
        }

        let old_end = current.end_key.clone();
        let replicas = current.replicas.clone();

        self.ranges[idx].end_key = split_key.clone();

        let new_range = RangeDescriptor {
            range_id: new_range_id,
            start_key: split_key,
            end_key: old_end,
            replicas,
        };

        self.ranges.insert(idx + 1, new_range);
        Ok(())
    }

    /// Merges two adjacent ranges.
    pub fn merge(&mut self, left_id: GroupId, right_id: GroupId) -> Result<(), RangeError> {
        let left_idx = self
            .ranges
            .iter()
            .position(|r| r.range_id == left_id)
            .ok_or(RangeError::NotFound(left_id))?;

        if left_idx + 1 >= self.ranges.len() || self.ranges[left_idx + 1].range_id != right_id {
            return Err(RangeError::NonAdjacentMerge {
                left: left_id,
                right: right_id,
            });
        }

        let right_end = self.ranges[left_idx + 1].end_key.clone();
        self.ranges[left_idx].end_key = right_end;
        self.ranges.remove(left_idx + 1);
        Ok(())
    }

    /// Returns the total number of managed ranges.
    pub fn range_count(&self) -> usize {
        self.ranges.len()
    }
}

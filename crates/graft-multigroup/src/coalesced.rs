//! Coalesced network batching across multi-groups.

use serde::{Deserialize, Serialize};
use graft_core::{Message, NodeId};
use crate::group::GroupId;

/// A batch of consensus messages multiplexed across multiple groups for a single destination node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CoalescedBatch {
    /// Target physical node.
    pub target_node: NodeId,
    /// Multiplexed group messages.
    pub messages: Vec<(GroupId, Message)>,
}

impl CoalescedBatch {
    /// Creates a new empty coalesced batch for the given target node.
    pub fn new(target_node: NodeId) -> Self {
        Self {
            target_node,
            messages: Vec::new(),
        }
    }

    /// Adds a group message to this batch.
    pub fn push(&mut self, group_id: GroupId, message: Message) {
        self.messages.push((group_id, message));
    }

    /// Returns the number of group messages in this batch.
    pub fn len(&self) -> usize {
        self.messages.len()
    }

    /// Returns true if the batch contains no messages.
    pub fn is_empty(&self) -> bool {
        self.messages.is_empty()
    }
}

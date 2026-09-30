//! Multi-group message router and tick coordinator.

use std::collections::{BTreeMap, HashMap};
use graft_core::{Index, Message, NodeId, RaftEngine};
use crate::coalesced::CoalescedBatch;
use crate::group::{GroupId, GroupState, GroupStatus};

/// Coordinator managing multiple co-located Raft consensus groups with shared coalesced transport.
pub struct MultiGroupRouter {
    groups: BTreeMap<GroupId, GroupState>,
}

impl MultiGroupRouter {
    /// Creates an empty multi-group router.
    pub fn new() -> Self {
        Self {
            groups: BTreeMap::new(),
        }
    }

    /// Registers a new consensus group with quiescence threshold.
    pub fn add_group(&mut self, group_id: GroupId, engine: RaftEngine, max_idle_ticks: u64) {
        self.groups
            .insert(group_id, GroupState::new(engine, max_idle_ticks));
    }

    /// Routes an incoming message to the specified group, waking it if quiescent.
    pub fn step(&mut self, group_id: GroupId, msg: Message) -> Result<(), &'static str> {
        let group = self.groups.get_mut(&group_id).ok_or("group not found")?;
        group.wake();
        group.engine.step(msg);
        Ok(())
    }

    /// Submits a proposal to the specified group, waking it if quiescent.
    pub fn propose(&mut self, group_id: GroupId, data: Vec<u8>) -> Result<Index, &'static str> {
        let group = self.groups.get_mut(&group_id).ok_or("group not found")?;
        group.wake();
        group.engine.propose(data)
    }

    /// Advances logical tick across all groups and returns coalesced network message batches.
    pub fn tick(&mut self) -> Vec<CoalescedBatch> {
        let mut node_batches: HashMap<NodeId, CoalescedBatch> = HashMap::new();

        for (&group_id, group) in self.groups.iter_mut() {
            let active = group.tick();
            if active {
                for msg in group.engine.take_outbox() {
                    node_batches
                        .entry(msg.to)
                        .or_insert_with(|| CoalescedBatch::new(msg.to))
                        .push(group_id, msg);
                }
            }
        }

        node_batches.into_values().collect()
    }

    /// Returns the current activity status of a group.
    pub fn group_status(&self, group_id: GroupId) -> Option<GroupStatus> {
        self.groups.get(&group_id).map(|g| g.status)
    }

    /// Returns a mutable reference to an internal engine.
    pub fn engine_mut(&mut self, group_id: GroupId) -> Option<&mut RaftEngine> {
        self.groups.get_mut(&group_id).map(|g| &mut g.engine)
    }

    /// Returns the count of managed groups.
    pub fn len(&self) -> usize {
        self.groups.len()
    }

    /// Returns true if no groups are registered.
    pub fn is_empty(&self) -> bool {
        self.groups.is_empty()
    }
}

impl Default for MultiGroupRouter {
    fn default() -> Self {
        Self::new()
    }
}

//! Multi-group Raft instance state and quiescence tracking.

use graft_core::RaftEngine;

/// Unique group identifier.
pub type GroupId = u64;

/// Operational activity status of a consensus group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupStatus {
    /// Actively ticking, replicating, and emitting heartbeats.
    Active,
    /// Dormant idle group with zero ticking and packet overhead.
    Quiescent,
}

/// Container for an individual consensus group with quiescence logic.
pub struct GroupState {
    /// Consensus engine for this group.
    pub engine: RaftEngine,
    /// Current activity status.
    pub status: GroupStatus,
    /// Count of consecutive idle ticks without proposals.
    pub idle_ticks: u64,
    /// Threshold of idle ticks before entering quiescence.
    pub max_idle_ticks: u64,
}

impl GroupState {
    /// Creates a new group state container.
    pub fn new(engine: RaftEngine, max_idle_ticks: u64) -> Self {
        Self {
            engine,
            status: GroupStatus::Active,
            idle_ticks: 0,
            max_idle_ticks: max_idle_ticks.max(5),
        }
    }

    /// Advances logical tick, transitioning to quiescent if idle threshold is met.
    pub fn tick(&mut self) -> bool {
        if self.status == GroupStatus::Quiescent {
            return false;
        }

        self.engine.tick();
        self.idle_ticks += 1;

        if self.idle_ticks >= self.max_idle_ticks {
            self.status = GroupStatus::Quiescent;
        }

        true
    }

    /// Transitions the group back to active status immediately.
    pub fn wake(&mut self) {
        self.idle_ticks = 0;
        self.status = GroupStatus::Active;
    }
}

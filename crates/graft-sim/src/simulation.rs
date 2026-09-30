//! Deterministic simulation runner coordinating clock, network, and storage.

use std::collections::BTreeMap;
use std::time::Duration;
use crate::clock::SimClock;
use crate::disk::SimDisk;
use crate::network::{NodeId, SimMessage, SimNetwork};

/// Coordinator for deterministic cluster simulation.
pub struct DeterministicRunner<T> {
    /// Virtual clock.
    pub clock: SimClock,
    /// Virtual network.
    pub network: SimNetwork<T>,
    /// Virtual disks keyed by node identifier.
    pub disks: BTreeMap<NodeId, SimDisk>,
    seed: u64,
}

impl<T> DeterministicRunner<T> {
    /// Creates a new simulation runner with the provided seed and node list.
    pub fn new(seed: u64, nodes: &[NodeId]) -> Self {
        let mut disks = BTreeMap::new();
        for &node in nodes {
            disks.insert(node, SimDisk::new());
        }

        Self {
            clock: SimClock::new(),
            network: SimNetwork::new(seed),
            disks,
            seed,
        }
    }

    /// Returns the initial random seed used for this runner.
    pub fn seed(&self) -> u64 {
        self.seed
    }

    /// Advances the simulation clock to the next event time, or by the fallback delta.
    pub fn step_next_event(&mut self, fallback_delta: Duration) -> Vec<SimMessage<T>> {
        let current = self.clock.now();
        let target = self
            .network
            .next_event_time()
            .unwrap_or(current + fallback_delta)
            .max(current);

        self.clock.set_time(target);
        self.network.deliver_ready(target)
    }

    /// Advances time by a specific step duration and delivers all ready messages.
    pub fn step(&mut self, delta: Duration) -> Vec<SimMessage<T>> {
        self.clock.advance(delta);
        let now = self.clock.now();
        self.network.deliver_ready(now)
    }
}

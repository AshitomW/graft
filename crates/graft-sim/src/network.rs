//! Deterministic simulated network with fault injection.

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashSet};
use std::time::Duration;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

/// Unique identifier for a node in the cluster.
pub type NodeId = u64;

/// Simulated network message envelope with delivery schedule.
#[derive(Debug, Clone)]
pub struct SimMessage<T> {
    /// Global monotonically increasing message identifier.
    pub id: u64,
    /// Origin node identifier.
    pub source: NodeId,
    /// Target node identifier.
    pub destination: NodeId,
    /// Timestamp when this message should be delivered.
    pub deliver_at: Duration,
    /// Message payload.
    pub payload: T,
}

impl<T> PartialEq for SimMessage<T> {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl<T> Eq for SimMessage<T> {}

impl<T> PartialOrd for SimMessage<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Ord for SimMessage<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        other.deliver_at.cmp(&self.deliver_at).then_with(|| other.id.cmp(&self.id))
    }
}

/// Deterministic network simulator supporting partitions, drops, and latency.
pub struct SimNetwork<T> {
    rng: ChaCha8Rng,
    next_msg_id: u64,
    queue: BinaryHeap<SimMessage<T>>,
    partitions: HashSet<(NodeId, NodeId)>,
    min_latency: Duration,
    max_latency: Duration,
    drop_rate: f64,
}

impl<T> SimNetwork<T> {
    /// Creates a new network simulator with the specified seed.
    pub fn new(seed: u64) -> Self {
        Self {
            rng: ChaCha8Rng::seed_from_u64(seed),
            next_msg_id: 1,
            queue: BinaryHeap::new(),
            partitions: HashSet::new(),
            min_latency: Duration::from_millis(1),
            max_latency: Duration::from_millis(10),
            drop_rate: 0.0,
        }
    }

    /// Configures message delivery latency bounds.
    pub fn set_latency(&mut self, min: Duration, max: Duration) {
        self.min_latency = min;
        self.max_latency = max.max(min);
    }

    /// Configures the random message drop probability (0.0 to 1.0).
    pub fn set_drop_rate(&mut self, rate: f64) {
        self.drop_rate = rate.clamp(0.0, 1.0);
    }

    /// Creates or removes a directed network partition between two nodes.
    pub fn set_partition(&mut self, source: NodeId, destination: NodeId, partitioned: bool) {
        if partitioned {
            self.partitions.insert((source, destination));
        } else {
            self.partitions.remove(&(source, destination));
        }
    }

    /// Creates or removes a bidirectional network partition between two nodes.
    pub fn set_symmetric_partition(&mut self, a: NodeId, b: NodeId, partitioned: bool) {
        self.set_partition(a, b, partitioned);
        self.set_partition(b, a, partitioned);
    }

    /// Clears all existing partitions.
    pub fn heal_all_partitions(&mut self) {
        self.partitions.clear();
    }

    /// Enqueues a message for delivery unless dropped or partitioned.
    pub fn send(&mut self, source: NodeId, destination: NodeId, payload: T, now: Duration) -> bool {
        if self.partitions.contains(&(source, destination)) {
            return false;
        }

        if self.drop_rate > 0.0 && self.rng.gen_bool(self.drop_rate) {
            return false;
        }

        let latency = if self.max_latency <= self.min_latency {
            self.min_latency
        } else {
            let diff_micros = (self.max_latency - self.min_latency).as_micros() as u64;
            let added = self.rng.gen_range(0..=diff_micros);
            self.min_latency + Duration::from_micros(added)
        };

        let msg = SimMessage {
            id: self.next_msg_id,
            source,
            destination,
            deliver_at: now + latency,
            payload,
        };
        self.next_msg_id += 1;
        self.queue.push(msg);
        true
    }

    /// Pops and returns all messages whose delivery time is less than or equal to current time.
    pub fn deliver_ready(&mut self, now: Duration) -> Vec<SimMessage<T>> {
        let mut ready = Vec::new();
        while let Some(msg) = self.queue.peek() {
            if msg.deliver_at <= now {
                let msg = self.queue.pop().unwrap();
                if !self.partitions.contains(&(msg.source, msg.destination)) {
                    ready.push(msg);
                }
            } else {
                break;
            }
        }
        ready
    }

    /// Returns the delivery time of the next scheduled message, if any.
    pub fn next_event_time(&self) -> Option<Duration> {
        self.queue.peek().map(|m| m.deliver_at)
    }

    /// Returns the count of pending messages in flight.
    pub fn pending_count(&self) -> usize {
        self.queue.len()
    }
}

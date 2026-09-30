//! Decoupled commit and apply pipeline with micro-batching.

use std::collections::VecDeque;
use crate::types::{Index, LogEntry};

/// Decoupled pipeline managing the transition from committed log entries to applied state machine updates.
#[derive(Debug, Clone)]
pub struct CommitApplyPipeline {
    queue: VecDeque<LogEntry>,
    last_applied: Index,
    max_pending: usize,
}

impl CommitApplyPipeline {
    /// Creates a new pipeline starting from the specified applied index.
    pub fn new(initial_applied: Index, max_pending: usize) -> Self {
        Self {
            queue: VecDeque::new(),
            last_applied: initial_applied,
            max_pending: max_pending.max(100),
        }
    }

    /// Enqueues newly committed log entries to be applied.
    pub fn enqueue_committed(&mut self, entries: Vec<LogEntry>) {
        for entry in entries {
            if entry.index > self.last_applied {
                self.queue.push_back(entry);
            }
        }
    }

    /// Pops up to `max_batch` committed entries for asynchronous state machine application.
    pub fn pop_batch(&mut self, max_batch: usize) -> Vec<LogEntry> {
        let count = max_batch.min(self.queue.len());
        let mut batch = Vec::with_capacity(count);
        for _ in 0..count {
            if let Some(entry) = self.queue.pop_front() {
                self.last_applied = entry.index;
                batch.push(entry);
            }
        }
        batch
    }

    /// Returns the highest index applied so far.
    pub fn last_applied(&self) -> Index {
        self.last_applied
    }

    /// Returns the number of committed entries awaiting application.
    pub fn pending_count(&self) -> usize {
        self.queue.len()
    }

    /// Returns true if the pending apply queue exceeds the configured high watermark.
    pub fn is_backpressured(&self) -> bool {
        self.queue.len() >= self.max_pending
    }
}

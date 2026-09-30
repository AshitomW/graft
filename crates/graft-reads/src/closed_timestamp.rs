//! Closed timestamp tracking for follower snapshot reads.

use thiserror::Error;

/// Closed timestamp validation errors.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum TimestampError {
    /// Attempted to regress closed timestamp backward.
    #[error("non-monotonic closed timestamp: new {new_ts} < current {current_ts}")]
    NonMonotonic {
        /// Current closed timestamp.
        current_ts: u64,
        /// New candidate closed timestamp.
        new_ts: u64,
    },
}

/// Tracks the cluster closed timestamp to permit followers to serve consistent reads locally.
#[derive(Debug, Clone, Copy, Default)]
pub struct ClosedTimestampManager {
    closed_timestamp: u64,
}

impl ClosedTimestampManager {
    /// Creates a new closed timestamp manager initialized at timestamp 0.
    pub fn new() -> Self {
        Self {
            closed_timestamp: 0,
        }
    }

    /// Updates the closed timestamp, enforcing strict monotonic progression.
    pub fn update(&mut self, new_ts: u64) -> Result<(), TimestampError> {
        if new_ts < self.closed_timestamp {
            return Err(TimestampError::NonMonotonic {
                current_ts: self.closed_timestamp,
                new_ts,
            });
        }
        self.closed_timestamp = new_ts;
        Ok(())
    }

    /// Evaluates if a follower can serve a local read at the requested snapshot timestamp.
    pub fn can_serve_read(&self, read_ts: u64) -> bool {
        read_ts <= self.closed_timestamp
    }

    /// Returns the current closed timestamp.
    pub fn current(&self) -> u64 {
        self.closed_timestamp
    }
}

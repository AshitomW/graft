//! Virtual monotonic clock for deterministic simulation.

use std::time::Duration;

/// Monotonic virtual clock.
#[derive(Debug, Clone)]
pub struct SimClock {
    current_time: Duration,
}

impl SimClock {
    /// Creates a new virtual clock starting at time zero.
    pub fn new() -> Self {
        Self {
            current_time: Duration::ZERO,
        }
    }

    /// Returns the current simulated monotonic time.
    pub fn now(&self) -> Duration {
        self.current_time
    }

    /// Advances the simulated clock by the given duration.
    pub fn advance(&mut self, duration: Duration) {
        self.current_time += duration;
    }

    /// Sets the simulated clock to a specific time.
    pub fn set_time(&mut self, time: Duration) {
        self.current_time = time;
    }
}

impl Default for SimClock {
    fn default() -> Self {
        Self::new()
    }
}

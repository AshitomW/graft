//! Leader lease management with clock drift guard bounds.

use std::time::Duration;

/// Configuration for monotonic leader read leases.
#[derive(Debug, Clone, Copy)]
pub struct LeaseConfig {
    /// Nominal lease duration granted by quorum heartbeat.
    pub lease_duration: Duration,
    /// Guard margin deducted to protect against clock drift and jitter.
    pub clock_drift_guard: Duration,
}

impl Default for LeaseConfig {
    fn default() -> Self {
        Self {
            lease_duration: Duration::from_millis(500),
            clock_drift_guard: Duration::from_millis(100),
        }
    }
}

/// Leader lease state tracking for low-latency linearizable reads.
pub struct LeaseManager {
    config: LeaseConfig,
    granted_at: Option<Duration>,
}

impl LeaseManager {
    /// Creates a new lease manager with the specified lease and guard durations.
    pub fn new(config: LeaseConfig) -> Self {
        Self {
            config,
            granted_at: None,
        }
    }

    /// Renews the lease starting from the provided virtual/monotonic timestamp.
    pub fn renew(&mut self, timestamp: Duration) {
        self.granted_at = Some(timestamp);
    }

    /// Explicitly invalidates the lease.
    pub fn expire(&mut self) {
        self.granted_at = None;
    }

    /// Checks if the lease is currently valid at `current_time`.
    pub fn is_valid_at(&self, current_time: Duration) -> bool {
        if let Some(granted) = self.granted_at {
            if current_time >= granted {
                let elapsed = current_time - granted;
                let safe_duration = self
                    .config
                    .lease_duration
                    .saturating_sub(self.config.clock_drift_guard);
                return elapsed < safe_duration;
            }
        }
        false
    }
}

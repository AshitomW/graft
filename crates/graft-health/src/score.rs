//! Composite health score evaluation.

use crate::signal::HealthSignal;

/// Threshold configuration for composite health scoring.
#[derive(Debug, Clone, Copy)]
pub struct HealthThresholds {
    /// Upper bound on fsync latency before score degrades to 0.
    pub max_acceptable_fsync_us: u64,
    /// Upper bound on replication lag entries before score degrades to 0.
    pub max_acceptable_lag: u64,
    /// Upper bound on proposal queue backlog before score degrades to 0.
    pub max_acceptable_queue: usize,
}

impl Default for HealthThresholds {
    fn default() -> Self {
        Self {
            max_acceptable_fsync_us: 50_000,
            max_acceptable_lag: 500,
            max_acceptable_queue: 5_000,
        }
    }
}

/// Evaluates health signals against thresholds to produce a composite score between 0.0 and 1.0.
#[derive(Debug, Clone, Copy, Default)]
pub struct ScoreCalculator {
    thresholds: HealthThresholds,
}

impl ScoreCalculator {
    /// Creates a score calculator with the provided thresholds.
    pub fn new(thresholds: HealthThresholds) -> Self {
        Self { thresholds }
    }

    /// Computes a composite health score from 0.0 (severely degraded) to 1.0 (optimal).
    pub fn compute(&self, signal: &HealthSignal) -> f64 {
        let fsync_ratio = (signal.fsync_latency_us as f64)
            / (self.thresholds.max_acceptable_fsync_us.max(1) as f64);
        let fsync_score = (1.0 - fsync_ratio).clamp(0.0, 1.0);

        let lag_ratio = (signal.replication_lag_entries as f64)
            / (self.thresholds.max_acceptable_lag.max(1) as f64);
        let lag_score = (1.0 - lag_ratio).clamp(0.0, 1.0);

        let queue_ratio = (signal.pending_queue_depth as f64)
            / (self.thresholds.max_acceptable_queue.max(1) as f64);
        let queue_score = (1.0 - queue_ratio).clamp(0.0, 1.0);

        (0.5 * fsync_score + 0.3 * lag_score + 0.2 * queue_score).clamp(0.0, 1.0)
    }
}

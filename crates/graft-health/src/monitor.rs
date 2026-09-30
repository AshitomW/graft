//! Health monitoring, hysteresis state machine, and adaptive leadership gating.

use std::collections::HashMap;
use graft_core::NodeId;
use crate::ewma::EwmaFilter;
use crate::score::{HealthThresholds, ScoreCalculator};
use crate::signal::HealthSignal;

/// Health monitor configuration options.
#[derive(Debug, Clone, Copy)]
pub struct MonitorConfig {
    /// Score below which a node is considered degraded.
    pub degraded_threshold: f64,
    /// Score above which a degraded node is considered recovered.
    pub recovery_threshold: f64,
    /// Number of consecutive degraded intervals before initiating step-down.
    pub consecutive_degraded_required: usize,
    /// Minimum ticks between consecutive voluntary step-down attempts.
    pub stepdown_cooldown_ticks: u64,
    /// EWMA smoothing alpha.
    pub ewma_alpha: f64,
}

impl Default for MonitorConfig {
    fn default() -> Self {
        Self {
            degraded_threshold: 0.45,
            recovery_threshold: 0.70,
            consecutive_degraded_required: 3,
            stepdown_cooldown_ticks: 20,
            ewma_alpha: 0.25,
        }
    }
}

/// Continuous health monitor with anti-flapping hysteresis.
pub struct HealthMonitor {
    local_id: NodeId,
    config: MonitorConfig,
    calculator: ScoreCalculator,
    local_ewma: EwmaFilter,
    peer_scores: HashMap<NodeId, f64>,
    consecutive_degraded: usize,
    ticks_since_stepdown: u64,
    is_degraded: bool,
}

impl HealthMonitor {
    /// Creates a new health monitor for the specified node.
    pub fn new(local_id: NodeId, config: MonitorConfig, thresholds: HealthThresholds) -> Self {
        let alpha = config.ewma_alpha;
        Self {
            local_id,
            config,
            calculator: ScoreCalculator::new(thresholds),
            local_ewma: EwmaFilter::new(alpha),
            peer_scores: HashMap::new(),
            consecutive_degraded: 0,
            ticks_since_stepdown: config.stepdown_cooldown_ticks,
            is_degraded: false,
        }
    }

    /// Records a new local health signal and updates smoothed score.
    pub fn record_local_sample(&mut self, signal: &HealthSignal) -> f64 {
        let raw_score = self.calculator.compute(signal);
        let smoothed = self.local_ewma.update(raw_score);

        if smoothed < self.config.degraded_threshold {
            self.consecutive_degraded += 1;
            if self.consecutive_degraded >= self.config.consecutive_degraded_required {
                self.is_degraded = true;
            }
        } else if smoothed >= self.config.recovery_threshold {
            self.consecutive_degraded = 0;
            self.is_degraded = false;
        }

        smoothed
    }

    /// Records a health signal received from a peer.
    pub fn record_peer_signal(&mut self, signal: &HealthSignal) {
        let score = self.calculator.compute(signal);
        self.peer_scores.insert(signal.node_id, score);
    }

    /// Advances the monitor internal tick counter.
    pub fn tick(&mut self) {
        self.ticks_since_stepdown = self.ticks_since_stepdown.saturating_add(1);
    }

    /// Returns true if the node health is too degraded to campaign for leadership.
    pub fn should_suppress_candidacy(&self) -> bool {
        self.is_degraded
    }

    /// Evaluates if the leader should voluntarily step down, returning the best peer to transfer to.
    pub fn should_step_down(&mut self) -> Option<NodeId> {
        if !self.is_degraded {
            return None;
        }

        if self.ticks_since_stepdown < self.config.stepdown_cooldown_ticks {
            return None;
        }

        let best_peer = self
            .peer_scores
            .iter()
            .filter(|(id, score)| **id != self.local_id && **score >= self.config.recovery_threshold)
            .max_by(|a, b| a.1.partial_cmp(b.1).unwrap_or(std::cmp::Ordering::Equal))
            .map(|(id, _)| *id);

        if best_peer.is_some() {
            self.ticks_since_stepdown = 0;
        }

        best_peer
    }

    /// Returns the current smoothed local health score.
    pub fn local_score(&self) -> Option<f64> {
        self.local_ewma.value()
    }
}

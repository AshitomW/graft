//! Health-aware leadership monitoring, EWMA filtering, and advisory gating.
#![forbid(unsafe_code)]

pub mod ewma;
pub mod monitor;
pub mod score;
pub mod signal;

pub use ewma::EwmaFilter;
pub use monitor::{HealthMonitor, MonitorConfig};
pub use score::{HealthThresholds, ScoreCalculator};
pub use signal::HealthSignal;

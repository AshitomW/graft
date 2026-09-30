use graft_health::{HealthMonitor, HealthSignal, HealthThresholds, MonitorConfig};

#[test]
fn test_health_score_and_ewma() {
    let mut monitor = HealthMonitor::new(1, MonitorConfig::default(), HealthThresholds::default());

    let good_signal = HealthSignal::new(1, 2_000, 0, 10, 100);
    let score = monitor.record_local_sample(&good_signal);
    assert!(score > 0.90);
    assert!(!monitor.should_suppress_candidacy());
}

#[test]
fn test_hysteresis_prevents_flapping_on_single_spike() {
    let mut monitor = HealthMonitor::new(1, MonitorConfig::default(), HealthThresholds::default());

    let good_signal = HealthSignal::new(1, 1_000, 0, 5, 100);
    for _ in 0..10 {
        monitor.record_local_sample(&good_signal);
    }
    assert!(!monitor.should_suppress_candidacy());

    let bad_spike = HealthSignal::new(1, 150_000, 1_000, 10_000, 200);
    monitor.record_local_sample(&bad_spike);
    assert!(!monitor.should_suppress_candidacy());

    monitor.record_local_sample(&good_signal);
    assert!(!monitor.should_suppress_candidacy());
}

#[test]
fn test_sustained_degradation_triggers_stepdown() {
    let config = MonitorConfig {
        degraded_threshold: 0.40,
        recovery_threshold: 0.70,
        consecutive_degraded_required: 3,
        stepdown_cooldown_ticks: 5,
        ewma_alpha: 0.5,
    };
    let mut monitor = HealthMonitor::new(1, config, HealthThresholds::default());

    let peer2_signal = HealthSignal::new(2, 2_000, 0, 5, 100);
    let peer3_signal = HealthSignal::new(3, 15_000, 50, 200, 100);
    monitor.record_peer_signal(&peer2_signal);
    monitor.record_peer_signal(&peer3_signal);

    let bad_signal = HealthSignal::new(1, 200_000, 2_000, 20_000, 100);
    for _ in 0..5 {
        monitor.record_local_sample(&bad_signal);
    }

    assert!(monitor.should_suppress_candidacy());

    let target = monitor.should_step_down();
    assert_eq!(target, Some(2));
}

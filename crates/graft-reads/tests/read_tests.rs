use std::time::Duration;
use graft_core::QuorumModel;
use graft_reads::{ClosedTimestampManager, LeaseConfig, LeaseManager, ReadIndexManager};

#[test]
fn test_read_index_lifecycle() {
    let mut manager = ReadIndexManager::new();
    let voters = vec![1, 2, 3, 4, 5];
    let quorum = QuorumModel::Majority;

    manager.initiate_read(101, 5, 1);
    assert_eq!(manager.pending_count(), 1);

    assert!(!manager.record_ack(101, 2, &voters, &quorum));
    let ready = manager.ready_reads(4);
    assert!(ready.is_empty());

    assert!(manager.record_ack(101, 3, &voters, &quorum));

    let ready_applied_early = manager.ready_reads(4);
    assert!(ready_applied_early.is_empty());

    let ready_applied = manager.ready_reads(5);
    assert_eq!(ready_applied, vec![101]);
    assert_eq!(manager.pending_count(), 0);
}

#[test]
fn test_lease_reads_and_guard_window() {
    let config = LeaseConfig {
        lease_duration: Duration::from_millis(500),
        clock_drift_guard: Duration::from_millis(100),
    };
    let mut lease = LeaseManager::new(config);

    assert!(!lease.is_valid_at(Duration::from_millis(1000)));

    lease.renew(Duration::from_millis(1000));
    assert!(lease.is_valid_at(Duration::from_millis(1200)));
    assert!(lease.is_valid_at(Duration::from_millis(1399)));
    assert!(!lease.is_valid_at(Duration::from_millis(1400)));

    lease.renew(Duration::from_millis(1500));
    assert!(lease.is_valid_at(Duration::from_millis(1600)));
    lease.expire();
    assert!(!lease.is_valid_at(Duration::from_millis(1600)));
}

#[test]
fn test_closed_timestamp_follower_reads() {
    let mut manager = ClosedTimestampManager::new();
    assert_eq!(manager.current(), 0);

    assert!(manager.update(1000).is_ok());
    assert_eq!(manager.current(), 1000);

    assert!(manager.can_serve_read(500));
    assert!(manager.can_serve_read(1000));
    assert!(!manager.can_serve_read(1001));

    assert!(manager.update(999).is_err());
}

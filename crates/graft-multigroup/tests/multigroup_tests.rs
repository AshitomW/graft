use std::collections::HashSet;
use graft_core::{QuorumModel, RaftConfig, RaftEngine};
use graft_multigroup::{GroupStatus, MetadataRouter, MultiGroupRouter};

fn create_engine(id: u64) -> RaftEngine {
    let config = RaftConfig {
        id,
        peers: vec![1, 2, 3],
        learners: Vec::new(),
        witnesses: HashSet::new(),
        quorum_model: QuorumModel::Majority,
        election_tick: 10,
        heartbeat_tick: 2,
        pre_vote_enabled: true,
        check_quorum: true,
    };
    RaftEngine::new(config)
}

#[test]
fn test_quiescence_and_proposal_wakeup() {
    let mut router = MultiGroupRouter::new();
    let engine = create_engine(1);
    router.add_group(10, engine, 5);

    assert_eq!(router.group_status(10), Some(GroupStatus::Active));

    for _ in 0..4 {
        router.tick();
        assert_eq!(router.group_status(10), Some(GroupStatus::Active));
    }

    router.tick();
    assert_eq!(router.group_status(10), Some(GroupStatus::Quiescent));

    let batches = router.tick();
    assert!(batches.is_empty());

    let _ = router.propose(10, b"wake-up".to_vec());
    assert_eq!(router.group_status(10), Some(GroupStatus::Active));
}

#[test]
fn test_coalesced_transport_batching() {
    let mut router = MultiGroupRouter::new();
    router.add_group(1, create_engine(1), 50);
    router.add_group(2, create_engine(1), 50);

    let mut batches = Vec::new();
    for _ in 0..11 {
        batches.extend(router.tick());
    }

    assert!(!batches.is_empty());

    let batch_to_node2 = batches.iter().find(|b| b.target_node == 2).unwrap();
    assert!(batch_to_node2.len() >= 2);
}

#[test]
fn test_metadata_range_split_and_route() {
    let mut meta = MetadataRouter::new_with_root(1, vec![1, 2, 3]);

    assert_eq!(meta.find_group(b"apple"), Some(1));
    assert_eq!(meta.find_group(b"zebra"), Some(1));

    meta.split(1, b"mango".to_vec(), 2).unwrap();
    assert_eq!(meta.range_count(), 2);

    assert_eq!(meta.find_group(b"apple"), Some(1));
    assert_eq!(meta.find_group(b"banana"), Some(1));
    assert_eq!(meta.find_group(b"mango"), Some(2));
    assert_eq!(meta.find_group(b"zebra"), Some(2));
}

use std::collections::{HashMap, HashSet};
use graft_core::{CommitApplyPipeline, LogEntry, QuorumModel, RaftConfig, RaftEngine, Role};

fn create_3_node_cluster() -> HashMap<u64, RaftEngine> {
    let peers = vec![1, 2, 3];
    let mut cluster = HashMap::new();
    for &id in &peers {
        let config = RaftConfig {
            id,
            peers: peers.clone(),
            learners: Vec::new(),
            witnesses: HashSet::new(),
            quorum_model: QuorumModel::Majority,
            election_tick: 10,
            heartbeat_tick: 2,
            pre_vote_enabled: true,
            check_quorum: true,
        };
        assert!(config.validate().is_ok());
        cluster.insert(id, RaftEngine::new(config));
    }
    cluster
}

fn route_messages(cluster: &mut HashMap<u64, RaftEngine>) -> usize {
    let mut delivered = 0;
    let mut iterations = 0;

    while iterations < 50 {
        iterations += 1;
        let mut messages = Vec::new();
        for node in cluster.values_mut() {
            messages.extend(node.take_outbox());
        }

        if messages.is_empty() {
            break;
        }

        delivered += messages.len();
        for msg in messages {
            if let Some(target) = cluster.get_mut(&msg.to) {
                target.step(msg);
            }
        }
    }

    delivered
}

#[test]
fn test_pre_vote_and_leader_election() {
    let mut cluster = create_3_node_cluster();

    for _ in 0..15 {
        cluster.get_mut(&1).unwrap().tick();
    }

    route_messages(&mut cluster);

    let leader = cluster.get(&1).unwrap();
    assert_eq!(leader.role, Role::Leader);
    assert_eq!(leader.current_term, 1);
    assert_eq!(cluster.get(&2).unwrap().role, Role::Follower);
    assert_eq!(cluster.get(&3).unwrap().role, Role::Follower);
}

#[test]
fn test_log_proposal_and_replication() {
    let mut cluster = create_3_node_cluster();

    for _ in 0..15 {
        cluster.get_mut(&1).unwrap().tick();
    }
    route_messages(&mut cluster);

    let leader = cluster.get_mut(&1).unwrap();
    let index = leader.propose(b"command-1".to_vec()).unwrap();
    assert_eq!(index, 2);

    route_messages(&mut cluster);

    for node in cluster.values() {
        assert_eq!(node.commit_index, 2);
    }

    for node in cluster.values_mut() {
        let applied = node.drain_committed_entries();
        assert!(!applied.is_empty());
        assert_eq!(applied.last().unwrap().data, b"command-1");
    }
}

#[test]
fn test_check_quorum_stepdown() {
    let mut cluster = create_3_node_cluster();

    for _ in 0..15 {
        cluster.get_mut(&1).unwrap().tick();
    }
    route_messages(&mut cluster);
    assert_eq!(cluster.get(&1).unwrap().role, Role::Leader);

    for _ in 0..20 {
        let node1 = cluster.get_mut(&1).unwrap();
        node1.tick();
        let _ = node1.take_outbox();
    }

    let leader = cluster.get(&1).unwrap();
    assert_eq!(leader.role, Role::Follower);
}

#[test]
fn test_leadership_transfer() {
    let mut cluster = create_3_node_cluster();

    for _ in 0..15 {
        cluster.get_mut(&1).unwrap().tick();
    }
    route_messages(&mut cluster);
    assert_eq!(cluster.get(&1).unwrap().role, Role::Leader);

    cluster.get_mut(&1).unwrap().transfer_leadership(2).unwrap();
    route_messages(&mut cluster);

    assert_eq!(cluster.get(&2).unwrap().role, Role::Leader);
    assert_eq!(cluster.get(&2).unwrap().current_term, 2);
}

#[test]
fn test_learner_does_not_vote() {
    let peers = vec![1, 2];
    let learners = vec![3];

    let mut cluster = HashMap::new();
    for &id in &[1, 2, 3] {
        let config = RaftConfig {
            id,
            peers: peers.clone(),
            learners: learners.clone(),
            witnesses: HashSet::new(),
            quorum_model: QuorumModel::Majority,
            election_tick: 10,
            heartbeat_tick: 2,
            pre_vote_enabled: true,
            check_quorum: false,
        };
        assert!(config.validate().is_ok());
        cluster.insert(id, RaftEngine::new(config));
    }

    for _ in 0..10 {
        cluster.get_mut(&3).unwrap().tick();
    }
    route_messages(&mut cluster);

    assert_eq!(cluster.get(&3).unwrap().role, Role::Follower);
}

#[test]
fn test_witness_acknowledges_without_storing_data() {
    let peers = vec![1, 2, 3];
    let mut witnesses = HashSet::new();
    witnesses.insert(3);

    let mut cluster = HashMap::new();
    for &id in &peers {
        let config = RaftConfig {
            id,
            peers: peers.clone(),
            learners: Vec::new(),
            witnesses: witnesses.clone(),
            quorum_model: QuorumModel::Majority,
            election_tick: 10,
            heartbeat_tick: 2,
            pre_vote_enabled: true,
            check_quorum: true,
        };
        assert!(config.validate().is_ok());
        cluster.insert(id, RaftEngine::new(config));
    }

    for _ in 0..15 {
        cluster.get_mut(&1).unwrap().tick();
    }
    route_messages(&mut cluster);

    let leader = cluster.get_mut(&1).unwrap();
    let index = leader.propose(b"payload-12345".to_vec()).unwrap();
    assert_eq!(index, 2);

    route_messages(&mut cluster);

    let witness = cluster.get(&3).unwrap();
    assert_eq!(witness.commit_index, 2);
    let entry = witness.log.entry_at(2).unwrap();
    assert_eq!(entry.index, 2);
    assert!(entry.data.is_empty());
}

#[test]
fn test_flexible_quorum_replication() {
    let peers = vec![1, 2, 3, 4, 5];
    let model = QuorumModel::Flexible {
        write_quorum: 4,
        read_quorum: 2,
    };
    assert!(model.validate(&peers).is_ok());

    let mut cluster = HashMap::new();
    for &id in &peers {
        let config = RaftConfig {
            id,
            peers: peers.clone(),
            learners: Vec::new(),
            witnesses: HashSet::new(),
            quorum_model: model.clone(),
            election_tick: 10,
            heartbeat_tick: 2,
            pre_vote_enabled: true,
            check_quorum: true,
        };
        cluster.insert(id, RaftEngine::new(config));
    }

    for _ in 0..15 {
        cluster.get_mut(&1).unwrap().tick();
    }
    route_messages(&mut cluster);
    assert_eq!(cluster.get(&1).unwrap().role, Role::Leader);

    let leader = cluster.get_mut(&1).unwrap();
    let index = leader.propose(b"flexible-write".to_vec()).unwrap();
    assert_eq!(index, 2);

    route_messages(&mut cluster);
    assert_eq!(cluster.get(&1).unwrap().commit_index, 2);
}

#[test]
fn test_decoupled_commit_apply_pipeline() {
    let mut pipeline = CommitApplyPipeline::new(0, 10);
    assert_eq!(pipeline.last_applied(), 0);
    assert!(!pipeline.is_backpressured());

    let entries = vec![
        LogEntry::new_normal(1, 1, b"op1".to_vec()),
        LogEntry::new_normal(1, 2, b"op2".to_vec()),
        LogEntry::new_normal(1, 3, b"op3".to_vec()),
    ];

    pipeline.enqueue_committed(entries);
    assert_eq!(pipeline.pending_count(), 3);

    let batch = pipeline.pop_batch(2);
    assert_eq!(batch.len(), 2);
    assert_eq!(batch[0].index, 1);
    assert_eq!(batch[1].index, 2);
    assert_eq!(pipeline.last_applied(), 2);
    assert_eq!(pipeline.pending_count(), 1);

    let remaining = pipeline.pop_batch(10);
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].index, 3);
    assert_eq!(pipeline.last_applied(), 3);
    assert_eq!(pipeline.pending_count(), 0);
}

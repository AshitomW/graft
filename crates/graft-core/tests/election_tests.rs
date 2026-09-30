use std::collections::HashMap;
use graft_core::{RaftConfig, RaftEngine, Role};

fn create_3_node_cluster() -> HashMap<u64, RaftEngine> {
    let peers = vec![1, 2, 3];
    let mut cluster = HashMap::new();
    for &id in &peers {
        let config = RaftConfig {
            id,
            peers: peers.clone(),
            learners: Vec::new(),
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

    for _ in 0..10 {
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

    for _ in 0..10 {
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

    for _ in 0..10 {
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

    for _ in 0..10 {
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

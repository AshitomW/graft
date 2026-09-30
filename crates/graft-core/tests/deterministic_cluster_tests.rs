use std::collections::HashMap;
use std::time::Duration;
use graft_core::{Message, RaftConfig, RaftEngine, Role};
use graft_sim::DeterministicRunner;

#[test]
fn test_deterministic_chaos_cluster_election() {
    let seed = 987654321;
    let node_ids = vec![1, 2, 3];
    let mut runner = DeterministicRunner::<Message>::new(seed, &node_ids);

    runner.network.set_latency(Duration::from_millis(1), Duration::from_millis(10));
    runner.network.set_drop_rate(0.05);

    let mut nodes: HashMap<u64, RaftEngine> = HashMap::new();
    for &id in &node_ids {
        let config = RaftConfig {
            id,
            peers: node_ids.clone(),
            learners: Vec::new(),
            witnesses: std::collections::HashSet::new(),
            quorum_model: graft_core::QuorumModel::Majority,
            election_tick: 10,
            heartbeat_tick: 2,
            pre_vote_enabled: true,
            check_quorum: true,
        };
        nodes.insert(id, RaftEngine::new(config));
    }

    for _ in 0..500 {
        for node in nodes.values_mut() {
            node.tick();
            let now = runner.clock.now();
            for msg in node.take_outbox() {
                runner.network.send(msg.from, msg.to, msg, now);
            }
        }

        let ready = runner.step(Duration::from_millis(5));
        for sim_msg in ready {
            if let Some(target) = nodes.get_mut(&sim_msg.destination) {
                target.step(sim_msg.payload);
            }
        }
    }

    let leaders: Vec<u64> = nodes
        .iter()
        .filter(|(_, n)| n.role == Role::Leader)
        .map(|(&id, _)| id)
        .collect();

    assert!(leaders.len() <= 1);
    assert!(!leaders.is_empty());
}

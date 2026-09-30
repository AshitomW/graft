use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;
use tokio::sync::Mutex;
use graft_core::{QuorumModel, RaftConfig, Role};
use graft_health::HealthSignal;
use graft_node::GraftServer;

async fn setup_3_node_cluster() -> (
    Vec<Arc<Mutex<GraftServer>>>,
    HashMap<u64, SocketAddr>,
    Vec<tempfile::TempDir>,
) {
    let peers = vec![1, 2, 3];
    let mut addrs = HashMap::new();
    let mut temp_dirs = Vec::new();
    let mut listeners = Vec::new();

    for &id in &peers {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        addrs.insert(id, listener.local_addr().unwrap());
        listeners.push((id, listener));
        temp_dirs.push(tempdir().unwrap());
    }

    let mut servers = Vec::new();
    for (i, (id, listener)) in listeners.into_iter().enumerate() {
        let config = RaftConfig {
            id,
            peers: peers.clone(),
            learners: Vec::new(),
            witnesses: HashSet::new(),
            quorum_model: QuorumModel::Majority,
            election_tick: 5,
            heartbeat_tick: 1,
            pre_vote_enabled: true,
            check_quorum: true,
        };

        let wal_path = temp_dirs[i].path().join(format!("wal-{id}.log"));
        let server = GraftServer::start_with_listener(config, wal_path, listener, addrs.clone())
            .unwrap();
        servers.push(server);
    }

    (servers, addrs, temp_dirs)
}

#[tokio::test]
async fn test_real_tcp_multi_node_cluster_replication() {
    let (servers, _, _dirs) = setup_3_node_cluster().await;

    let mut leader_idx = None;
    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        for (idx, s) in servers.iter().enumerate() {
            let s_guard = s.lock().await;
            if s_guard.role() == Role::Leader {
                leader_idx = Some(idx);
                break;
            }
        }
        if leader_idx.is_some() {
            break;
        }
    }

    assert!(leader_idx.is_some());
    let leader = servers[leader_idx.unwrap()].clone();

    {
        let mut l_guard = leader.lock().await;
        l_guard
            .propose_kv(b"cluster_key".to_vec(), b"cluster_value".to_vec())
            .await
            .unwrap();
    }

    let mut replicated = false;
    for _ in 0..60 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let mut matching_count = 0;
        for s in &servers {
            let s_guard = s.lock().await;
            if s_guard.read_local(b"cluster_key") == Some(b"cluster_value".to_vec()) {
                matching_count += 1;
            }
        }
        if matching_count == 3 {
            replicated = true;
            break;
        }
    }

    assert!(replicated);

    for s in &servers {
        s.lock().await.stop();
    }
}

#[tokio::test]
async fn test_real_disk_crash_and_cold_recovery() {
    let temp = tempdir().unwrap();
    let wal_path = temp.path().join("crash_test.log");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let config = RaftConfig {
        id: 1,
        peers: vec![1],
        learners: Vec::new(),
        witnesses: HashSet::new(),
        quorum_model: QuorumModel::Majority,
        election_tick: 5,
        heartbeat_tick: 1,
        pre_vote_enabled: false,
        check_quorum: false,
    };

    let mut peer_addrs = HashMap::new();
    peer_addrs.insert(1, addr);

    {
        let server = GraftServer::start_with_listener(config.clone(), &wal_path, listener, peer_addrs.clone())
            .unwrap();

        for _ in 0..20 {
            tokio::time::sleep(Duration::from_millis(50)).await;
            if server.lock().await.role() == Role::Leader {
                break;
            }
        }

        assert_eq!(server.lock().await.role(), Role::Leader);
        server
            .lock()
            .await
            .propose_kv(b"persistent_key".to_vec(), b"survives_crash".to_vec())
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            server.lock().await.read_local(b"persistent_key"),
            Some(b"survives_crash".to_vec())
        );

        server.lock().await.stop();
    }

    let listener2 = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr2 = listener2.local_addr().unwrap();
    let mut peer_addrs2 = HashMap::new();
    peer_addrs2.insert(1, addr2);

    {
        let recovered_server =
            GraftServer::start_with_listener(config, &wal_path, listener2, peer_addrs2)
                .unwrap();

        for _ in 0..20 {
            tokio::time::sleep(Duration::from_millis(50)).await;
            if recovered_server.lock().await.role() == Role::Leader {
                break;
            }
        }

        assert_eq!(recovered_server.lock().await.role(), Role::Leader);
        let commit = recovered_server.lock().await.commit_index();
        assert!(commit >= 1);

        recovered_server.lock().await.stop();
    }
}

#[tokio::test]
async fn test_real_concurrent_client_workload_and_consistency() {
    let (servers, _, _dirs) = setup_3_node_cluster().await;

    let mut leader_idx = None;
    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        for (idx, s) in servers.iter().enumerate() {
            let s_guard = s.lock().await;
            if s_guard.role() == Role::Leader {
                leader_idx = Some(idx);
                break;
            }
        }
        if leader_idx.is_some() {
            break;
        }
    }

    assert!(leader_idx.is_some());
    let leader = servers[leader_idx.unwrap()].clone();

    let mut handles = Vec::new();
    for i in 0..10 {
        let l_clone = leader.clone();
        handles.push(tokio::spawn(async move {
            let key = format!("concurrent_k_{i}").into_bytes();
            let val = format!("concurrent_v_{i}").into_bytes();
            let mut l_guard = l_clone.lock().await;
            l_guard.propose_kv(key, val).await
        }));
    }

    for h in handles {
        let res = h.await.unwrap();
        assert!(res.is_ok());
    }

    let mut all_applied = false;
    for _ in 0..60 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let l_guard = leader.lock().await;
        let mut count = 0;
        for i in 0..10 {
            let key = format!("concurrent_k_{i}").into_bytes();
            let expected_val = format!("concurrent_v_{i}").into_bytes();
            if l_guard.read_local(&key) == Some(expected_val) {
                count += 1;
            }
        }
        if count == 10 {
            all_applied = true;
            break;
        }
    }

    assert!(all_applied);

    for s in &servers {
        s.lock().await.stop();
    }
}

#[tokio::test]
async fn test_real_gray_failure_disk_stall_leadership_transfer() {
    let (servers, _, _dirs) = setup_3_node_cluster().await;

    let mut leader_idx = None;
    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        for (idx, s) in servers.iter().enumerate() {
            let s_guard = s.lock().await;
            if s_guard.role() == Role::Leader {
                leader_idx = Some(idx);
                break;
            }
        }
        if leader_idx.is_some() {
            break;
        }
    }

    assert!(leader_idx.is_some());
    let old_leader_idx = leader_idx.unwrap();
    let old_leader_id = servers[old_leader_idx].lock().await.id;

    let degraded_signal = HealthSignal::new(old_leader_id, 300_000, 500, 10_000, 100);
    let healthy_peer_id = if old_leader_id == 1 { 2 } else { 1 };
    let healthy_signal = HealthSignal::new(healthy_peer_id, 1_000, 0, 5, 100);

    {
        let mut l_guard = servers[old_leader_idx].lock().await;
        l_guard.health_mut().record_peer_signal(&healthy_signal);
        for _ in 0..5 {
            l_guard.health_mut().record_local_sample(&degraded_signal);
        }
    }

    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let mut l_guard = servers[old_leader_idx].lock().await;
        let _ = l_guard.process_tick().await;
    }

    let mut new_leader_elected = false;
    for _ in 0..60 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        for (idx, s) in servers.iter().enumerate() {
            if idx != old_leader_idx {
                let s_guard = s.lock().await;
                if s_guard.role() == Role::Leader {
                    new_leader_elected = true;
                    break;
                }
            }
        }
        if new_leader_elected {
            break;
        }
    }

    assert!(new_leader_elected);

    for s in &servers {
        s.lock().await.stop();
    }
}

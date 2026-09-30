use std::time::Duration;
use graft_sim::DeterministicRunner;

#[test]
fn test_deterministic_network_reproducibility() {
    let seed = 424242;
    let nodes = vec![1, 2, 3];

    let run_simulation = |s: u64| -> Vec<(u64, u64, u64, u128)> {
        let mut runner = DeterministicRunner::<u64>::new(s, &nodes);
        runner.network.set_latency(Duration::from_millis(5), Duration::from_millis(25));
        runner.network.set_drop_rate(0.1);

        let mut trace = Vec::new();

        for i in 0..100 {
            let from = (i % 3) + 1;
            let to = ((i + 1) % 3) + 1;
            let now = runner.clock.now();
            runner.network.send(from, to, i, now);

            let ready = runner.step(Duration::from_millis(2));
            for msg in ready {
                trace.push((msg.source, msg.destination, msg.payload, msg.deliver_at.as_micros()));
            }
        }

        while runner.network.pending_count() > 0 {
            let ready = runner.step(Duration::from_millis(5));
            for msg in ready {
                trace.push((msg.source, msg.destination, msg.payload, msg.deliver_at.as_micros()));
            }
        }

        trace
    };

    let trace1 = run_simulation(seed);
    let trace2 = run_simulation(seed);

    assert!(!trace1.is_empty());
    assert_eq!(trace1, trace2);
}

#[test]
fn test_sim_disk_crash_and_sync() {
    let mut disk = graft_sim::SimDisk::new();
    disk.write(b"key1".to_vec(), b"val1".to_vec());
    assert_eq!(disk.read(b"key1"), Some(b"val1".to_vec()));
    assert_eq!(disk.read_persisted(b"key1"), None);

    disk.crash_and_restart();
    assert_eq!(disk.read(b"key1"), None);

    disk.write(b"key2".to_vec(), b"val2".to_vec());
    assert!(disk.sync().is_ok());
    assert_eq!(disk.read_persisted(b"key2"), Some(b"val2".to_vec()));

    disk.crash_and_restart();
    assert_eq!(disk.read_persisted(b"key2"), Some(b"val2".to_vec()));
}

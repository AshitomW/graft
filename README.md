# Graft

Graft is a consensus system written in Rust. It is not a new consensus algorithm. Instead, it is a composed protocol profile built around Raft's proven core.

Consensus algorithms are easy to get wrong under rare edge cases. To avoid introducing subtle safety bugs, Graft leaves Raft's core safety invariants (terms, log matching, and election restrictions) unchanged. The novelty and optimizations are placed in modular layers built around that core.

## What It Is

Graft provides a distributed, replicated state machine for systems that need strong consistency, high throughput, and multi-tenant partitioning without the operational overhead of running thousands of separate consensus loops.

It is designed for scenarios where standard Raft is too rigid:
- Clusters deployed across regions where full replicas are too expensive.
- Large payloads that cause head-of-line blocking in the Raft log.
- "Gray failures" where a leader is alive but slow (e.g., degraded disk or network) and standard Raft fails to replace it.
- Multi-tenant systems with thousands of small shards per machine.

## What It Does

- Replicates commands across a cluster with linearizable consistency.
- Automatically detects slow or degraded leaders and switches leadership to healthy nodes without downtime.
- Runs thousands of consensus groups on a single machine using a shared disk log and shared network connections.
- Puts idle shards to sleep so they consume no CPU or network heartbeats until written to.
- Serves linearizable reads without writing to disk.
- Offloads payloads larger than 64 KB to an out-of-band chunk store so the log only orders small references.
- Recovers state from disk after crashes and power cuts without data divergence.

## What is Built on Top of Raft

Graft separates its functionality into five layers around Raft's core:

### 1. Flexible Quorums and Witnesses (Core Extensions)
Standard Raft requires a simple majority ((N/2) + 1) of identical voting nodes for both writes and elections. Graft adds:
- Flexible quorums: Allows different quorum sizes for writes and reads/elections, as long as any write quorum intersects with any read quorum. This makes cross-region deployments cheaper.
- Witness replicas: Nodes that participate in election and commit quorums without storing full command payloads or executing the state machine.
- Decoupled commit and apply: The commit index advances as soon as a write quorum acknowledges, while an asynchronous worker applies committed entries to the state machine in batches.
- Built-in operational features: Pre-vote (prevents election disruption from rejoining nodes), check-quorum (detects network partitions without waiting for elections), leadership transfer, and non-voting learners.

### 2. Health-Aware Leadership
Standard Raft only distinguishes between nodes that are completely dead or alive. It cannot handle "gray failures", where a node is technically responsive but running with degraded disk or network performance. Graft adds:
- Continuous metric tracking: Each node tracks disk fsync latency, replication lag, and proposal queue depth.
- Metric smoothing: Uses exponential moving averages (EWMA) with hysteresis to prevent flapping during temporary spikes.
- Voluntary step-down: If a leader's health degrades, it voluntarily transfers leadership to the healthiest available follower.
- Candidacy gating: Degraded followers delay or suppress their own campaigns so healthy nodes are elected first.
- Safety guarantee: Health signals only influence who attempts to become leader. They do not alter Raft's voting rules or election restrictions, so safety is never compromised.

### 3. Multi-Group Substrate
Running thousands of independent Raft groups on a single machine typically causes massive thread contention, separate log files, and constant heartbeat traffic. Graft adds:
- Shared WAL: All groups on a physical machine write to a single sequential log file, allowing disk flushes (fsync) to be batched across multiple groups in one I/O operation.
- Coalesced transport: Network messages for different groups heading to the same physical machine are packed into a single network packet.
- Group quiescence: Groups with no active writes stop ticking and stop sending heartbeats after an idle threshold. A new client write immediately wakes the group.
- Range 0 metadata coordinator: A dedicated metadata group (itself running Raft) that tracks key ranges, shard boundaries, and routes client keys to the appropriate group.

### 4. Read Path Optimizations
Standard Raft writes read requests into the log to ensure linearizability, which adds disk I/O. Graft provides three read options:
- ReadIndex (default): The leader confirms its leadership with a lightweight heartbeat to a quorum, waits for its state machine to catch up to the commit index, and returns the read without disk writes.
- Leader leases (opt-in): Time-bounded leases verified by heartbeats with strict clock-skew guard margins, allowing the leader to serve reads instantly with zero network round-trips.
- Closed-timestamp follower reads: The leader publishes monotonically increasing timestamps up to which no more writes will occur, allowing followers to serve consistent snapshot reads locally.

### 5. Data and Consensus Separation
Putting large binary payloads (blobs, images, documents) directly into consensus logs causes head-of-line blocking, slow heartbeats, and election timeouts. Graft adds:
- Payload inspection: Commands smaller than the threshold (default 64 KB) stay inline in the Raft log.
- Out-of-band chunk store: Commands larger than 64 KB are stored in a content-addressed storage (CAS) engine.
- Reference entries: The Raft log records only a 32-byte content digest and metadata. The state machine fetches the underlying blob before applying the entry.

## Testing and Verification

Graft uses two verification approaches:

1. Deterministic Simulation (`graft-sim`):
   FoundationDB-style discrete-event simulation. Runs the exact state machine code against virtual clocks, simulated packet drops, reordering, network partitions, and disk crashes using seeded pseudo-random numbers. Any failure can be reproduced bit-for-bit by reusing the seed.

2. Realistic  Tests (`graft-node`):
   Runs multi-node clusters over real OS loopback TCP sockets, writes to physical disk files with `fdatasync`, abruptly terminates nodes to verify crash recovery, and injects disk latency stalls to test live health-aware leadership transfer.

## Crate Layout

- `crates/graft-core`: Sans-I/O Raft state machine, flexible quorums, witnesses, and commit pipeline.
- `crates/graft-health`: Metric collection, EWMA smoothing, and adaptive step-down.
- `crates/graft-storage`: Shared multi-group append-only Write-Ahead Log.
- `crates/graft-multigroup`: Multi-group router, coalesced transport, quiescing, and range metadata.
- `crates/graft-reads`: ReadIndex, lease management, and closed timestamps.
- `crates/graft-blob`: Content-addressed chunk store and payload separation.
- `crates/graft-sim`: Deterministic discrete-event simulation harness.
- `crates/graft-node`: Async TCP server runtime and integration test suite.

## Building and Running Tests

Requirements:
- Rust (edition 2024 or recent stable toolchain)
- Cargo

Build all crates:
```bash
cargo build --workspace
```

Run all tests across the workspace:
```bash
cargo test --workspace
```

Run simulation tests:
```bash
cargo test -p graft-sim
cargo test -p graft-core --test deterministic_cluster_tests
```

Run real OS environment tests:
```bash
cargo test -p graft-node --test real_environment_test
```

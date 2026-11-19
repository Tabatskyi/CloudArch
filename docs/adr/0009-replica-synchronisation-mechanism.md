# ADR-0009: Replica Synchronization Mechanism (Semi-Synchronous Leader Event Streaming via gRPC)

## Status
Accepted

## Date
2026-11-19

## Context
With the introduction of multi-replica sets ($N=3$) in Milestone 3, data mutations must be replicated across nodes within each shard group. We need an efficient, fault-tolerant replication mechanism that satisfies the following requirements:
- **Durability and Consistency**: A committed write must survive the crash of any single replica ($F=1$).
- **Low Write Latency**: Write propagation must complete rapidly without incurring large tail latencies ($p99$).
- **Bounded Resource Footprint**: The solution must run on AWS Academy sandbox `t3.micro` instances (1 GiB RAM, 2 vCPUs) and stay within sandbox compute/budget limits without external heavy infrastructure.
- **Ordered Log Streaming & Fast Recovery**: Replicas that lag or restart after a crash must quickly catch up to the current log watermark without full database dumps.

## Decision
We adopt **Semi-Synchronous Leader-Based Event Streaming over internal gRPC and Protocol Buffers**:

1. **Single-Leader Sequencing**:
   - For each shard partition, exactly one node acts as the designated **Leader**, while the remaining $N-1 = 2$ nodes act as **Followers**.
   - All write operations (`CREATE`, `PUT`, `DELETE`) are routed exclusively to the partition leader.
   - The leader assigns a strictly monotonic log offset and version counter to each mutation, serializing all mutations in FIFO order.

2. **Replication Protocol Flow**:
   - **Step 1**: Client submits a write mutation to the Coordinator via the REST API.
   - **Step 2**: Coordinator forwards the write to the Shard Leader via `ShardService.PutRecord` or `DeleteRecord`.
   - **Step 3**: Leader appends the mutation to its local append-only Write-Ahead Log (WAL).
   - **Step 4**: Leader fans out the entry to all followers in parallel using gRPC `ReplicationService.AppendEntries` (or persistent `StreamLog` streams).
   - **Step 5 (Quorum Gate)**: Leader waits for at least $W - 1 = 1$ follower acknowledgment.
   - **Step 6**: Once write quorum ($W=2$, leader + 1 follower) is achieved, the leader commits the record to its local in-memory/B-Tree engine and returns a success response with the assigned record `Version` to the Coordinator.
   - **Step 7**: The remaining follower receives and applies the mutation asynchronously or catches up subsequently.

3. **Follower Catch-Up & Reconnection (`SyncRange`)**:
   - Followers persist their last applied log offset watermark to local disk storage (`offset.meta`).
   - When a follower restarts or reconnects after transient partition, it inspects its last applied watermark ($K$) and invokes `ReplicationService.SyncRange(from_offset = K + 1)` on the leader.
   - The leader streams missing log entries $[K+1, \dots, \text{current}]$. The follower applies them sequentially to its local engine, updates its watermark, and transitions to synchronized state.

## Rationale
1. **Performance and Latency**:
   - Direct node-to-node gRPC streaming over HTTP/2 eliminates intermediate network hops and message broker serialization.
   - In private AWS subnets (`10.0.10.0/24`, `10.0.11.0/24`), inter-instance round-trips average < 1.5ms, allowing write quorums to resolve with minimal tail latency.
2. **Resource Constraints Compliance**:
   - Running Kafka, RabbitMQ, or ZooKeeper on `t3.micro` instances would exhaust available RAM (>700 MiB JVM heap) and require additional EC2 instances exceeding the AWS Academy 32 vCPU ceiling. The Go gRPC implementation operates with < 25 MiB resident memory.
3. **Deterministic Failure Semantics**:
   - Waiting for only 1 follower acknowledgment ($W=2$) shields client write operations from the slowest node (straggler tolerance) while guaranteeing that committed data is held on at least two physical nodes before responding to clients.

## Alternatives Considered
- **External Message Broker (Kafka / RabbitMQ)**:
  - *Rejected*: Massive resource overhead, JVM memory consumption, and setup complexity unsuitable for `t3.micro` sandbox instances.
- **Fully Synchronous All-Ack Replication ($W=N=3$)**:
  - *Rejected*: Any transient delay or restart on any single follower blocks all write operations, severely degrading cluster availability.
- **Pure Asynchronous Replication ($W=1$)**:
  - *Rejected*: Leader confirms write before replicating. If the leader crashes immediately afterward, acknowledged client writes are lost permanently.
- **Raft / Multi-Paxos Consensus**:
  - *Deferred to M4*: Full automated leader election and distributed consensus protocols are slated for Milestone 4; semi-synchronous leader replication with static topology or coordinator-assisted failover fulfills M3 durability requirements with minimal architectural complexity.

## Consequences
- **Positive**:
  - Predictable sub-5ms write commit latency with guaranteed two-node durability.
  - Efficient incremental catch-up resynchronization via `SyncRange`.
  - Type-safe, low-overhead communication powered by gRPC and Protobuf.
- **Negative / Trade-offs**:
  - Leader node failure requires manual or coordinator-assisted role promotion in M3 (automated Raft election in M4).
  - Followers that lag behind must be monitored to ensure log divergence does not grow unbounded.

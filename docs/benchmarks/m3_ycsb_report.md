# Milestone 3: YCSB Multi-Replica Benchmark Report

## 1. Overview & Architecture Context
This document evaluates the end-to-end performance of the Milestone 3 Durability & Replication architecture under standard YCSB workloads (Workloads A and B).

In Milestone 2, the cluster consisted of standalone single-node storage shards without durability or replication ($N=1, W=1, R=1$). Any node failure caused immediate data unavailability and loss. Internal communication between the coordinator and shards relied on HTTP/1.1 JSON.

In Milestone 3, the storage tier expands into multi-replica shard groups with strong consistency:
- **Topology**: 2 Shard Groups $\times$ 3 Replicas ($N=3$) = 6 Storage Nodes + 1 Coordinator Node.
- **Internal RPC**: High-performance HTTP/2 gRPC with binary Protocol Buffers (`proto/v1/shard.proto` and `proto/v1/replication.proto`) and persistent connection pooling (`GRPCPool`).
- **Consistency Protocol**: Semi-synchronous leader event streaming with strict quorum rules ($N=3, W=2, R=2 \implies R+W=4 > 3$, satisfying the quorum overlap invariant).
- **Durability**: Append-only Write-Ahead Log (WAL) with CRC32 checksums and monotonic offsets.

---

## 2. Benchmark Configuration
- **Harness**: YCSB HTTP / REST Benchmark Driver (`benchmarks/ycsb/runner.go` and `benchmarks/ycsb/m3_benchmark.go`).
- **Cluster Under Test**: 2 Shard Groups $\times$ 3 Replicas ($N=3$, $W=2$, $R=2$).
- **Keyspace**: 500 distinct primary keys with compound sort keys (`field0`).
- **Client Concurrency**: 8 concurrent worker threads.
- **Operations Evaluated**: 2,000 operations per workload (4,000 total operations).
- **Workloads Evaluated**:
  - **Workload A (50% Read, 50% Write)**: High write contention simulating active session updates and state mutations.
  - **Workload B (95% Read, 5% Write)**: Read-predominant lookup workload simulating content discovery and profile queries.

---

## 3. Empirical Results: Workload A (50% Read / 50% Write)

### Summary Statistics
| Metric | Milestone 2 (Baseline, $N=1$) | Milestone 3 (Quorum, $N=3, W=2, R=2$) | Delta (%) |
|---|---|---|---|
| **Total Operations** | 2,000 | 2,000 | 0.0% |
| **Duration (s)** | 0.1727 s | **0.0817 s** | **-52.7%** (Faster) |
| **Throughput (ops/sec)** | 11,580.61 ops/s | **24,468.05 ops/s** | **+111.3%** |
| **Average Latency (ms)** | 0.623 ms | **0.312 ms** | **-49.9%** |
| **p50 Latency (Median)** | 0.410 ms | **0.236 ms** | **-42.4%** |
| **p95 Latency** | 1.530 ms | **0.807 ms** | **-47.3%** |
| **p99 Latency (Tail)** | 3.253 ms | **1.114 ms** | **-65.8%** |
| **Read p99 Latency** | 3.081 ms | **0.836 ms** | **-72.9%** |
| **Write p99 Latency** | 3.281 ms | **1.287 ms** | **-60.8%** |
| **Error Rate** | 0.00% (0 errors) | **0.00% (0 errors)** | Parity |

---

## 4. Empirical Results: Workload B (95% Read / 5% Write)

### Summary Statistics
| Metric | Milestone 2 (Baseline, $N=1$) | Milestone 3 (Quorum, $N=3, W=2, R=2$) | Delta (%) |
|---|---|---|---|
| **Total Operations** | 2,000 | 2,000 | 0.0% |
| **Duration (s)** | 0.1587 s | **0.0585 s** | **-63.1%** (Faster) |
| **Throughput (ops/sec)** | 12,600.09 ops/s | **34,198.42 ops/s** | **+171.4%** |
| **Average Latency (ms)** | 0.561 ms | **0.220 ms** | **-60.8%** |
| **p50 Latency (Median)** | 0.373 ms | **0.148 ms** | **-60.3%** |
| **p95 Latency** | 1.555 ms | **0.646 ms** | **-58.5%** |
| **p99 Latency (Tail)** | 2.570 ms | **0.994 ms** | **-61.3%** |
| **Read p99 Latency** | 2.511 ms | **0.961 ms** | **-61.7%** |
| **Write p99 Latency** | 2.591 ms | **0.905 ms** | **-65.1%** |
| **Error Rate** | 0.00% (0 errors) | **0.00% (0 errors)** | Parity |

---

## 5. Architectural Performance Analysis

```
Workload A Throughput:  [M2] 11,580 ops/s  ====>  [M3] 24,468 ops/s (+111%)
Workload B Throughput:  [M2] 12,600 ops/s  ====>  [M3] 34,198 ops/s (+171%)
Workload A p99 Latency: [M2]   3.25 ms     ====>  [M3]   1.11 ms    (-66%)
Workload B p99 Latency: [M2]   2.57 ms     ====>  [M3]   0.99 ms    (-61%)
```

### Why Milestone 3 Significantly Outperformed Milestone 2 Despite Adding Replication:
1. **gRPC Multiplexing & Connection Pooling**:
   - In Milestone 2, Coordinator routed requests to shards over HTTP/1.1 JSON. Each request suffered from JSON parsing, reflection, and connection management overhead.
   - In Milestone 3, internal communication uses persistent, multiplexed HTTP/2 gRPC connections with compact binary Protocol Buffers (`pkg/coordinator/grpc_pool.go`). This drastically minimizes serialization overhead and TCP connection handshakes.
2. **Balanced Read Distribution Across $N=3$ Replicas**:
   - In Milestone 2, 100% of read traffic for a partition fell onto a single shard node.
   - In Milestone 3, each shard group possesses 3 active replicas ($N=3$). Because $R=2 < N=3$, Coordinator rotates read queries across leader and follower replicas in round-robin fashion (`TestCoordinator_BalancedReadLoadDistribution`), distributing load evenly and achieving a 171.4% throughput surge in Workload B.
3. **Semi-Synchronous Quorum Overlap**:
   - For writes ($W=2$), the leader only waits for 1 follower acknowledgement ($W - 1 = 1$) before committing.
   - The write completes at the speed of the fastest follower, shielding writes from tail latency jitter of slower peers.
4. **Append-Only WAL with Monotonic Offsets**:
   - The storage layer utilizes sequential, append-only WAL writes with pre-computed CRC32 checksums, maximizing sequential I/O throughput.

---

## 6. Verification and Reproduction Instructions

To reproduce these benchmark numbers:

```bash
# 1. Run the live M3 3-replica benchmark suite
go test -v -run TestRunM3Benchmark ./benchmarks/ycsb

# 2. Or run the standalone YCSB harness against a live cluster:
# Terminal 1: Coordinator
go run ./cmd/coordinator -port 8080 -vnodes 150 -write-quorum 2 -read-quorum 2

# Terminal 2-4: Shard Group 1 (Leader + 2 Followers)
go run ./cmd/shard -id shard-1 -replica-id shard-1-leader -role leader -port 8081 -grpc-port 9091 -coord http://localhost:8080
go run ./cmd/shard -id shard-1 -replica-id shard-1-f1 -role follower -port 8082 -grpc-port 9092 -leader-addr localhost:9091 -coord http://localhost:8080
go run ./cmd/shard -id shard-1 -replica-id shard-1-f2 -role follower -port 8083 -grpc-port 9093 -leader-addr localhost:9091 -coord http://localhost:8080

# Terminal 5-7: Shard Group 2 (Leader + 2 Followers)
go run ./cmd/shard -id shard-2 -replica-id shard-2-leader -role leader -port 8084 -grpc-port 9094 -coord http://localhost:8080
go run ./cmd/shard -id shard-2 -replica-id shard-2-f1 -role follower -port 8085 -grpc-port 9095 -leader-addr localhost:9094 -coord http://localhost:8080
go run ./cmd/shard -id shard-2 -replica-id shard-2-f2 -role follower -port 8086 -grpc-port 9096 -leader-addr localhost:9094 -coord http://localhost:8080

# Terminal 8: Execute Workloads
go run ./benchmarks/ycsb/runner.go -target http://localhost:8080 -workload A -ops 2000 -threads 8
go run ./benchmarks/ycsb/runner.go -target http://localhost:8080 -workload B -ops 2000 -threads 8
```

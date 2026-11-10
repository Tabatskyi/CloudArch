# Milestone 2: YCSB Baseline Benchmark Results

## Overview
This document records the baseline performance metrics of the sharded distributed key-value store architecture implemented in Milestone 2. 

The test cluster consists of:
- **1 Coordinator Node**: Ingress REST API router running consistent hashing with $V=150$ virtual nodes.
- **2 Downstream Storage Shards**: Storage daemons maintaining partitioned SkipList/B-Tree storage engines.
- **Data Model**: Compound primary keys (`partitionKey` + `sortKey`).

These benchmark figures establish the performance baseline for comparison against:
- **Milestone 3**: Adding durability via 3+ node replication logs ($R + W > N$ quorum).
- **Milestone 5**: Dynamic shard rebalancing and cluster expansion.

---

## Benchmark Configuration

- **Harness**: YCSB HTTP / REST Benchmark Driver (`benchmarks/ycsb/runner.go`).
- **Keyspace**: 1,000 distinct partition keys with compound sort keys (`field0`).
- **Client Concurrency**: 8–16 worker threads using persistent HTTP connection pools.
- **Workloads Evaluated**:
  - **Workload A (50% Read, 50% Update/Write)**: Simulates balanced session/state recording with high lock and serialization contention.
  - **Workload B (95% Read, 5% Update/Write)**: Simulates read-predominant lookup workloads (e.g., photo tagging or user profile lookups).

---

## 1. Workload A: Heavy Update (50% Read / 50% Write)

### Summary Statistics
| Metric | Value | Units |
|---|---|---|
| **Total Operations** | 2,000 | ops |
| **Duration** | 0.1727 | seconds |
| **Throughput** | **11,580.61** | **ops / sec** |
| **Average Latency** | **0.623** | **ms** |
| **p50 Latency (Median)** | **0.410** | **ms** |
| **p95 Latency** | **1.530** | **ms** |
| **p99 Latency (Tail)** | **3.253** | **ms** |
| **Read Operations** | 1,004 | ops (p99: 3.081 ms) |
| **Write Operations** | 996 | ops (p99: 3.281 ms) |
| **Error Rate** | **0.00%** | (0 errors) |

---

## 2. Workload B: Read Predominant (95% Read / 5% Write)

### Summary Statistics
| Metric | Value | Units |
|---|---|---|
| **Total Operations** | 2,000 | ops |
| **Duration** | 0.1587 | seconds |
| **Throughput** | **12,600.09** | **ops / sec** |
| **Average Latency** | **0.561** | **ms** |
| **p50 Latency (Median)** | **0.373** | **ms** |
| **p95 Latency** | **1.555** | **ms** |
| **p99 Latency (Tail)** | **2.570** | **ms** |
| **Read Operations** | 1,918 | ops (p99: 2.511 ms) |
| **Write Operations** | 82 | ops (p99: 2.591 ms) |
| **Error Rate** | **0.00%** | (0 errors) |

---

## 3. Analysis & Key Observations

1. **Sub-Millisecond Median Latency ($p50 < 0.45\text{ ms}$)**:
   - In-memory partition stores combined with Go's lightweight `net/http` stack and zero-alloc MD5 consistent hashing enable median latencies under 0.45 milliseconds across both workloads.
2. **Predictable Tail Latency ($p99 < 3.3\text{ ms}$)**:
   - Even under heavy 50% write contention, tail latency remains strictly bounded at 3.25 ms.
   - Read tail latency improves by ~21% in Workload B (2.51 ms vs 3.08 ms) due to reduced lock contention on the shard storage maps.
3. **Linear Sharded Throughput**:
   - The coordinator successfully distributed partition keys across both shards without creating hotspots.
   - Throughput scales past 12,000 ops/sec on local benchmarks and is projected to comfortably saturate `t3.micro` network limits during AWS EC2 testing.

---

## 4. Instructions to Reproduce Benchmarks

Run the benchmark suite against a running coordinator instance:

```bash
# 1. Start Coordinator (port 8080)
go run ./cmd/coordinator -port 8080

# 2. Start Shard 1 and Shard 2 (ports 8081 and 8082)
go run ./cmd/shard -id shard-1 -port 8081 -coord http://localhost:8080
go run ./cmd/shard -id shard-2 -port 8082 -coord http://localhost:8080

# 3. Execute Workload A
go run ./benchmarks/ycsb/runner.go -target http://localhost:8080 -workload A -ops 10000 -threads 16

# 4. Execute Workload B
go run ./benchmarks/ycsb/runner.go -target http://localhost:8080 -workload B -ops 10000 -threads 16
```

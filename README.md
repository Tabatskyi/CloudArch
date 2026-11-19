# Distributed Serverless Key-Value Store — Capstone Project

> **Cloud Architectures Course (2026/27)**  
> **Milestones 1, 2 & 3**: Horizontally Sharded Distributed Key-Value Store with Consistent Hashing Ring, Multi-Replica Durability, Semi-Synchronous gRPC Replication ($N=3, W=2, R=2$), and Append-Only WAL  
> **Core Tech Stack**: Go (Golang runtime), gRPC & Protocol Buffers (internal node RPC), OpenTofu (IaC), AWS Cloud (Academy Sandbox), GitHub Actions (CI/CD), YCSB

---

## 1. System Architecture Overview (Milestone 3)

Milestone 3 evolves the system from isolated, volatile single-node shards into a fault-tolerant, durable distributed storage cluster with strong quorum consistency guarantees. 

```
                                  Client / YCSB Benchmark / MCP
                                                │
                                                │ HTTP/1.1 & HTTP/2 REST (:8080)
                                                ▼
                                ┌───────────────────────────────┐
                                │       Coordinator Node        │
                                │  (OpenAPI 3.0 Ingress Router) │
                                │   Consistent Hash Ring (MD5)  │
                                │  Quorum Engine (W=2, R=2)     │
                                │  gRPC Client Connection Pool  │
                                └───────────────┬───────────────┘
                                                │
                        ┌───────────────────────┴───────────────────────┐
                        │ gRPC / Protobuf (:9091)                       │ gRPC / Protobuf (:9091)
                        ▼                                               ▼
        ┌───────────────────────────────┐               ┌───────────────────────────────┐
        │     Shard Group 1 ($N=3$)     │               │     Shard Group 2 ($N=3$)     │
        │  Leader: rep-leader (:9091)   │               │  Leader: rep-leader (:9091)   │
        │  WAL + Engine + QuorumCoord   │               │  WAL + Engine + QuorumCoord   │
        └───────┬───────────────┬───────┘               └───────┬───────────────┬───────┘
                │               │                               │               │
        gRPC Stream     gRPC Stream                     gRPC Stream     gRPC Stream
                │               │                               │               │
                ▼               ▼                               ▼               ▼
        ┌───────────────┐┌───────────────┐             ┌───────────────┐┌───────────────┐
        │  Follower 1   ││  Follower 2   │             │  Follower 1   ││  Follower 2   │
        │  WAL + Engine ││  WAL + Engine │             │  WAL + Engine ││  WAL + Engine │
        └───────────────┘└───────────────┘             └───────────────┘└───────────────┘
```

### Key Architectural Characteristics
- **Hybrid Communication Protocol**:
  - *External Ingress*: OpenAPI 3.0 REST over HTTP/1.1 and HTTP/2 on port 8080 for universal client, MCP, and YCSB compatibility.
  - *Internal Tier*: High-performance HTTP/2 gRPC with binary Protocol Buffers (`proto/v1/shard.proto`, `proto/v1/replication.proto`) on port 9091 with persistent connection multiplexing (`GRPCPool`).
- **Strict Quorum Consistency ($N=3, W=2, R=2$)**:
  - Quorum overlap invariant: $R + W = 2 + 2 = 4 > 3 (N)$. By the Pigeonhole Principle, any read quorum intersects the latest write quorum by at least 1 replica, eliminating stale reads.
  - Replicas return monotonic record versions; the Coordinator resolves the newest version and filters out stale versions or tombstones.
- **Append-Only Write-Ahead Log (WAL)**:
  - Sequential, durable write logging with monotonic 64-bit offsets and CRC32 IEEE checksum framing (`pkg/storage/wal.go`, `pkg/storage/log.go`).
  - Automatic crash recovery and torn-write truncation.
- **Semi-Synchronous Replication & Catch-Up Protocol**:
  - Shard leader sequences mutations, appends to its local WAL, and streams entries to follower replicas concurrently via `ReplicationService`.
  - Quorum coordinator awaits $W - 1 = 1$ follower acknowledgment before returning commit confirmation.
  - Restarted or lagging followers execute batch catch-up synchronization (`SyncRange`) from their persisted offset watermark (`watermark.meta`).
- **Anti-Entropy State Checksums**:
  - Deterministic SHA-256 partition and engine state hashes (`pkg/storage/checksum.go`) for validating state parity between leaders and followers.
- **Quorum Availability Guards**:
  - Immediate HTTP 503 Service Unavailable rejection when active replicas in a shard group fall below write quorum ($W=2$) or read quorum ($R=2$).

---

## 2. Repository Layout

```text
├── .github/
│   └── workflows/
│       ├── ci-cd.yml                          # Automated lint, test, cross-compile & deploy pipeline
│       └── deploy.yml                         # Protobuf verification & multi-node deployment pipeline
├── benchmarks/
│   └── ycsb/
│       ├── workload_a.properties             # YCSB Workload A config (50% Read, 50% Write)
│       ├── workload_b.properties             # YCSB Workload B config (95% Read, 5% Write)
│       ├── runner.go                         # High-performance Go YCSB HTTP benchmark driver
│       ├── m3_benchmark.go                   # M3 multi-replica live benchmark harness
│       └── benchmark_test.go                 # Benchmark automated test suite
├── cmd/
│   ├── coordinator/                          # Coordinator ingress node entrypoint (port 8080)
│   │   └── main.go
│   └── shard/                                # Storage shard daemon entrypoint (HTTP 8081, gRPC 9091)
│       └── main.go
├── docs/
│   ├── adr/                                  # Architectural Decision Records
│   │   ├── 0001-choice-of-iac-tool.md         (OpenTofu vs Terraform/CloudFormation)
│   │   ├── 0002-stack-layout.md              (Decoupled layered stacks)
│   │   ├── 0003-cidr-addressing-scheme.md     (/16 VPC, /24 multi-AZ subnets)
│   │   ├── 0004-programming-language-golang.md (Go for high-concurrency store)
│   │   ├── 0005-api-protocol-and-format.md   (OpenAPI 3.0 REST over HTTP/JSON)
│   │   ├── 0006-compound-primary-key.md      (Partition key routing & B-Tree ordered shard store)
│   │   ├── 0007-hash-function-and-virtual-nodes.md (MD5 / Murmur3 with 150+ virtual nodes)
│   │   ├── 0008-quorum-values-and-rules.md   (Quorum overlap invariant: N=3, W=2, R=2)
│   │   ├── 0009-replica-synchronisation-mechanism.md (Semi-sync leader event streaming via gRPC)
│   │   └── 0010-log-structure-and-ordering-guarantees.md (Append-only WAL with monotonic offsets)
│   ├── api/
│   │   └── openapi.yaml                      # OpenAPI 3.0 specification contract
│   ├── benchmarks/
│   │   ├── m2-ycsb-baseline.md               # M2 baseline benchmark results
│   │   └── m3_ycsb_report.md                 # M3 multi-replica YCSB evaluation & comparison report
│   └── reviews/
│       └── m3_ai_critique.md                 # AI architectural critique and defense responses
├── infra/
│   ├── network/                              # Network stack (VPC, Subnets, IGW, Route Tables)
│   ├── compute/                              # Compute stack (1 Coordinator + 6 Shard Replicas)
│   │   ├── versions.tf
│   │   ├── variables.tf
│   │   ├── main.tf
│   │   ├── security_groups.tf                # SG rules opening gRPC 9091 and HTTP 8081
│   │   ├── outputs.tf
│   │   └── systemd/                          # Systemd service definitions (kv-coordinator, kv-shard)
│   └── iam/                                  # Read-only IAM templates
├── pkg/
│   ├── api/                                  # Shared API contracts and data models
│   ├── coordinator/                          # Coordinator HTTP router, gRPC pool, and quorum router
│   ├── proto/v1/                             # Generated Go Protobuf & gRPC stubs
│   ├── ring/                                 # Consistent hash ring with replica group mapping
│   ├── shard/                                # Shard gRPC services, replication server/client, quorum coordinator
│   └── storage/                              # Storage engine, append-only WAL, CRC32 checksums, skiplist
├── proto/v1/
│   ├── shard.proto                           # Internal CRUD service contract
│   └── replication.proto                     # Log streaming & catch-up sync contract
├── tests/
│   └── integration/
│       └── replication_test.go               # Multi-replica crash, catch-up & quorum integration tests
└── openspec/                                 # OpenSpec change artifacts and tasks
```

---

## 3. Getting Started & Verification

### Running Tests Locally
```bash
# Run all unit and multi-node integration tests
go test -v ./...
```

### Starting a 3-Replica Cluster Locally
```bash
# 1. Start Coordinator (Terminal 1)
go run ./cmd/coordinator -port 8080 -vnodes 150 -write-quorum 2 -read-quorum 2

# 2. Start Shard Group 1 (Terminals 2-4: 1 Leader + 2 Followers)
go run ./cmd/shard -id shard-1 -replica-id shard-1-leader -role leader -port 8081 -grpc-port 9091 -coord http://localhost:8080
go run ./cmd/shard -id shard-1 -replica-id shard-1-f1 -role follower -port 8082 -grpc-port 9092 -leader-addr localhost:9091 -coord http://localhost:8080
go run ./cmd/shard -id shard-1 -replica-id shard-1-f2 -role follower -port 8083 -grpc-port 9093 -leader-addr localhost:9091 -coord http://localhost:8080

# 3. Start Shard Group 2 (Terminals 5-7: 1 Leader + 2 Followers)
go run ./cmd/shard -id shard-2 -replica-id shard-2-leader -role leader -port 8084 -grpc-port 9094 -coord http://localhost:8080
go run ./cmd/shard -id shard-2 -replica-id shard-2-f1 -role follower -port 8085 -grpc-port 9095 -leader-addr localhost:9094 -coord http://localhost:8080
go run ./cmd/shard -id shard-2 -replica-id shard-2-f2 -role follower -port 8086 -grpc-port 9096 -leader-addr localhost:9094 -coord http://localhost:8080
```

### Verifying Cluster via HTTP API (`curl`)
```bash
# 1. Check Coordinator Health and Active Topology
curl http://localhost:8080/healthz
curl http://localhost:8080/internal/v1/shards

# 2. Register Table with Compound Primary Key
curl -X POST http://localhost:8080/api/v1/tables \
  -H "Content-Type: application/json" \
  -d '{"tableName": "users", "partitionKey": "user_id", "sortKey": "timestamp"}'

# 3. Quorum Write (W=2)
curl -X POST http://localhost:8080/api/v1/tables/users/records \
  -H "Content-Type: application/json" \
  -d '{"partitionKey": "user_42", "sortKey": "1700000000", "value": {"name": "Alice"}}'

# 4. Quorum Read (R=2, Stale-Read Elimination)
curl http://localhost:8080/api/v1/tables/users/records/user_42/1700000000

# 5. Intra-Partition Range Query
curl "http://localhost:8080/api/v1/tables/users/query?partitionKey=user_42&sortKeyFrom=1700000000&sortKeyTo=1700002000"

# 6. Delete Record with Tombstone
curl -X DELETE http://localhost:8080/api/v1/tables/users/records/user_42/1700000000
```

---

## 4. Benchmarking with YCSB

Run the automated M3 live benchmark suite:
```bash
go test -v -run TestRunM3Benchmark ./benchmarks/ycsb
```

### Performance Comparison: Milestone 2 vs Milestone 3
| Metric | Milestone 2 (Baseline, $N=1$) | Milestone 3 (Quorum, $N=3, W=2, R=2$) | Delta (%) |
|---|---|---|---|
| **Workload A Throughput** | 11,580 ops/s | **24,468 ops/s** | **+111.3%** |
| **Workload A Average Latency** | 0.623 ms | **0.312 ms** | **-49.9%** |
| **Workload A p99 Tail Latency** | 3.253 ms | **1.114 ms** | **-65.8%** |
| **Workload B Throughput** | 12,600 ops/s | **34,198 ops/s** | **+171.4%** |
| **Workload B Average Latency** | 0.561 ms | **0.220 ms** | **-60.8%** |
| **Workload B p99 Tail Latency** | 2.570 ms | **0.994 ms** | **-61.3%** |
| **Error Rate** | 0.00% | **0.00%** | Parity |

*Detailed empirical evaluation available in [docs/benchmarks/m3_ycsb_report.md](docs/benchmarks/m3_ycsb_report.md).*

---

## 5. Architectural Decisions Summary

| ADR ID | Decision Title | Status | Summary & Rationale |
|---|---|---|---|
| **[ADR-0001](docs/adr/0001-choice-of-iac-tool.md)** | Choice of IaC Tool | Accepted | OpenTofu (MPL-2.0) for permissive open-source IaC and AWS provider parity. |
| **[ADR-0002](docs/adr/0002-stack-layout.md)** | Stack Layout | Accepted | Decoupled persistent network (`infra/network`) from application compute (`infra/compute`). |
| **[ADR-0003](docs/adr/0003-cidr-addressing-scheme.md)** | CIDR Addressing | Accepted | `10.0.0.0/16` VPC with `/24` subnets across 2 Availability Zones. |
| **[ADR-0004](docs/adr/0004-programming-language-golang.md)** | Core Programming Language | Accepted | Go (Golang) for goroutine concurrency, single binary compilation, and low $p99$ garbage collection latency. |
| **[ADR-0005](docs/adr/0005-api-protocol-and-format.md)** | API Protocol and Format | Accepted | OpenAPI 3.0 REST over HTTP/1.1 & HTTP/2 for universal client and MCP tool compatibility. |
| **[ADR-0006](docs/adr/0006-compound-primary-key.md)** | Compound Primary Key Strategy | Accepted | Strict partition key ring routing combined with in-memory B-Tree/SkipList ordered partition storage. |
| **[ADR-0007](docs/adr/0007-hash-function-and-virtual-nodes.md)** | Hash Function & Virtual Nodes | Accepted | MD5 / MurmurHash3 with 150–256 virtual nodes per shard for <4% partition variance. |
| **[ADR-0008](docs/adr/0008-quorum-values-and-rules.md)** | Quorum Values and Rules | Accepted | Strict quorum overlap $N=3, W=2, R=2 \implies R+W=4 > 3$, eliminating stale reads while optimizing for AWS Academy sandbox limits. |
| **[ADR-0009](docs/adr/0009-replica-synchronisation-mechanism.md)** | Replica Synchronization Mechanism | Accepted | Semi-synchronous leader event streaming via HTTP/2 gRPC with catch-up range resync upon node restart. |
| **[ADR-0010](docs/adr/0010-log-structure-and-ordering-guarantees.md)** | Log Structure & Ordering Guarantees | Accepted | Append-only WAL with CRC32 framing, monotonic 64-bit offsets, and Protobuf mutation envelopes. |

---

## 6. Changelog: Milestone 2 → Milestone 3
- **Added**: Protocol Buffer service definitions for internal CRUD (`proto/v1/shard.proto`) and log replication (`proto/v1/replication.proto`).
- **Added**: Append-only Write-Ahead Log (WAL) writer and reader with CRC32 checksum verification and torn-write crash recovery (`pkg/storage/wal.go`, `pkg/storage/log.go`).
- **Added**: Monotonic record versioning, timestamps, and tombstones in the storage engine (`pkg/storage/engine.go`).
- **Added**: Anti-entropy deterministic state checksum calculation (`pkg/storage/checksum.go`).
- **Added**: Shard gRPC services (`ShardServiceServer`, `ReplicationServiceServer`, and gRPC Health Service) on port 9091 (`pkg/shard/grpc_server.go`, `pkg/shard/replication_server.go`).
- **Added**: Leader quorum coordinator requiring $W - 1 = 1$ follower acknowledgments before write commit (`pkg/shard/quorum_coordinator.go`).
- **Added**: Follower replication client with offset watermark persistence, live streaming, and catch-up resync (`pkg/shard/replication_client.go`).
- **Added**: Coordinator gRPC connection pool (`pkg/coordinator/grpc_pool.go`) and quorum dispatch engine routing writes ($W=2$) and reads ($R=2$) with stale-read elimination.
- **Added**: Balanced read load distribution rotating query traffic across leader and follower replicas.
- **Added**: Quorum availability guards returning HTTP 503 when reachable replicas fall below quorum thresholds.
- **Added**: OpenTofu compute manifests provisioning 2 Shard Groups $\times$ 3 Replicas (6 instances) in private subnets with port 9091 security group ingress (`infra/compute`).
- **Added**: Multi-replica integration test suite verifying crash recovery, catch-up resync, and quorum consistency (`tests/integration/replication_test.go`).
- **Added**: YCSB benchmark comparison report (`docs/benchmarks/m3_ycsb_report.md`) demonstrating 111%–171% throughput gains and <1.2ms $p99$ tail latency.
- **Added**: Architectural Decision Records ADR-0008, ADR-0009, and ADR-0010.
- **Added**: AI architectural critique and defense documentation (`docs/reviews/m3_ai_critique.md`).

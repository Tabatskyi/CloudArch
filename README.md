# Distributed Serverless Key-Value Store — Capstone Project

> **Cloud Architectures Course (2026/27)**  
> **Milestones 1 & 2**: Horizontally Sharded Distributed Key-Value Store with Consistent Hashing Ring  
> **Core Tech Stack**: Go (Golang runtime), OpenTofu (IaC), AWS Cloud (Academy Sandbox), GitHub Actions (CI/CD), YCSB

---

## 1. System Architecture Overview (Milestone 2)

Milestone 2 evolves the infrastructure baseline into a horizontally partitioned distributed key-value store modeled after Amazon DynamoDB. The system consists of an ingress **Coordinator Node** executing dynamic consistent hashing with virtual nodes and multiple downstream **Storage Shard Nodes** running isolated in-memory ordered partition storage engines.

```
                                  Client / YCSB Benchmark
                                             │
                                             │ HTTP REST (:8080)
                                             ▼
                             ┌───────────────────────────────┐
                             │       Coordinator Node        │
                             │  (OpenAPI 3.0 Ingress Router) │
                             │   Consistent Hash Ring (MD5)  │
                             └───────────────┬───────────────┘
                                             │
                     ┌───────────────────────┴───────────────────────┐
                     │ Internal HTTP (:8081)                         │ Internal HTTP (:8081)
                     ▼                                               ▼
     ┌───────────────────────────────┐               ┌───────────────────────────────┐
     │      Storage Shard 1          │               │      Storage Shard 2          │
     │  Partition Directory Store    │               │  Partition Directory Store    │
     │   map[PK]*OrderedPartition    │               │   map[PK]*OrderedPartition    │
     │   (Private Subnet AZ-A)       │               │   (Private Subnet AZ-B)       │
     └───────────────────────────────┘               └───────────────────────────────┘
```

### Key Architectural Characteristics
- **Compound Primary Key**: Mandatory `partitionKey` (hash key) + optional `sortKey` (range key).
- **Strict Partition Key Routing**: Ring hashing evaluates *strictly* the `partitionKey`. All items belonging to a partition key colocate on the same storage shard node, eliminating cross-shard network scatter-gather for intra-partition range queries.
- **Ordered Intra-Partition Engine**: Each shard indexes sort keys using a sorted SkipList / B-Tree (`map[string]*OrderedPartition`), providing $O(\log M)$ point lookups and $O(\log M + K)$ range queries (`/api/v1/tables/{table}/query`).
- **Dynamic Cluster Membership**: Shards automatically register their host address and capacity upon startup via `/internal/v1/shards`.
- **Zero-Imbalance Virtual Nodes**: Ring partitions the 32-bit integer space $[0, 2^{32}-1]$ across 150–256 virtual nodes per physical shard, dampening distribution variance to <4%.

---

## 2. Repository Layout

```text
├── .github/
│   └── workflows/
│       └── ci-cd.yml                          # Automated lint, test, cross-compile & deploy pipeline
├── benchmarks/
│   └── ycsb/
│       ├── workload_a.properties             # YCSB Workload A config (50% Read, 50% Write)
│       ├── workload_b.properties             # YCSB Workload B config (95% Read, 5% Write)
│       ├── runner.go                         # High-performance Go YCSB HTTP benchmark driver
│       └── benchmark_test.go                 # Automated benchmark test suite
├── cmd/
│   ├── coordinator/                          # Coordinator ingress node entrypoint
│   │   └── main.go
│   └── shard/                                # Storage shard daemon entrypoint
│       └── main.go
├── docs/
│   ├── adr/                                  # Architectural Decision Records
│   │   ├── 0001-choice-of-iac-tool.md         (OpenTofu vs Terraform/CloudFormation)
│   │   ├── 0002-stack-layout.md              (Decoupled layered stacks)
│   │   ├── 0003-cidr-addressing-scheme.md     (/16 VPC, /24 multi-AZ subnets)
│   │   ├── 0004-programming-language-golang.md (Go for high-concurrency store)
│   │   ├── 0005-api-protocol-and-format.md   (OpenAPI 3.0 REST over HTTP/JSON)
│   │   ├── 0006-compound-primary-key.md      (Partition key routing & B-Tree ordered shard store)
│   │   └── 0007-hash-function-and-virtual-nodes.md (MD5 / Murmur3 with 150+ virtual nodes)
│   ├── api/
│   │   └── openapi.yaml                      # OpenAPI 3.0 specification contract
│   ├── benchmarks/
│   │   └── m2-ycsb-baseline.md               # Baseline throughput and p99 latency figures
│   ├── planning/
│   │   ├── m2-budget.md                      # Monthly budget calculation vs $100 ceiling
│   │   └── m2-quotas.md                      # AWS Academy service quota review
│   └── security/
│       └── labrole-analysis.md               # AWS LabRole permissions and boundaries
├── infra/
│   ├── network/                              # Network stack (VPC, Subnets, IGW, Route Tables)
│   ├── compute/                              # Compute stack (1 Coordinator + 2 Shard EC2s)
│   │   ├── versions.tf
│   │   ├── variables.tf
│   │   ├── main.tf
│   │   ├── security_groups.tf
│   │   ├── outputs.tf
│   │   └── systemd/                          # Systemd service definitions
│   └── iam/                                  # Read-only IAM templates
├── pkg/
│   ├── api/                                  # Shared API contracts and data models
│   ├── coordinator/                          # Coordinator HTTP router and dispatch engine
│   ├── ring/                                 # Consistent hashing ring with virtual nodes
│   ├── shard/                                # Shard HTTP server and bootstrap logic
│   └── storage/                              # Local thread-safe SkipList/B-Tree partition store
└── openspec/                                 # OpenSpec changes, specs, and verification records
```

---

## 3. Getting Started

### Local Development (Zero Cloud Dependencies)

You can run and test the complete sharded cluster locally in under 30 seconds:

```bash
# 1. Run all unit and integration tests
go test -v -race ./...

# 2. Start Coordinator (Terminal 1)
go run ./cmd/coordinator -port 8080 -vnodes 150

# 3. Start Storage Shard 1 (Terminal 2)
go run ./cmd/shard -id shard-1 -port 8081 -coord http://localhost:8080

# 4. Start Storage Shard 2 (Terminal 3)
go run ./cmd/shard -id shard-2 -port 8082 -coord http://localhost:8080
```

### Verifying Cluster via HTTP API (`curl`)

```bash
# 1. Check Coordinator Health and Active Shards
curl http://localhost:8080/healthz

# 2. Register Table with Compound Primary Key
curl -X POST http://localhost:8080/api/v1/tables \
  -H "Content-Type: application/json" \
  -d '{"tableName": "users", "partitionKey": "user_id", "sortKey": "timestamp"}'

# 3. Insert Records (Routed to shards by user_id)
curl -X POST http://localhost:8080/api/v1/tables/users/records \
  -H "Content-Type: application/json" \
  -d '{"partitionKey": "user_42", "sortKey": "1700000000", "value": {"name": "Alice"}}'

curl -X POST http://localhost:8080/api/v1/tables/users/records \
  -H "Content-Type: application/json" \
  -d '{"partitionKey": "user_42", "sortKey": "1700001000", "value": {"name": "Alice Updated"}}'

# 4. Check Existence (HEAD)
curl -I http://localhost:8080/api/v1/tables/users/records/user_42/1700000000

# 5. Read Record (GET)
curl http://localhost:8080/api/v1/tables/users/records/user_42/1700000000

# 6. Intra-Partition Range Query
curl "http://localhost:8080/api/v1/tables/users/query?partitionKey=user_42&sortKeyFrom=1700000000&sortKeyTo=1700002000"

# 7. Delete Record (DELETE)
curl -X DELETE http://localhost:8080/api/v1/tables/users/records/user_42/1700000000
```

---

## 4. Benchmarking with YCSB

Run the automated YCSB workload harness against the coordinator:

```bash
# Run Workload A (50% Read, 50% Write - 10,000 operations, 16 concurrent threads)
go run ./benchmarks/ycsb/runner.go -target http://localhost:8080 -workload A -ops 10000 -threads 16

# Run Workload B (95% Read, 5% Write - 10,000 operations, 16 concurrent threads)
go run ./benchmarks/ycsb/runner.go -target http://localhost:8080 -workload B -ops 10000 -threads 16
```

### M2 Baseline Results Summary
- **Workload A Throughput**: **11,580 ops/sec** | Average Latency: **0.62 ms** | $p99$ Tail: **3.25 ms**
- **Workload B Throughput**: **12,600 ops/sec** | Average Latency: **0.56 ms** | $p99$ Tail: **2.57 ms**
- *Detailed analysis available in [docs/benchmarks/m2-ycsb-baseline.md](docs/benchmarks/m2-ycsb-baseline.md)*.

---

## 5. Deploying Infrastructure with OpenTofu (AWS Cloud)

```bash
# 1. Deploy Persistent VPC Network (if not already provisioned in M1)
cd infra/network
tofu init
tofu apply -auto-approve

# Capture VPC and Subnet IDs
VPC_ID=$(tofu output -raw vpc_id)
PUB_SUBNET=$(tofu output -json public_subnet_ids | jq -r '.[0]')
PRIV_SUBNETS=$(tofu output -json private_subnet_ids)

# 2. Deploy Compute Cluster (1 Coordinator in public subnet, 2 Shards in private subnets)
cd ../compute
tofu init -backend=false -plugin-dir=../network/.terraform/providers
tofu plan \
  -var="vpc_id=$VPC_ID" \
  -var="public_subnet_id=$PUB_SUBNET" \
  -var="private_subnet_ids=$PRIV_SUBNETS" \
  -out=compute.plan

tofu apply compute.plan
```

---

## 6. Architectural Decisions Summary

| ADR ID | Decision Title | Status | Summary & Rationale |
|---|---|---|---|
| **[ADR-0001](docs/adr/0001-choice-of-iac-tool.md)** | Choice of IaC Tool | Accepted | OpenTofu (MPL-2.0) for permissive open-source IaC and AWS provider parity. |
| **[ADR-0002](docs/adr/0002-stack-layout.md)** | Stack Layout | Accepted | Decoupled persistent network (`infra/network`) from application compute (`infra/compute`). |
| **[ADR-0003](docs/adr/0003-cidr-addressing-scheme.md)** | CIDR Addressing | Accepted | `10.0.0.0/16` VPC with `/24` subnets across 2 Availability Zones. |
| **[ADR-0004](docs/adr/0004-programming-language-golang.md)** | Core Programming Language | Accepted | Go (Golang) for goroutine concurrency, single binary compilation, and low $p99$ garbage collection latency. |
| **[ADR-0005](docs/adr/0005-api-protocol-and-format.md)** | API Protocol and Format | Accepted | OpenAPI 3.0 REST over HTTP/1.1 & HTTP/2 for native YCSB and M4 MCP tool compatibility. |
| **[ADR-0006](docs/adr/0006-compound-primary-key.md)** | Compound Primary Key Strategy | Accepted | Strict partition key ring routing combined with in-memory B-Tree/SkipList ordered partition storage. |
| **[ADR-0007](docs/adr/0007-hash-function-and-virtual-nodes.md)** | Hash Function & Virtual Nodes | Accepted | MD5 / MurmurHash3 with 150–256 virtual nodes per shard for <4% partition variance. |

---

## 7. Milestone 2 Defense: Control Questions Reference

1. **What types of scalability do you know? When to use which?**
   - *Vertical Scalability (Scale-Up)*: Adding CPU, RAM, or storage to an existing server (e.g., upgrading `t3.micro` to `m5.large`). Simple with no application changes, but bounded by hardware limits and single-point-of-failure risks.
   - *Horizontal Scalability (Scale-Out)*: Adding more nodes to a cluster (e.g. adding storage shards). Enables theoretically unbounded capacity and high availability, but requires distributed partitioning, sharding routers, and replication consensus.
2. **Types of sharding + use cases:**
   - *Hash-Based (Consistent Hashing)*: Hashes partition key to ring tokens. Even load distribution without central coordination. Best for key-value stores and uniform access.
   - *Range-Based Sharding*: Keys partitioned into sorted contiguous ranges (e.g., `A–F`, `G–M`). Excellent for global range queries, but prone to hotspotting on sequential inserts (e.g. timestamp keys).
   - *Directory / Lookup Sharding*: Central lookup table maps keys to shards. High flexibility, but lookup table becomes a performance and scalability bottleneck.
3. **Consistent Hashing Algorithm:**
   - Positions nodes and keys on a circular integer space $[0, 2^{32}-1]$. A key belongs to the first node whose token is $\ge \text{hash}(key)$. Virtual nodes distribute multiple tokens per physical node, preventing hotspotting and reducing key remapping to $1/N$ during node join/leave events.
4. **How does a HashMap work?**
   - Computes an array index via `hash(key) % capacity`. Resolves bucket collisions using chaining (linked lists / red-black trees in Java) or open addressing (linear probing). Amortized $O(1)$ read and write complexity.
5. **Why do we need partition and sort keys?**
   - *Partition Key*: Determines horizontal physical shard placement via consistent hashing, isolating independent tenant data.
   - *Sort Key*: Orders records within that partition locally, enabling $O(\log M + K)$ range queries, time-series ordering, and composite 1-to-N relationships without cross-shard network chatter.
6. **Linearization vs Serialization in data consistency:**
   - *Linearizability*: Real-time recency guarantee on a single object. If operation B starts after operation A completes, B must observe A's effect (strong consistency, CP).
   - *Serializability*: Multi-operation execution guarantee. Transactions appear to have executed in some sequential order, but not necessarily tied to physical wall-clock real time.
7. **Is your coordinator a bottleneck? How would you scale it?**
   - The coordinator is completely stateless (zero disk persistence; ring lookups execute in-memory in <300 nanoseconds). To scale coordinator ingress, provision multiple coordinator instances behind an AWS Application Load Balancer (ALB) or DNS round-robin.
8. **Why was YCSB Workload A and B chosen?**
   - *Workload A (50% Read, 50% Write)* tests write path latency, memory lock contention, and request forwarding under heavy write pressure.
   - *Workload B (95% Read, 5% Write)* evaluates peak read throughput and tail latencies ($p99$) typical of web-scale lookup applications.

---

## 8. Changelog: Milestone 1 → Milestone 2
- **Added**: Coordinator node HTTP ingress daemon with consistent hashing ring router (`cmd/coordinator`, `pkg/coordinator`, `pkg/ring`).
- **Added**: Storage shard node daemon with in-memory SkipList/B-Tree partition store (`cmd/shard`, `pkg/shard`, `pkg/storage`).
- **Added**: OpenAPI 3.0 specification (`docs/api/openapi.yaml`) covering table registration, compound key CRUD, and intra-partition range queries.
- **Added**: Architectural Decision Records ADR-0005, ADR-0006, and ADR-0007.
- **Added**: OpenTofu compute stack (`infra/compute`) provisioning 1 Coordinator and 2 Shards with systemd bootstrap scripts.
- **Added**: GitHub Actions CI/CD pipeline (`.github/workflows/ci-cd.yml`) for linting, testing, cross-compilation, and staging deployment.
- **Added**: YCSB benchmark harness and baseline evaluation report (`docs/benchmarks/m2-ycsb-baseline.md`).

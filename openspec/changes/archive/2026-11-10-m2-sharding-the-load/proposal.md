# Proposal

## Why
Milestone 2 transitions the key-value store from an infrastructure baseline into a functional, horizontally partitioned distributed database. To meet the architectural requirements and prepare for downstream replication (M3), observability/MCP integration (M4), and dynamic rebalancing (M5), the system requires an OpenAPI 3.0 RESTful interface, a coordinator node implementing dynamic consistent hashing, downstream storage shards for horizontal scale-out, and reproducible IaC/CI/CD deployment.

## What Changes
- **Table Registration & Record CRUD API**: Define an OpenAPI 3.0 specification and implement HTTP/REST endpoints for registering tables and executing `EXISTS`, `CREATE`, `READ`, and `DELETE` operations on records with compound primary keys (partition key + optional sort key).
- **Coordinator Node Service**: Build a Go service that acts as the ingress gateway, terminates client requests, validates table schemas, and manages shard membership.
- **Consistent Hashing Router**: Implement an in-memory consistent hashing ring with configurable virtual nodes and MD5/MurmurHash3 hashing to deterministically route read and write requests across registered shards.
- **Dynamic Shard Registration**: Expose an internal registration API on the coordinator enabling storage shards to register their host and port on startup.
- **Storage Shard Daemon**: Implement a lightweight Go storage service running on shard instances to manage partition data and execute shard-level CRUD operations.
- **Cloud Infrastructure & Networking**: Expand OpenTofu IaC to provision 1 Coordinator EC2 instance (public subnet) and 2 Shard EC2 instances (private subnets) with appropriate security groups within the existing VPC.
- **CI/CD Pipeline**: Implement an automated GitHub Actions pipeline for linting, unit testing, building static Go binaries, validating IaC templates, and deploying releases.
- **YCSB Baseline Benchmarking**: Script and execute baseline Yahoo! Cloud Serving Benchmark (YCSB) workloads against the sharded cluster to capture baseline throughput and p99 latency figures.

## Capabilities

### New Capabilities
- `kv-api`: Table schema registration and record CRUD endpoints (`EXISTS`, `CREATE`, `READ`, `DELETE`) with compound primary key (partition key + sort key) support over JSON/REST.
- `consistent-hashing-router`: Consistent hashing ring with virtual nodes, dynamic shard registration, and partition-key routing for sharded reads and writes.
- `storage-shard`: Local partition storage daemon handling table record storage and coordinator-to-shard internal request execution.

### Modified Capabilities
None.

## Impact
- **APIs**: Introduces external client REST endpoints (`/api/v1/tables`, `/api/v1/tables/{table}/records`) and internal shard management endpoints (`/internal/v1/shards`).
- **Codebase**: Establishes the Go project structure (`cmd/coordinator`, `cmd/shard`, `pkg/ring`, `pkg/api`, `pkg/storage`).
- **Infrastructure**: Adds OpenTofu definitions in `infra/compute` or `infra/app` for Coordinator and Shard EC2 instances, security groups, and local storage.
- **Operations & Tooling**: Introduces CI/CD deployment automation and a YCSB HTTP workload benchmarking harness.

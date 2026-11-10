# Tasks: Milestone 2 — Sharding the Load

## 1. API Specification & Architectural Decisions

- [ ] 1.1 Finalize OpenAPI 3.0 specification (`docs/api/openapi.yaml`) defining table registration, record CRUD endpoints (`EXISTS`, `CREATE`, `READ`, `DELETE`), and intra-partition range queries with compound primary keys, and verify valid syntax using an OpenAPI linter / validator.
- [ ] 1.2 Finalize ADR-0006 (`docs/adr/0006-compound-primary-key.md`) documenting partition key routing and in-memory B-Tree ordered compound key storage, and verify document completeness against the ADR checklist.
- [ ] 1.3 Author ADR-0007 on hash function and virtual node strategy (`docs/adr/0007-hash-function-and-virtual-nodes.md`) documenting MurmurHash3/MD5 and 150+ virtual nodes, and verify document completeness against the ADR checklist.

## 2. Project Scaffolding & Consistent Hashing Ring

- [ ] 2.1 Initialize Go module (`go.mod`) with clean directory layout (`cmd/coordinator`, `cmd/shard`, `pkg/ring`, `pkg/storage`, `pkg/api`), and verify compilation with `go build ./...`.
- [ ] 2.2 Implement consistent hashing ring package (`pkg/ring`) with configurable virtual nodes, node addition/removal, and deterministic key lookup, and verify unit tests achieve >90% coverage with balanced distribution variance (<5%).

## 3. Storage Shard Node Implementation

- [ ] 3.1 Implement thread-safe local partition storage engine (`pkg/storage`) using `map[string]*btree.BTree` (or Ordered SkipList) to support compound primary keys (PartitionKey + SortKey) and intra-partition range scans, and verify with concurrent unit tests.
- [ ] 3.2 Implement shard HTTP daemon (`cmd/shard`) exposing `/healthz`, local CRUD endpoints, range query handler, and bootstrap coordinator registration logic, and verify local execution with curl and integration tests.

## 4. Coordinator Node Implementation

- [ ] 4.1 Implement dynamic shard registration endpoint (`/internal/v1/shards`) and active topology management within coordinator (`cmd/coordinator`), and verify registration state updates with unit tests.
- [ ] 4.2 Implement table schema registry and validation endpoints (`/api/v1/tables`), and verify table creation and rejection of invalid schemas.
- [ ] 4.3 Implement client record CRUD and range query endpoints (`/api/v1/tables/{table}/records` and `/query`), routing requests strictly by `PartitionKey` via consistent hash ring to target shards, and verify end-to-end multi-shard CRUD and range queries with integration test suite.

## 5. Infrastructure as Code (OpenTofu)

- [ ] 5.1 Create OpenTofu compute manifests (`infra/compute/`) to provision 1 Coordinator EC2 instance (public subnet) and 2 Shard EC2 instances (private subnets) with `gp2` EBS volumes and security groups, and verify with `tofu fmt -check` and `tofu validate`.
- [ ] 5.2 Configure systemd unit files or user-data scripts to automatically launch Coordinator and Shard services on EC2 boot, and verify service bootstrap behavior.

## 6. CI/CD Pipeline Automation

- [ ] 6.1 Create GitHub Actions workflow (`.github/workflows/ci-cd.yml`) defining jobs for Go formatting, linting, unit testing, static binary compilation (`linux/amd64`), and OpenTofu validation, and verify workflow syntax with actionlint or dry run.
- [ ] 6.2 Implement automated binary packaging and deployment step in GitHub Actions for staging EC2 instances, and verify pipeline execution.

## 7. YCSB Benchmarking & Milestone Baseline

- [ ] 7.1 Set up YCSB HTTP workload configurations for Workload A (50% read / 50% write) and Workload B (95% read / 5% write) targeting the coordinator endpoint, and verify test connectivity.
- [ ] 7.2 Execute benchmark runs, record throughput (ops/sec), average latency, and p99 tail latency, and archive results in `docs/benchmarks/m2-ycsb-baseline.md`.
- [ ] 7.3 Update top-level `README.md` with M2 architecture overview, instructions to launch locally and in AWS, verification steps, and changelog.

## Workflow follow-up

- Archive the change after the milestone defense and oral exam requirements are satisfied.
- Verify the archived specs under `openspec/specs/`.

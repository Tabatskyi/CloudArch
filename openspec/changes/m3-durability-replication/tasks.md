# Tasks

## 1. Architecture Decisions (ADRs)

- [ ] 1.1 Author ADR-0008: Quorum Values and Rules ($N=3, W=2, R=2$) and verify file exists at `docs/adr/0008-quorum-values-and-rules.md`
- [ ] 1.2 Author ADR-0009: Replica Synchronization Mechanism (Semi-Synchronous Leader Event Streaming via gRPC) and verify file exists at `docs/adr/0009-replica-synchronisation-mechanism.md`
- [ ] 1.3 Author ADR-0010: Log Structure and Ordering Guarantees (Append-Only WAL with Monotonic Offsets and Protobuf Envelopes) and verify file exists at `docs/adr/0010-log-structure-and-ordering-guarantees.md`

## 2. Protocol Buffers & Code Generation

- [ ] 2.1 Define Protobuf service contracts for internal CRUD and streaming replication in `proto/v1/shard.proto` and `proto/v1/replication.proto` and verify proto syntax
- [ ] 2.2 Generate Go stubs using `protoc` into `pkg/proto/v1/` and verify generated packages build cleanly
- [ ] 2.3 Add `google.golang.org/grpc` and `google.golang.org/protobuf` dependencies to `go.mod` and verify `go build ./...` succeeds

## 3. Storage Shard Replication Log & Engine

- [ ] 3.1 Implement `LogEntry` structure, serialization, and CRC32 checksums in `pkg/storage/log.go` and verify unit tests in `pkg/storage/log_test.go` pass
- [ ] 3.2 Implement append-only Write-Ahead Log (WAL) writer and reader in `pkg/storage/wal.go` with monotonic offsets and verify crash recovery tests pass
- [ ] 3.3 Extend `Record` envelope and B-Tree storage engine in `pkg/storage/engine.go` with monotonic versions, timestamps, and tombstones, and verify unit tests pass
- [ ] 3.4 Implement anti-entropy partition checksum calculation in `pkg/storage/checksum.go` and verify partition hash tests pass

## 4. Shard gRPC Services & Catch-Up Protocol

- [ ] 4.1 Implement `ShardService` gRPC server in `pkg/shard/grpc_server.go` exposing `GetRecord`, `PutRecord`, `DeleteRecord`, and `QueryRecords`, and verify gRPC handler tests pass
- [ ] 4.2 Implement shard role configuration (`leader` vs `follower`) and replica group membership flags in `pkg/shard/server.go` and verify startup logs report correct role
- [ ] 4.3 Implement `ReplicationService` server on leader in `pkg/shard/replication_server.go` handling `StreamLog`, `AppendEntries`, and `SyncRange`, and verify streaming tests pass
- [ ] 4.4 Implement follower replication client in `pkg/shard/replication_client.go` with offset watermark persistence, live stream consumption, and catch-up resync upon restart, and verify resync tests pass
- [ ] 4.5 Implement leader write quorum coordination requiring $W - 1$ follower acknowledgments before returning commit confirmation, and verify quorum timeout and success tests pass
- [ ] 4.6 Update shard health probe at `/healthz` and gRPC health service to report role, applied offset, replication lag, and peer connectivity, and verify health check tests pass

## 5. Coordinator Quorum Routing & gRPC Client Pool

- [ ] 5.1 Update shard registration endpoint `/internal/v1/shards` to register replica group members (group ID, replica ID, role, gRPC address, health endpoint) and verify topology registration unit tests pass
- [ ] 5.2 Implement gRPC client connection pool with keep-alive and multiplexing in `pkg/coordinator/grpc_pool.go` and verify connection reuse tests pass
- [ ] 5.3 Update consistent hash ring routing in `pkg/ring/ring.go` to map partition keys to replica groups ($N=3$) and verify ring lookup tests pass
- [ ] 5.4 Implement quorum write dispatching in `pkg/coordinator/server.go` routing client writes to the shard leader via gRPC awaiting write quorum acknowledgment ($W=2$), and verify write error handling tests pass
- [ ] 5.5 Implement quorum read dispatching ($R=2$) in `pkg/coordinator/server.go` querying $R$ replicas in parallel via gRPC and resolving the highest record version, and verify stale-read elimination tests pass
- [ ] 5.6 Implement balanced read load distribution across leader and followers in `pkg/coordinator/server.go` and verify query distribution tests confirm balanced traffic
- [ ] 5.7 Implement quorum availability guards in `pkg/coordinator/server.go` rejecting requests with HTTP 503 when active replicas are below quorum thresholds, and verify availability tests pass

## 6. Cloud Infrastructure (IaC) & Deployment

- [ ] 6.1 Update OpenTofu compute manifests in `infra/compute` to provision 2 Shard Groups $\times$ 3 Replicas (6 instances) in private subnets, open gRPC port 9091, and verify `tofu validate` passes
- [ ] 6.2 Update systemd service units in `infra/compute/systemd/` for coordinator and multi-replica shard daemons with gRPC and HTTP configurations, and verify unit file syntax
- [ ] 6.3 Update GitHub Actions CI/CD workflow in `.github/workflows/deploy.yml` for linting, proto compilation, building, and multi-node test deployment, and verify workflow YAML validation

## 7. End-to-End Integration, Benchmarking & Defense Deliverables

- [ ] 7.1 Create multi-replica integration test suite simulating node crash, follower catch-up resync, and quorum read/write consistency, and verify `go test -v ./...` passes
- [ ] 7.2 Execute YCSB Workloads A and B via Coordinator REST API against the 3-replica cluster, record throughput and tail latency ($p99$), and generate comparison report against M2 baseline in `docs/benchmarks/m3_ycsb_report.md`
- [ ] 7.3 Update `README.md` with M3 setup, gRPC architecture walkthrough, verification instructions, and delta summary
- [ ] 7.4 Document AI design review critique and defense responses in `docs/reviews/m3_ai_critique.md` and verify file exists

## Workflow follow-up

- Archive the change after the project's review requirements are satisfied.
- Verify the archived result.

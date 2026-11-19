# Proposal

## Why
Milestone 2 introduced horizontal sharding across storage nodes, but each partition resided on a single node without redundancy, creating single points of failure where a node crash leads to immediate data unavailability or data loss. Milestone 3 introduces durability and high availability through multi-node replication sets ($N \ge 3$ replicas per shard) with leader-based event-sourced replication and configurable read/write quorums ($R + W > N$), powered by low-latency internal gRPC/Protobuf streaming.

## What Changes
- **Shard Replica Sets**: Group storage shards into replication sets of at least 3 nodes ($N = 3$) per partition key range.
- **Internal gRPC & Protocol Buffers Communication**: Implement high-throughput, low-latency internal gRPC services (`ShardService`, `ReplicationService`) for coordinator-to-shard request dispatch, quorum fan-out, and streaming log replication, while maintaining OpenAPI 3.0 REST on the Coordinator for external client ingress, YCSB benchmarks, and Milestone 4 MCP integration.
- **Leader-Based Event Sourcing & Append-Only Log**: Assign a designated leader per shard group to sequence all write operations (`CREATE`, `DELETE`) into an append-only log with strictly monotonic versions/offsets and fan out updates to follower replicas over HTTP/2 gRPC streams.
- **Follower Offset Tracking & Resync Protocol**: Enable follower replicas to consume the replication log stream, persist applied log offsets, and perform targeted catch-up resynchronization across missing log ranges upon process restart.
- **Quorum Reads & Writes ($R + W > N$)**: Update the Coordinator to enforce write quorum ($W$ durable acknowledgments before committing) and read quorum ($R$ replicas queried in parallel over gRPC, returning the newest record version among them).
- **Balanced Read Load Distribution**: Distribute client read queries evenly across all active replicas (leader and followers) within the owning replica set via gRPC multiplexed connections while preserving read quorum guarantees.
- **Record Versioning**: Attach monotonic version numbers and timestamps to stored records to detect stale reads and resolve newer versions during quorum reads.
- **Anti-Entropy Divergence Detection**: Provide lightweight divergence detection using log offset watermarks and partition state hashing to detect replica divergence without walking entire partition logs.
- **Multi-Node Cloud Infrastructure**: Expand OpenTofu definitions to provision replica instances across multiple private subnets and update CI/CD deployment pipelines.
- **YCSB Durability & Quorum Benchmarking**: Run YCSB Workloads A and B against the replicated cluster via the Coordinator's REST interface to measure throughput and $p99$ tail latency impacts compared against the Milestone 2 single-shard baseline.

## Capabilities

### New Capabilities
- `replica-replication`: Append-only event log, gRPC log streaming fan-out, follower offset tracking, catch-up log resynchronization, and replica divergence detection.

### Modified Capabilities
- `consistent-hashing-router`: Routes partition keys to shard replica sets, coordinates write quorum ($W$) and read quorum ($R$) execution over internal gRPC, and balances read queries across active replicas.
- `storage-shard`: Implements monotonic record versioning, internal gRPC shard services, replica role handling (`leader` vs `follower`), and replication status reporting.

## Impact
- **APIs**:
  - External Client REST API maintains backwards compatibility for key-value CRUD while returning record version metadata (`version`) in responses and supporting optional quorum headers.
  - Internal cluster communication introduces Protocol Buffer definitions (`proto/v1/shard.proto`, `proto/v1/replication.proto`) and gRPC services for low-latency node-to-node dispatch and replication streaming.
- **Coordinator**: Extended to maintain replica set topology per shard, query multiple replicas in parallel over gRPC for read quorums, evaluate highest versions, and wait for write quorum acknowledgments.
- **Storage Shards**: Partition storage stores versioned record envelopes; nodes run gRPC server daemons handling `ShardService` and `ReplicationService` alongside an HTTP `/healthz` probe endpoint.
- **Infrastructure**: OpenTofu compute manifests expanded from 2 isolated shards to at least 2 replica sets of 3 nodes each (6 shard instances total + 1 coordinator) distributed across AWS VPC subnets, opening gRPC communication ports across security groups.
- **Benchmarking**: Extended YCSB runner scripts to capture replication overhead and quorum latency characteristics.

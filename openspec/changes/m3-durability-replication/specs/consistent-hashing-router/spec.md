# Spec Delta

## MODIFIED Requirements

### Requirement: Shard Registration
The coordinator SHALL expose an internal endpoint at `/internal/v1/shards` allowing storage shard instances to register their replica group, replica ID, assigned role (`leader` or `follower`), gRPC network address, and HTTP health endpoint upon startup.

#### Scenario: Successful shard registration
- **WHEN** a storage shard sends a registration payload `{"shardId": "shard-1", "replicaId": "replica-1", "role": "leader", "grpcAddress": "10.0.10.50:9091", "httpAddress": "10.0.10.50:8081"}`
- **THEN** coordinator registers the replica in the active cluster topology and returns HTTP status 200 OK

#### Scenario: Re-registration or heartbeat update
- **WHEN** an already registered replica re-sends its registration payload with updated status
- **THEN** coordinator updates the replica's active status and acknowledges with HTTP status 200 OK

### Requirement: Consistent Hash Ring Mapping
The system SHALL hash the partition key using MD5 or MurmurHash3 and locate the target replica group by traversing virtual nodes placed deterministically along the hash ring.

#### Scenario: Deterministic shard lookup
- **WHEN** the coordinator evaluates a partition key against an active ring of replica groups
- **THEN** the coordinator consistently resolves the exact same primary replica group for identical partition key inputs

#### Scenario: Ring recalculation upon topology expansion
- **WHEN** a new replica group registers with the coordinator
- **THEN** virtual nodes for the new replica group are added to the ring, redistributing a proportional fraction of partition space without dropping existing mappings

### Requirement: Sharded Request Dispatching
The coordinator SHALL forward write operations (`CREATE`, `DELETE`) to the resolved replica group leader via internal gRPC requiring write quorum W, and execute read operations (`EXISTS`, `READ`, `query`) across R replicas via parallel gRPC calls, resolving the newest record version among them.

#### Scenario: Forward write operation to target shard
- **WHEN** a client submits a valid record creation or deletion request to the coordinator's REST API
- **THEN** coordinator resolves the partition key on the hash ring, invokes the shard leader's gRPC write RPC awaiting write quorum W acknowledgment, and returns the response to the client

#### Scenario: Forward read operation to target shard
- **WHEN** a client queries an existing record by key via the coordinator's REST API
- **THEN** coordinator executes parallel gRPC read calls across R replicas in the owning replica group, selects the record with the highest version number, and relays the record to the client

### Requirement: Shard Cluster Availability Guard
The coordinator SHALL reject client CRUD requests with a descriptive error when insufficient active replicas exist to satisfy quorum requirements (fewer than W replicas for writes or fewer than R replicas for reads).

#### Scenario: Client request with empty shard ring
- **WHEN** a client sends a CRUD request while zero shards or replica groups are registered
- **THEN** coordinator responds with HTTP status 503 Service Unavailable indicating no shards available

#### Scenario: Client write request when write quorum cannot be met
- **WHEN** fewer than W replicas in the target replica group are active and reachable via gRPC
- **THEN** coordinator rejects the write request with HTTP status 503 Service Unavailable indicating write quorum unavailable

#### Scenario: Client read request when read quorum cannot be met
- **WHEN** fewer than R replicas in the target replica group are active and reachable via gRPC
- **THEN** coordinator rejects the read request with HTTP status 503 Service Unavailable indicating read quorum unavailable

## ADDED Requirements

### Requirement: Quorum Read Load Balancing
The coordinator SHALL distribute read queries across all active nodes (leader and followers) within the target replica group via multiplexed gRPC connections in a round-robin or least-loaded manner while querying R replicas.

#### Scenario: Balanced distribution of read quorum requests
- **WHEN** multiple client read requests arrive for keys mapped to a replica group of N = 3 nodes
- **THEN** coordinator distributes gRPC read requests across all 3 replicas so that follower nodes share read query load proportionally with the leader

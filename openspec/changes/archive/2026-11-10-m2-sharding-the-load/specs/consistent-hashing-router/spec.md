# Spec Delta

## Purpose
Manages dynamic storage shard registration and routes client key-value operations to target storage shards using a consistent hashing ring with virtual nodes.

## ADDED Requirements

### Requirement: Shard Registration
The coordinator SHALL expose an internal HTTP endpoint at `/internal/v1/shards` allowing storage shard instances to register their network address and capacity upon startup.

#### Scenario: Successful shard registration
- **WHEN** a storage shard sends a registration payload `{"shardId": "shard-1", "address": "10.0.10.50:8081"}`
- **THEN** coordinator adds the shard to the active cluster topology and returns HTTP status 200 OK

#### Scenario: Re-registration or heartbeat update
- **WHEN** an already registered shard re-sends its registration payload with updated status
- **THEN** coordinator updates the shard's active status and acknowledges with HTTP status 200 OK

### Requirement: Consistent Hash Ring Mapping
The system SHALL hash the partition key using MD5 or MurmurHash3 and locate the primary storage shard by traversing virtual nodes placed deterministically along the hash ring.

#### Scenario: Deterministic shard lookup
- **WHEN** the coordinator evaluates a partition key against an active ring of multiple shards
- **THEN** the coordinator consistently resolves the exact same primary shard for identical partition key inputs

#### Scenario: Ring recalculation upon topology expansion
- **WHEN** a new shard registers with the coordinator
- **THEN** virtual nodes for the new shard are added to the ring, redistributing a proportional fraction of partition space without dropping existing mappings

### Requirement: Sharded Request Dispatching
The coordinator SHALL forward incoming CRUD requests (`EXISTS`, `CREATE`, `READ`, `DELETE`) to the resolved storage shard over HTTP/REST and return the shard's response to the client.

#### Scenario: Forward write operation to target shard
- **WHEN** a client submits a valid record creation request
- **THEN** coordinator resolves the partition key on the hash ring, proxies the payload to the selected shard, and returns the response

#### Scenario: Forward read operation to target shard
- **WHEN** a client queries an existing record by key
- **THEN** coordinator routes the read query to the owning shard and relays the stored record back to the client

### Requirement: Shard Cluster Availability Guard
The coordinator SHALL reject client CRUD requests with a descriptive error when no storage shards are currently registered or reachable in the ring.

#### Scenario: Client request with empty shard ring
- **WHEN** a client sends a CRUD request while zero shards are registered
- **THEN** coordinator responds with HTTP status 503 Service Unavailable indicating no shards available

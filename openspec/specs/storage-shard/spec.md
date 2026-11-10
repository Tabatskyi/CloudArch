# storage-shard Specification

## Purpose
Provides a lightweight storage node daemon that manages local table partitions and executes shard-level CRUD operations dispatched by the coordinator.

## Requirements

### Requirement: Shard Bootstrapping and Registration
The storage shard daemon SHALL contact the configured coordinator URL upon process startup to register its unique shard ID, network address, and listening port.

#### Scenario: Successful bootstrap registration
- **WHEN** the shard process starts with coordinator connectivity
- **THEN** it transmits its shard identifier and endpoint and logs successful cluster registration

#### Scenario: Coordinator bootstrap retry
- **WHEN** the coordinator is temporarily unreachable at shard launch
- **THEN** the shard retries registration with exponential backoff until connection succeeds or max retries are exceeded

### Requirement: Local Shard Record CRUD
The storage shard SHALL expose endpoints to execute `EXISTS`, `CREATE`, `READ`, and `DELETE` operations against its local table partitions.

#### Scenario: Store record in local partition
- **WHEN** coordinator issues a record write request for a given table and primary key to the shard
- **THEN** shard stores the record in its local partition storage and returns HTTP status 200 or 201

#### Scenario: Retrieve record from local partition
- **WHEN** coordinator issues a read request for an existing primary key
- **THEN** shard retrieves the record from local storage and returns HTTP status 200 with the payload

#### Scenario: Delete record from local partition
- **WHEN** coordinator issues a delete request for a primary key
- **THEN** shard removes the record from local storage and returns HTTP status 204 No Content

### Requirement: Ordered Partition Storage and Range Query
The storage shard SHALL maintain partition records using an in-memory ordered data structure (`map[string]*btree.BTree`) and execute intra-partition range queries over sort keys.

#### Scenario: Execute range scan across sort keys
- **WHEN** coordinator dispatches a range query for a partition key with sort key boundaries
- **THEN** shard iterates its ordered partition structure and returns matching records in sorted order

#### Scenario: Point lookup in compound partition
- **WHEN** coordinator requests a record with partition key and sort key
- **THEN** shard traverses the partition B-Tree and returns the exact matching record

### Requirement: Shard Health Probe
The storage shard SHALL expose an HTTP GET endpoint at `/healthz` that reports node liveness, partition record count, and operational readiness.

#### Scenario: Liveness probe verification
- **WHEN** a health probe requests `/healthz` on an active shard
- **THEN** the shard returns HTTP status 200 OK with runtime telemetry

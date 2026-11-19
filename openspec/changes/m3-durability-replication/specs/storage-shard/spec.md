# Spec Delta

## MODIFIED Requirements

### Requirement: Shard Bootstrapping and Registration
The storage shard daemon SHALL contact the configured coordinator URL upon process startup to register its unique shard ID, replica ID, assigned role (`leader` or `follower`), gRPC listening address, and HTTP health endpoint.

#### Scenario: Successful bootstrap registration
- **WHEN** the shard process starts with coordinator connectivity
- **THEN** it transmits its shard identifier, replica identifier, assigned role, gRPC address, and health endpoint, and logs successful cluster registration

#### Scenario: Coordinator bootstrap retry
- **WHEN** the coordinator is temporarily unreachable at shard launch
- **THEN** the shard retries registration with exponential backoff until connection succeeds or max retries are exceeded

### Requirement: Local Shard Record CRUD
The storage shard SHALL expose gRPC endpoints (`ShardService`) to execute `EXISTS`, `CREATE`, `READ`, and `DELETE` operations against its local table partitions, persisting and returning monotonic record versions and timestamps.

#### Scenario: Store record in local partition
- **WHEN** coordinator issues a record write request for a given table and primary key to the shard over gRPC
- **THEN** shard stores the record with assigned version and timestamp in its local partition storage and returns a success response with the persisted version

#### Scenario: Retrieve record from local partition
- **WHEN** coordinator issues a read request for an existing primary key over gRPC
- **THEN** shard retrieves the record with its version and timestamp from local storage and returns the payload

#### Scenario: Delete record from local partition
- **WHEN** coordinator issues a delete request for a primary key over gRPC
- **THEN** shard removes or tombstone-marks the record with an incremented deletion version and returns a success response

### Requirement: Shard Health Probe
The storage shard SHALL expose an HTTP GET endpoint at `/healthz` (and standard gRPC health check) reporting node liveness, replica role (`leader` or `follower`), last applied log offset, replication lag, partition record count, and operational readiness.

#### Scenario: Liveness probe verification
- **WHEN** a health probe requests `/healthz` on an active shard
- **THEN** the shard returns HTTP status 200 OK with runtime telemetry including replica role, current log offset, and record count

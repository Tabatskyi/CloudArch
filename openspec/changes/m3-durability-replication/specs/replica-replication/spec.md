# Spec Delta

## Purpose
Provides an append-only replication log, gRPC event streaming fan-out, follower offset tracking, catch-up log resynchronization, and anti-entropy verification for storage shard replica sets.

## ADDED Requirements

### Requirement: Append-Only Event Log
The leader storage shard SHALL record all write operations (`CREATE`, `DELETE`) to a local, append-only replication log with monotonically increasing offsets and version counters.

#### Scenario: Append write mutation to replication log
- **WHEN** the leader receives a valid create or delete operation
- **THEN** it appends an entry with a monotonically incremented log offset and returns the assigned offset

#### Scenario: Reject out-of-order log writes
- **WHEN** an append request carries an unexpected sequence gap or lower offset
- **THEN** the shard rejects the entry with an error indicating an offset mismatch

### Requirement: Log Streaming Fan-Out
The leader storage shard SHALL expose an internal gRPC streaming RPC (`StreamLog`) allowing follower replicas to consume committed log entries in strict FIFO order over an HTTP/2 stream.

#### Scenario: Follower consumes new log entries
- **WHEN** a follower opens a gRPC log stream requesting entries starting from offset K
- **THEN** the leader streams all committed entries from offset K in sequential order

#### Scenario: Follower poll when up-to-date
- **WHEN** a follower opens a log stream with an offset equal to the leader's current watermark
- **THEN** the stream remains open and idle until new write entries are committed by the leader

### Requirement: Follower Offset Tracking and Catch-Up Resync
Follower replicas SHALL maintain persistent watermarks of their last applied log offset and request missing log ranges from the leader via gRPC (`SyncRange`) upon node startup or reconnection.

#### Scenario: Catch-up resync after follower restart
- **WHEN** a restarted follower reconnects with applied offset N while the leader is at offset N + M
- **THEN** the follower fetches missing entries N+1 through N+M via gRPC, applies them sequentially, and transitions to synchronized state

#### Scenario: Resync when follower is already synchronized
- **WHEN** a follower reconnects with its applied offset matching the leader's current watermark
- **THEN** the follower confirms synchronized state without replaying log records

### Requirement: Write Quorum Acknowledgment
The leader storage shard SHALL require at least W - 1 follower replicas to durably persist a write entry before returning a successful commit acknowledgment to the coordinator.

#### Scenario: Successful write quorum commit
- **WHEN** leader receives a write operation requiring write quorum W = 2 and at least 1 follower acknowledges persistence
- **THEN** leader commits the entry and returns a successful response with the committed version

#### Scenario: Write quorum timeout failure
- **WHEN** fewer than W - 1 followers acknowledge the write within the configured timeout threshold
- **THEN** leader returns an RPC deadline exceeded or unavailable error indicating quorum failure

### Requirement: Anti-Entropy Divergence Detection
The system SHALL provide an anti-entropy verification RPC (`VerifyState`) comparing log offset watermarks and partition state checksums between leader and followers to detect replica divergence without full log scans.

#### Scenario: Detect synchronized replica state
- **WHEN** leader and follower exchange state checksums for matching applied offsets
- **THEN** verification reports synchronized status with matching partition state hashes

#### Scenario: Detect partition divergence
- **WHEN** state checksums differ for the same applied log offset
- **THEN** verification flags the follower as diverged and triggers partition state reconciliation

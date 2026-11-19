# ADR-0010: Log Structure and Ordering Guarantees (Append-Only WAL with Monotonic Offsets and Protobuf Envelopes)

## Status
Accepted

## Date
2026-11-19

## Context
Milestone 3 mandates durable leader-based replication across storage shard nodes. Replicating state machines require a robust storage and transmission format for mutation events. Key technical requirements include:
- **Crash Recovery & Durability**: Shards must persist incoming write operations to non-volatile disk before acknowledging commits, ensuring state can be reconstructed upon unexpected restarts.
- **Strict Monotonic Ordering**: Distributed replicas must process events in identical order to avoid state divergence (linearizable execution).
- **Data Integrity & Corruption Detection**: The format must detect bitrot, torn writes from unexpected power loss, or network corruption.
- **Serialization Efficiency**: Disk I/O and CPU overhead on `t3.micro` instances must remain bounded.

## Decision
We implement a **binary Append-Only Write-Ahead Log (WAL) with framed Protocol Buffer envelopes and CRC32 checksum validation**:

1. **Log Record Framing**:
   Each entry written to disk or transmitted over the wire adheres to a framed binary format:
   - **Magic Header (2 bytes)**: Fixed preamble `0xCA 0xFE` to detect alignment issues.
   - **Payload Length (4 bytes, Big-Endian uint32)**: Length in bytes of the serialized Protobuf payload.
   - **CRC32 Checksum (4 bytes, IEEE CRC32 uint32)**: Checksum covering the serialized Protobuf bytes.
   - **Serialized Protobuf Payload ($L$ bytes)**: The encoded `cloudarch.v1.LogEntry` message.

2. **Protobuf `LogEntry` Structure**:
   ```protobuf
   message LogEntry {
     uint64 offset = 1;       // Monotonically increasing 64-bit sequence offset
     uint64 term = 2;         // Leadership term (defaults to 1 in M3)
     int64 timestamp = 3;     // Unix nano commit timestamp
     string op_type = 4;      // "CREATE", "PUT", "DELETE"
     string table = 5;        // Target table
     string partition_key = 6;// Compound partition key
     string sort_key = 7;     // Optional sort key
     bytes value = 8;         // Raw payload
     uint32 checksum = 9;     // Payload integrity checksum
   }
   ```

3. **Ordering Guarantees**:
   - **Strict FIFO Monotonicity**: Offsets increment sequentially by 1 ($1, 2, 3, \dots$). Gaps or regressions in offset are treated as fatal protocol errors.
   - **Deterministic Idempotency**: Applying a `LogEntry` at offset $K$ multiple times yields identical in-memory B-Tree state.
   - **Sequential I/O**: Writes strictly append to the end of `/data/kv-store/replication.log` followed by `fsync` / sync writes, maximizing disk throughput on EBS `gp2` volumes by eliminating random seek overhead.

4. **Crash Recovery & Truncation**:
   - On daemon startup, the shard scans the WAL sequentially from offset 0 (or latest checkpoint), validates magic bytes and CRC32 checksums, and replays valid mutations into the in-memory storage engine.
   - If an uncommitted torn write is detected at the tail of the log (CRC32 mismatch or truncated payload), the file is truncated to the last known valid boundary.

## Rationale
1. **Performance**:
   - Sequential disk appends on EBS gp2 volumes achieve the highest possible throughput and lowest latency.
   - Protobuf binary encoding is $>60\%$ smaller and substantially faster to serialize than JSON or YAML.
2. **Robust Corruption Protection**:
   - CRC32 checksums on every frame guard against partial page writes during EC2 power cycles or kernel panics.
3. **Seamless gRPC Synergy**:
   - Because log entries are encoded as Protobuf `LogEntry` messages, the leader can read disk records and stream them directly over gRPC `StreamLog` or `SyncRange` without costly trans-coding.

## Alternatives Considered
- **JSON Lines Format (`.jsonl`)**:
  - *Rejected*: Text parsing imposes heavy CPU and allocation overhead on `t3.micro` instances, lacks built-in binary checksum framing, and increases log file footprint by $2.5\times-4\times$.
- **Embedded SQLite / BadgerDB as WAL**:
  - *Rejected*: Introduces significant write amplification (B-Tree/LSM compaction overhead) and third-party CGO or Go dependencies, whereas a dedicated append-only file offers optimal sequential performance with minimal complexity.
- **In-Memory Only Replication Buffer**:
  - *Rejected*: Violates durability requirements; a simultaneous power outage or node reboot loses all uncheckpointed state.

## Consequences
- **Positive**:
  - Fast, crash-safe sequential writes with CRC32 integrity verification.
  - Transparent recovery on restart.
  - Direct zero-copy or minimal-copy streaming over gRPC replication endpoints.
- **Negative / Trade-offs**:
  - Binary format requires tooling/CLI utilities to inspect disk log files directly.
  - Unbounded log growth requires periodic log compaction or snapshot checkpoints (addressed in recovery protocol).

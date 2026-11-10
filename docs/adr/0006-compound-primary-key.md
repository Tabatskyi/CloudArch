# ADR-0006: Partition Key and Compound Primary Key Strategy

## Status
Accepted

## Date
2026-11-10

## Context
Milestone 2 requires implementing an API and storage engine supporting compound primary keys: a mandatory **Partition Key** (hash key) and an optional **Sort Key** (range key), mirroring the data model of modern distributed databases such as Amazon DynamoDB.

Key architectural requirements and operational constraints include:
- **Routing Isolation**: The horizontal sharding mechanism must deterministically locate the target physical storage shard based strictly on the partition key, ensuring all items with the same partition key colocate on the same shard.
- **Intra-Partition Ordering & Range Scans**: Many target workloads (e.g., user event logs, IoT time-series, financial transactions) require querying records by partition key while filtering or ordering across sort keys (e.g., `WHERE user_id = 'user#42' AND timestamp >= 1700000000`).
- **Resource Constraints**: Shards run on AWS Academy sandbox instances (`t3.micro` with 1 GiB RAM). In-memory indexing structures must offer minimal pointer overhead, predictable garbage collection behavior, and bounded memory consumption.

## Decision
We adopt a **Two-Tier Compound Key Storage Architecture**:
1. **Routing Tier**: The Coordinator hashes *strictly* the `PartitionKey` using the consistent hashing ring. Sort keys are never included in the ring hash.
2. **Shard Storage Tier**: Within each storage shard, tables are organized as a partition directory mapping each partition key to an in-memory ordered data structure: **`map[string]*btree.BTree`** (or Ordered SkipList).
3. **Compound Key Identity**: Individual records within a partition are uniquely identified by their `SortKey`. Point lookups use the tuple `(PartitionKey, SortKey)`, while point operations without a sort key use an empty string or default sentinel.

## Rationale
1. **Efficient Intra-Partition Range Queries**:
   - Storing partition items inside a B-Tree / Ordered SkipList provides $O(\log M + K)$ range queries, where $M$ is the number of records in the partition and $K$ is the number of returned records.
   - Enables range operators (`=`, `>`, `>=`, `<`, `<=`, `BETWEEN`, and prefix scans) without sorting on the fly.
2. **Strict Colocation Without Scatter-Gather**:
   - Because the hash ring evaluates only `PartitionKey`, all records belonging to a partition key reside on the same shard node.
   - Range scans are fulfilled entirely by a single target shard, eliminating costly cross-shard network scatter-gather queries.
3. **Cache Locality and Memory Efficiency**:
   - In-memory B-Trees group multiple keys within contiguous node blocks, offering significantly superior CPU cache line utilization and reduced pointer overhead compared to binary search trees (e.g. Red-Black trees).
   - Keeps memory consumption well within `t3.micro` limits even with tens of thousands of records.

## Alternatives Considered
- **Flat Hash Table with Delimited Composite Keys (`map[string][]byte` with `pk#sk`)**:
  - *Pros*: Extremely simple; $O(1)$ point lookups.
  - *Cons*: Fails range queries. Querying a range of sort keys requires either a full table scan ($O(N)$ across all keys) or building complex external secondary indices.
- **Two-Level Nested Hash Map (`map[string]map[string][]byte`)**:
  - *Pros*: $O(1)$ point lookups and straightforward deletion.
  - *Cons*: Sort keys are unordered. Range queries require collecting all keys in the partition and sorting them in memory on every request ($O(M \log M)$), causing CPU spikes and GC pressure.
- **Ordered SkipList**:
  - *Pros*: Probabilistic balance, simpler concurrent lock-free implementations.
  - *Trade-off*: Higher memory pointer overhead per node compared to B-Trees. Retained as an acceptable alternative if third-party B-Tree libraries are restricted.

## Consequences
- **Positive**:
  - Full support for DynamoDB-style partition + sort key query semantics.
  - High-performance point reads ($O(\log M)$) and intra-partition range queries ($O(\log M + K)$).
  - Clean separation of concerns: horizontal scalability handled by hash ring, intra-partition ordering handled by B-Tree.
- **Negative / Trade-offs**:
  - Slightly higher insertion latency than a raw hash map ($O(\log M)$ vs $O(1)$) due to tree balancing.
  - Requires maintaining an ordered tree structure per active partition key.

# ADR-0007: Hash Function and Virtual Node Strategy for Consistent Hashing

## Status
Accepted

## Date
2026-11-10

## Context
Milestone 2 mandates horizontal sharding of key-value records across downstream storage nodes (starting with 2 shards in M2, expanding to 3+ replicated shards in M3, and dynamic rebalancing in M5). The system coordinator must deterministically map incoming partition keys to storage shards.

Key design constraints and requirements include:
- **Uniform Load Distribution**: The partition key space must be evenly dispersed across active shards. High variance leads to hotspotting, where one node exhausts memory or IOPS while another remains idle.
- **Minimal Request Routing Overhead**: Hash ring lookups occur on every single client read/write operation at the Coordinator. Hashing and ring traversal must execute in sub-microsecond time with minimal CPU and memory allocations.
- **Dynamic Topology Resilience**: Adding or removing a storage shard should only shift a minimal fraction of keys ($1/N$), preserving data locality without requiring full cluster redistribution.
- **Constrained Hardware Profile**: The cluster runs on AWS Academy sandbox `t3.micro` instances (2 vCPUs, 1 GiB RAM).

## Decision
1. **Hash Algorithm**: We adopt **MurmurHash3 (32-bit integer variant, with standard fallback to MD5/FNV-1a)** for hashing partition keys and virtual node identifiers onto a 32-bit unsigned integer ring space $[0, 2^{32}-1]$.
2. **Virtual Node Factor ($V$)**: We assign **150 to 256 virtual nodes** per physical storage shard.
3. **Token Identification**: Virtual node tokens are deterministically named `<shardId>#vn<index>` (e.g. `shard-1#vn0`, `shard-1#vn1`, ... `shard-1#vn199`).
4. **Ring Search**: The coordinator maintains an in-memory sorted slice of 32-bit token hashes. Point lookup computes $\text{hash}(partitionKey)$ and performs a binary search (`sort.Search`) for the smallest token hash $\ge \text{hash}(partitionKey)$, wrapping around to index 0 at the ring boundary.

## Rationale
1. **High Throughput and Low Latency**:
   - MurmurHash3 is a non-cryptographic hash function optimized for speed, processing several gigabytes of data per second per CPU core with excellent bit avalanche characteristics.
   - Lookups complete in $O(\log(N \times V))$ time using Go's standard binary search. For 2 shards with 200 virtual nodes each, the ring has 400 entries, requiring at most $\approx 9$ comparisons (< 300 nanoseconds).
2. **Statistically Uniform Variance Reduction**:
   - In consistent hashing without virtual nodes ($V=1$), two physical nodes typically exhibit load variance exceeding 30–40%.
   - Increasing the virtual node count to $V \in [150, 256]$ dampens the standard deviation of partition distribution to below 5% ($\sigma / \mu < 0.05$), ensuring both shards share an equal load during YCSB benchmarks.
3. **Negligible Memory Footprint**:
   - Storing 400 to 1,000 32-bit integers and their string node mappings consumes less than 20 KB of RAM, negligible for the 1 GiB memory limit on `t3.micro`.
4. **Smooth Incremental Scaling**:
   - When a new physical shard joins the ring, it claims virtual node positions evenly across the circumference, taking approximately $1/(N+1)$ load from existing nodes rather than invalidating the entire key space.

## Alternatives Considered
- **Modulo Hashing (`hash(key) % N`)**:
  - *Rejected*: Adding or removing a shard node invalidates $(N-1)/N$ of all keys, requiring near-complete data movement across the cluster and breaking dynamic scale-out.
- **Cryptographic Hashes (SHA-256 / SHA-1)**:
  - *Rejected*: While distribution is uniform, SHA-256 requires substantial CPU compute and creates unnecessary latency overhead on compute-limited `t3.micro` instances without providing any security advantage for internal ring routing.
- **Rendezvous Hashing (Highest Random Weight)**:
  - *Rejected*: Requires computing $N$ hashes for every lookup ($O(N)$), whereas consistent hashing with binary search resolves in $O(\log(N \times V))$.
- **Low Virtual Node Count ($V \le 20$)**:
  - *Rejected*: Benchmark simulations demonstrate that low virtual node counts create severe load imbalance on small clusters (e.g. 70/30 split between two shards).

## Consequences
- **Positive**:
  - Sub-microsecond partition key routing with zero heap allocations during lookups.
  - Balanced distribution across downstream shards during YCSB benchmark runs.
  - Straightforward deterministic testing in Go unit test suites.
- **Negative / Trade-offs**:
  - Rebuilding or updating the sorted slice of virtual nodes requires an $O(M \log M)$ sort operation whenever a shard registers or deregisters (infrequent event).
  - Shards must handle virtual node assignments during future data rebalancing (Milestone 5).

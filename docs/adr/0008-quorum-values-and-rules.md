# ADR-0008: Quorum Values and Rules ($N=3, W=2, R=2$)

## Status
Accepted

## Date
2026-11-19

## Context
In Milestone 2, each shard operated as a single, isolated node without replication. A failure of any shard node resulted in immediate data unavailability and loss for all keys mapped to that shard's partitions. Milestone 3 introduces durability and high availability through replica sets.

To ensure consistency and fault tolerance in an asynchronous or semi-synchronous distributed environment, the system must establish concrete quorum configuration values ($N$, $W$, $R$) and enforce the strict quorum overlap invariant:
$$R + W > N$$

Key requirements and constraints include:
- **Fault Tolerance**: Surviving at least one replica node crash per shard group ($F=1$) without losing durability or service availability.
- **Consistency Guarantees**: Eliminating stale reads by ensuring that any read quorum intersects with the most recent write quorum by at least one replica node holding the latest version.
- **Low Write Latency**: Minimizing round-trip latency on write operations under AWS Academy sandbox hardware constraints (`t3.micro` instances with 2 vCPUs and 1 GiB RAM).
- **Resource Constraints**: Total compute footprint must remain within AWS sandbox budget ceilings ($100 budget, 32 vCPU limit). 2 shard groups $\times$ 3 replicas + 1 coordinator = 7 instances (14 vCPUs).

## Decision
1. **Replication Factor ($N = 3$)**:
   - Each shard partition key range is replicated across exactly $N = 3$ storage nodes (1 designated leader and 2 followers).
2. **Write Quorum ($W = 2$)**:
   - A write mutation (`CREATE`, `PUT`, `DELETE`) is considered committed only after it is durably persisted by the leader and acknowledged by at least $W - 1 = 1$ follower replica (total 2 nodes).
3. **Read Quorum ($R = 2$)**:
   - The coordinator executes client read operations (`GET`, `EXISTS`, `query`) across $R = 2$ active replicas in parallel via gRPC.
   - The coordinator evaluates the returned record envelopes, resolves the newest record version based on the monotonic `Version` attribute, and discards stale versions.
4. **Quorum Overlap Invariant**:
   - $R + W = 2 + 2 = 4 > 3 (N)$.
   - By the Pigeonhole Principle, any subset of $R=2$ nodes and any subset of $W=2$ nodes drawn from a replica set of size $N=3$ must overlap by at least $(R + W) - N = 1$ node.
   - The overlapping node is guaranteed to have participated in the latest acknowledged write and therefore possesses the most up-to-date version.
5. **Availability Guard Thresholds**:
   - If fewer than $W=2$ replicas are reachable for a write, the coordinator immediately rejects the request with HTTP 503 (Write Quorum Unavailable).
   - If fewer than $R=2$ replicas are reachable for a read, the coordinator rejects the request with HTTP 503 (Read Quorum Unavailable).

## Rationale
1. **Strict Stale-Read Elimination**:
   - The condition $R + W > N$ mathematically guarantees strong consistency (linearizability or sequential consistency per partition) under non-Byzantine crash-recovery failures, provided version monotonicity is enforced.
2. **Minimal Tail Latency ($p99$) on Writes**:
   - In an $N=3, W=2$ configuration with a single sequencing leader, the leader only waits for the fastest follower to acknowledge before returning success to the coordinator. The write is unaffected by the slowest follower experiencing a transient GC pause or network blip.
3. **Balanced Read Load Distribution**:
   - Because $R=2 < N=3$, the coordinator does not need to query all nodes. By rotating the chosen pair of replicas across all three members (leader, follower 1, follower 2) using round-robin or least-loaded selection, read traffic is balanced across all three nodes.
4. **AWS Academy Sandbox Optimization**:
   - An $N=3$ cluster achieves $F=1$ fault tolerance with the minimal number of nodes. 2 shard groups require 6 instances, which fits comfortably within the 14 vCPU budget and <$20 projected cloud spend.

## Alternatives Considered
- **$N=3, W=1, R=3$ (Asynchronous Replication)**:
  - *Rejected*: While write latency is minimal (leader commits locally), followers may lag arbitrarily. A leader crash before asynchronous replication causes silent permanent data loss. Additionally, reads must query all 3 replicas, increasing read latency and vulnerability to any single slow node.
- **$N=3, W=3, R=1$ (Synchronous All-Ack Replication)**:
  - *Rejected*: Allows single-node reads, but provides zero write fault tolerance ($F=0$ for writes). If even a single follower restarts or undergoes maintenance, all write operations to that partition block.
- **$N=5, W=3, R=3$ ($F=2$ Fault Tolerance)**:
  - *Rejected*: Requires 10 shard instances + 1 coordinator = 11 instances (22 vCPUs), which approaches the AWS Academy sandbox limit and substantially increases cross-node network traffic on constrained `t3.micro` instances.

## Consequences
- **Positive**:
  - High availability: The cluster survives any single node failure per shard group without interruption to read or write traffic.
  - Strong durability: Committed writes survive single-node crashes.
  - Consistent reads: Stale data is filtered out at the Coordinator through version comparison.
- **Negative / Trade-offs**:
  - Network overhead: Write operations require internal gRPC replication round-trips before committing ($W=2$).
  - Read overhead: Quorum reads require 2 parallel gRPC calls and version reconciliation rather than a single direct read.
  - Multi-node failure: If 2 of 3 replicas in a shard group become partitioned simultaneously, operations on that shard group are rejected until quorum is restored.

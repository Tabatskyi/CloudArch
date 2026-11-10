# ADR-0004: Choice of Core Programming Language

## Status
Accepted

## Date
2026-11-03

## Context
Implementing a high-performance, distributed key-value store across Milestones 2–5 demands a programming language capable of handling:
- High-concurrency network I/O and low-latency HTTP/gRPC request routing (Coordinator node in M2).
- Dynamic consistent hashing ring lookups and partition mapping.
- Replication log streaming, quorum read/write synchronization ($R + W > N$), and heartbeat gossiping (M3).
- Structured telemetry, health actuators, and operational MCP server tooling (M4).
- Shard rebalancing data migration background jobs (M5).
- High-throughput YCSB benchmark workloads where tail latency ($p99$) is strictly evaluated.

Furthermore, all services will run on AWS Academy sandbox EC2 instances with tight compute and memory constraints (typically `t3.micro` with 1 GB RAM or `t3.small` with 2 GB RAM).

## Decision
We adopt **Go (Golang)** as the primary implementation language for the key-value store's coordinator service, storage shard daemons, replication log processors, and CLI verification utilities.

## Rationale
1. **First-Class Concurrency Model**: Go's lightweight goroutines (starting at ~2 KB stack size) and channels enable thousands of concurrent client connections, asynchronous replication pipelines, and non-blocking background workers with minimal memory overhead.
2. **Static Single-Binary Deployment**: Go compiles directly into self-contained machine code. A deployment artifact is a single static binary that can be copied directly to an EC2 instance or packaged in a `scratch`/`alpine` container without needing external runtimes, virtual environments, or package managers.
3. **Predictable Low Tail Latency**: Unlike Java/JVM platforms with complex garbage collection pauses that degrade $p99$ latencies, Go's modern concurrent GC maintains sub-millisecond pause times, ensuring consistent performance during YCSB benchmarks.
4. **Lean Memory Footprint**: Go services routinely idle at under 15–30 MB of RAM, leaving maximum memory available for in-memory key-value caching on memory-constrained sandbox instances.
5. **Mature Networking & Cloud SDKs**: Go provides an exceptionally fast standard `net/http` package, native JSON serialization, and official support from the AWS SDK for Go v2.

## Alternatives Considered
- **Python**:
  - *Rejected*: The Global Interpreter Lock (GIL) limits multi-threaded CPU utilization. High memory overhead per process and slower execution speed lead to severe throughput penalties in YCSB benchmarks.
- **Rust**:
  - *Rejected*: While offering superior raw performance and memory safety, Rust's steep learning curve, strict borrow checker ergonomics, and longer compilation times create unnecessary delivery friction against tight milestone deadlines.
- **Java / Kotlin**:
  - *Rejected*: The JVM runtime typically demands hundreds of megabytes of baseline memory, risking Out-Of-Memory (OOM) crashes on 1 GB `t3.micro` instances.

## Consequences
- **Positive**:
  - Fast iteration cycle with rapid compilation and built-in testing (`go test`).
  - Painless deployment to AWS EC2 via SCP/SSH or minimal container images.
  - High concurrency throughput out of the box.
- **Negative / Trade-offs**:
  - Explicit error handling boilerplate (`if err != nil`).
  - Developers must be disciplined with goroutine lifecycles and context cancellation to prevent goroutine leaks.


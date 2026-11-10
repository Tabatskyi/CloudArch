# ADR-0005: API Protocol and Format

## Status
Accepted

## Date
2026-11-10

## Context
Milestone 2 mandates an API specification for table registration and CRUD operations (`EXISTS`, `CREATE`, `READ`, `DELETE`) over table records, with support for compound primary keys (partition key + optional sort key). The API must:
- Serve external client requests and enable standard benchmarking with YCSB (Yahoo! Cloud Serving Benchmark).
- Seamlessly integrate into an AI-agent-facing Model Context Protocol (MCP) server in Milestone 4.
- Support low-latency request routing and dispatch between the Coordinator node and downstream horizontal storage shards.
- Run predictably within compute- and memory-constrained AWS Academy sandbox EC2 instances (`t3.micro` with 1 GB RAM / `t3.small` with 2 GB RAM).

## Decision
We expose an **OpenAPI 3.0 RESTful API (JSON over HTTP/1.1 and HTTP/2)** for external clients and Coordinator ingress, paired with lightweight HTTP/REST (or internal gRPC) for coordinator-to-shard dispatch.

## Rationale
1. **Interoperability**: Native compatibility with YCSB HTTP workload drivers without writing custom Java/C++ bindings or proprietary client drivers.
2. **Tooling Alignment**: Seamless integration with MCP (Model Context Protocol) tool calling and JSON Schema schemas required in Milestone 4.
3. **Simplicity & Predictability**: Go's standard `net/http` package and lightweight routing libraries enable zero-dependency, transparent endpoint creation with predictable memory profiles and minimal footprint on memory-constrained instances.
4. **Standardized Contract**: OpenAPI 3.0 provides an unambiguous, machine-readable specification allowing straightforward contract testing, mock generation, and clear developer documentation.

## Alternatives Considered
- **GraphQL**:
  - *Rejected*: Excessive parsing and execution overhead for single-key hash lookups; high barrier for standard benchmarking tools like YCSB.
- **Pure gRPC (Client-facing)**:
  - *Rejected*: Introduces high friction for browser/CLI debugging (`curl`) and requires custom client scaffolding inside standard YCSB harnesses. Internal coordinator-to-shard dispatch can remain lightweight HTTP/REST or gRPC, but client-facing ingress standardizes on HTTP/JSON.

## Consequences
- **Positive**:
  - Standard HTTP tooling (`curl`, Postman, standard HTTP benchmark drivers) works out-of-the-box.
  - Smooth integration with YCSB HTTP binding for baseline latency and throughput measurements.
  - Direct 1:1 mapping to MCP tools in Milestone 4 with standard JSON schemas.
- **Negative / Trade-offs**:
  - Higher serialization payload overhead compared to binary protocols such as Protocol Buffers.
  - Text-based JSON parsing CPU cost, though negligible for key-value payloads in Go.

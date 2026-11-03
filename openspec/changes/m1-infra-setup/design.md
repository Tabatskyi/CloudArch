# Design

## Context

See `proposal.md` for motivation and scope. The infrastructure must operate within the AWS Academy learner sandbox. The sandbox environment imposes strict boundary policies:
- Pre-provisioned credentials run under the `LabRole` IAM role.
- Creating or attaching new IAM policies, roles, or users is denied by the sandbox boundary policy.
- Sandbox credits are capped (typically $100 per course) without credit card backing.
- Resource creation is restricted to supported regions (primarily `us-east-1` or `us-west-2`) and standard compute types (`t2.micro`, `t3.micro`, `t3.small`).

This design establishes a reproducible network, security baseline, quota/budget review, and ADR foundation required prior to M2.

## Goals / Non-Goals

**Goals:**
- Provide modular, declarative Infrastructure-as-Code (IaC) that provisions a multi-AZ VPC, public/private subnets, route tables, and an Internet Gateway.
- Architect clean separation between persistent network infrastructure and ephemeral application stacks.
- Define a strict CIDR allocation scheme supporting multi-AZ placement with headroom for horizontal sharding.
- Model least-privilege IAM artifacts (view-only policy, assumable role) in IaC and systematically analyze `LabRole` permissions and limitations.
- Provide M2 capacity planning, service quota checks, and cost estimations against sandbox ceilings.
- Demonstrate and document stack recreatability with teardown/redeploy benchmarks.
- Document mandatory ADRs (IaC tool selection, stack decoupling, CIDR scheme).

**Non-Goals:**
- Deploying M2 coordinator or storage shard compute instances (deferred to Milestone 2).
- Setting up NAT Gateways (cost optimization: NAT Gateways incur ~$32/month per AZ, which would consume the sandbox budget; public subnets with direct IGW or free endpoints are preferred for initial egress).
- Production peering, VPN tunnels, or Transit Gateways.

## Decisions

### 1. Choice of IaC Tool
- **Decision**: Use **OpenTofu** (v1.x with AWS Provider) for infrastructure provisioning.
- **Rationale**:
  - Open-source, community-driven fork of Terraform governed by the Linux Foundation under the Mozilla Public License 2.0 (MPL-2.0), avoiding HashiCorp BSL licensing restrictions.
  - 100% drop-in compatibility with existing Terraform AWS Provider resources and state formats.
  - Declarative HCL syntax with robust modular composition and precise local execution plans (`tofu plan`) prior to apply.
  - Strong ecosystem integration for verification, linting, and automated state management.
- **Alternatives Considered**:
  - *HashiCorp Terraform*: Rejected due to Business Source License (BSL 1.1) ambiguity and community fragmentation.
  - *AWS CloudFormation*: Native AWS service requiring no external state file management. Rejected due to verbose YAML/JSON templates, slower feedback loops, and less intuitive local diffing compared to `tofu plan`.
  - *AWS CDK*: Imperative programming language abstraction. Rejected to avoid unnecessary language runtime dependencies (Node.js/Python SDK compilation) for baseline VPC resources.

### 2. Stack Layout (Decoupled Layered Architecture)
- **Decision**: Separate the infrastructure into distinct stacks/modules:
  - `infra/network`: Manages VPC, subnets, route tables, and Internet Gateway.
  - `infra/iam`: Defines view-only policies and assumable roles (evaluated in IaC).
  - `infra/app` (Milestone 2+): Manages coordinator, shard nodes, and security groups.
- **Rationale**:
  - Network components are low-churn, stateful foundational resources. Decoupling them prevents accidental destruction during application-tier iterations.
  - Allows independent lifecycle operations and isolated state files.
- **Alternatives Considered**:
  - *Monolithic single stack*: Combining network and compute in one root module. Rejected because modifying application resources risks accidental drift or destruction of networking foundations.

### 3. CIDR Addressing Scheme
- **Decision**: Allocate a `/16` IPv4 block (`10.0.0.0/16`, 65,536 addresses) partitioned into `/24` subnets (256 addresses each, 251 usable in AWS):
  - **VPC**: `10.0.0.0/16`
  - **Public Subnet AZ-A (`us-east-1a`)**: `10.0.1.0/24`
  - **Public Subnet AZ-B (`us-east-1b`)**: `10.0.2.0/24`
  - **Private Subnet AZ-A (`us-east-1a`)**: `10.0.10.0/24` (coordinator & shard nodes)
  - **Private Subnet AZ-B (`us-east-1b`)**: `10.0.11.0/24` (shard replicas)
  - **Reserved / Future**: `10.0.20.0/24` - `10.0.255.0/24` reserved for future shards, caches, or VPC endpoints.
- **Rationale**:
  - `/24` allocations provide ample IP space for distributed nodes and dynamic scaling while keeping subnet boundaries clean and human-readable.
  - Even/odd or distinct block ranges (`1-2` for public, `10-11` for private) prevent routing misconfigurations.
- **Alternatives Considered**:
  - *Tight `/28` subnets*: Minimizes IP waste, but restricts future scaling and container/pod IP allocation.
  - *Single public subnet*: Fails multi-AZ high-availability requirements.

### 4. IAM Security Modeling & Sandbox Handling
- **Decision**:
  - Write standard IaC definitions for a read-only view policy (`ReadOnlyResourceViewPolicy`) and an assumable role (`AuditorRole`).
  - Document the exact permission refusal behavior in the AWS Academy sandbox (explicit `AccessDenied` when attempting `iam:CreateRole` / `iam:CreatePolicy`).
  - Dissect the sandbox `LabRole` using AWS CLI queries (`aws iam get-role`, `aws iam get-policy`) and the IAM policy simulator to catalog effective permissions.
- **Rationale**: Meets learning requirements SEC-1 and SEC-2 while respecting sandbox boundary constraints.

### 5. Quota & Cost Forecasting Methodology
- **Decision**:
  - Service Quota Review: Inspect EC2 vCPU quotas (`L-1216C47A`), VPC limits, and Elastic IP limits via AWS CLI `aws service-quotas list-service-quotas`.
  - M2 Budget Model: Calculate monthly run-rate for 1 Coordinator + 2 Shard instances (`t3.micro` or `t3.small` On-Demand) + 3x 20GB gp3 EBS volumes + estimated network egress. Compare total monthly cost against the $100 sandbox budget.

### 6. Choice of Core Programming Language (Go / Golang)
- **Decision**: Use **Go (Golang)** as the standard implementation language for the key-value store system (Coordinator node, horizontal storage shards, consensus/replication in M3, and observability hooks in M4) as well as operational testing.
- **Rationale**:
  - Built-in lightweight concurrency primitives (goroutines and channels) ideally suited for asynchronous request routing, fan-out event-sourcing, and consistent hash ring lookups.
  - Compiles to static standalone binaries with zero external runtime dependencies, dramatically simplifying EC2 boot scripts and container packaging on lean sandbox instances.
  - Predictable memory footprint and low garbage collection pause times, which are essential for maintaining stable tail latencies (p99) during YCSB benchmarks.
  - Rich and mature standard library for HTTP/gRPC networking, JSON handling, and official AWS SDK v2 support.
- **Alternatives Considered**:
  - *Python*: Rejected for performance-critical coordinator routing and storage engine due to Global Interpreter Lock (GIL) concurrency bottlenecks, higher memory footprint, and runtime dependency packaging friction.
  - *Rust*: Excellent performance and memory safety, but rejected due to steeper learning curve and slower compilation/prototyping velocity within fast-paced milestone deadlines.
  - *Java/Kotlin*: Enterprise standard, but rejected due to heavy JVM memory overhead and long JVM warmup times that strain small sandbox EC2 instance types (`t3.micro`/`t3.small`).

## Risks / Trade-offs

- **[Risk] Sandbox IAM restrictions block role deployment** → *Mitigation*: Deliver the IAM templates in IaC, validate syntax with `tofu validate` / linter, and document the intentional sandbox refusal in defense materials.
- **[Risk] High cost of NAT Gateways depleting sandbox credit** → *Mitigation*: Avoid provisioning AWS NAT Gateways ($0.045/hr + data processing charges ~$32/month per AZ). For M1, private subnets remain purely isolated; any future outbound updates can utilize VPC endpoints or temporary bastion routing.
- **[Risk] Residual resources during teardown cause recreation failure** → *Mitigation*: Structure IaC with explicit resource dependencies; verify `tofu destroy` removes all dependencies (Internet Gateway attachments, route tables, subnets) cleanly.

## Recreatability & Lifecycle Plan

1. **Deployment (`tofu init && tofu apply`)**:
   - Provisions VPC, subnets, route tables, internet gateway, and associations.
   - Measures time from initial command execution to resource readiness.
2. **Verification**:
   - Query AWS API to confirm subnet CIDRs, routing rules, and multi-AZ placement.
   - Validate syntax and dry-run IAM definitions.
3. **Teardown (`tofu destroy`)**:
   - Destroys all networking resources.
   - Measures teardown latency and confirms zero lingering orphaned network interfaces or routes.
4. **Recreation**:
   - Re-applies the configuration to prove 100% automated reproducibility.
   - Records timings and any edge-case failures.


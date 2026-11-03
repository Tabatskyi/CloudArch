# ADR-0002: Stack Layout and Decoupling Strategy

## Status
Accepted

## Date
2026-11-03

## Context
As we build out the distributed key-value store across five milestones, infrastructure resources exhibit vastly different change frequencies, operational risk profiles, and lifecycles:
- **Networking Foundation (VPC, Subnets, Gateways, Route Tables)**: High stability, low churn, foundational dependency for all services. Accidental modification or destruction breaks all connectivity and invalidates node IPs.
- **Security Baseline (IAM Policies & Roles)**: Governed by sandbox boundaries; defined declaratively for compliance audits.
- **Application & Storage Tier (Coordinator, Shards, Replicas, ASGs)**: High churn, frequent code deployments, dynamic scaling, and experimentation.

We must decide whether to bundle all infrastructure into a single monolithic stack or decouple them into layered stacks with isolated state management.

## Decision
We adopt a **Decoupled Layered Stack Architecture** structured into independent OpenTofu modules with isolated state boundaries:
1. `infra/network`: Provisions the VPC, multi-AZ public and private subnets, route tables, and Internet Gateway.
2. `infra/iam`: Contains declarative IAM policies and role templates for auditing and least-privilege verification.
3. `infra/app` (Milestones 2–5): Manages EC2 instances, EBS storage volumes, security groups, and coordinator routing.

Downstream application stacks consume networking references through standardized OpenTofu stack outputs and AWS data sources (filtering by VPC tags and subnet tiers).

## Rationale
1. **Minimized Blast Radius**: An issue or unintended destroy command during application iteration cannot accidentally tear down the underlying VPC or disrupt existing network allocations.
2. **Lifecycle and Churn Alignment**: High-velocity compute resources are isolated from slow-moving, foundational network primitives.
3. **Faster Execution Cycles**: Running `tofu plan` or `tofu apply` in the application layer only refreshes compute resources without querying and checking every subnet and route table.
4. **Clean Operational Teardown**: Allows tearing down and recreating ephemeral compute nodes without needing to recreate the VPC, while still supporting full end-to-end teardowns for recreatability proofs.

## Alternatives Considered
- **Monolithic Single Stack**: Managing VPC, subnets, compute instances, and security groups in a single root module.
  - *Rejected*: Modifying compute instances or security rules would continually risk network recreation or state locking issues.
- **Fine-Grained Micro-Stacks (Per-Resource)**: Creating separate state stacks for VPC, subnets, route tables, and instances individually.
  - *Rejected*: Introduces unnecessary complexity and excessive cross-state dependency wiring for a project of this scale.

## Consequences
- **Positive**:
  - Independent failure domains and state files.
  - Clean separation of concerns between network topology and database runtime infrastructure.
  - Reusable module interfaces via clear input variables and output values.
- **Negative / Trade-offs**:
  - Requires maintaining outputs in `infra/network/outputs.tf` that downstream stacks must read via variables or data lookups.
  - Teardown requires executing `tofu destroy` in reverse dependency order (`infra/app` before `infra/network`).


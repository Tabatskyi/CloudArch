# ADR-0001: Choice of Infrastructure as Code (IaC) Tool

## Status
Accepted

## Date
2026-11-03

## Context
The capstone project requires provisioning and managing AWS cloud infrastructure within the AWS Academy learner sandbox across five evolutionary milestones (networking baseline, horizontal sharding, replication, observability, and autoscaling). 

Key constraints and requirements include:
- Must run reliably inside the AWS Academy sandbox environment under `LabRole`.
- Must provide declarative, deterministic execution plans (`plan`/`apply`) to prevent accidental destruction or drift.
- Must support clean modular decoupling (e.g., separating networking stacks from application workloads).
- Must adhere to open-source software best practices without restrictive proprietary licensing.

## Decision
We adopt **OpenTofu** with the AWS Provider as the standard Infrastructure as Code (IaC) tool for all milestones.

## Rationale
1. **Open Source & Permissive Licensing**: OpenTofu is a community-driven, Linux Foundation project licensed under the Mozilla Public License 2.0 (MPL-2.0). It ensures long-term open access without the commercial limitations or ambiguities imposed by HashiCorp's Business Source License (BSL 1.1). 
2. **Provider & Ecosystem Parity**: OpenTofu maintains 100% drop-in compatibility with the Terraform AWS Provider, HCL syntax, and existing module structures, allowing us to leverage battle-tested AWS resource specifications.
3. **Deterministic Planning**: OpenTofu provides dry-run execution diffs (`tofu plan`) before any cloud resource mutation occurs, critical for verifying changes within the capped sandbox budget.
4. **Modular Architecture & State Isolation**: OpenTofu supports clear separation of concerns across directories (`infra/network`, `infra/iam`, `infra/app`), enabling independent state files and minimizing blast radius.

## Alternatives Considered
- **HashiCorp Terraform**: Rejected due to the shift from MPL to the restrictive BSL 1.1 license, creating vendor-lock concerns.
- **AWS CloudFormation**: While natively supported without external state backends, CloudFormation was rejected due to verbose JSON/YAML authoring, slower change set feedback loops, and less intuitive local diffing compared to HCL.
- **AWS CDK**: Synthesizes CloudFormation templates from imperative languages (TypeScript/Python). Rejected because it adds heavy language runtime dependencies and complex build chains for static VPC and IAM declarations.

## Consequences
- **Positive**:
  - Transparent, community-governed, freely redistributable IaC manifests.
  - Fast local development cycle with `tofu fmt`, `tofu validate`, and `tofu plan`.
  - Easy state inspection and drift detection.
- **Negative / Trade-offs**:
  - Requires the `tofu` CLI binary to be installed on developer workstations.
  - State files must be preserved locally or in sandbox-compliant S3 buckets.


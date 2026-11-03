# Proposal

## Why

Establish the foundational cloud infrastructure and access control baseline for the distributed key-value store in the AWS Academy sandbox environment. A reproducible, secure networking and IAM foundation is required prior to deploying coordinator and storage shard components in Milestone 2.

## What Changes

- Define and provision a multi-AZ VPC network architecture featuring segregated public subnets (with Internet Gateway for inbound/outbound edge connectivity) and private subnets (isolated for internal storage shards and coordinator nodes).
- Define Infrastructure-as-Code (IaC) configuration using OpenTofu to manage networking resources with full lifecycle reproducibility.
- Formulate a least-privilege, view-only IAM policy and an assumable cross-principal role modeled in IaC, documenting sandbox permission boundaries.
- Provide an audit and dissection of the AWS Academy `LabRole` permissions boundary (allowed, denied, and verification methods).
- Review AWS service quotas and calculate monthly budget estimates for Milestone 2 against the sandbox credit ceiling.
- Validate stack recreatability through teardown and redeployment testing with OpenTofu.
- Author Architectural Decision Records (ADRs) for OpenTofu IaC tool selection, Go (Golang) as the core distributed store programming language, stack layout, and CIDR addressing scheme.

## Capabilities

### New Capabilities
- `cloud-network`: VPC network topology providing isolated public and private subnets across multiple Availability Zones, Internet Gateway routing, and CIDR allocation for distributed store nodes.
- `iam-security`: Least-privilege access control definitions, including read-only resource inspection policies, assumable IAM role templates, and AWS Academy sandbox LabRole permission boundary constraints.

### Modified Capabilities
*(None — greenfield project)*

## Impact

- Establishes root infrastructure directories and OpenTofu manifests (in `infra/tofu/` or `infra/`).
- Provisions AWS networking primitives (VPC, Subnets, Route Tables, Internet Gateway) that will host M2 compute nodes.
- Establishes ADR directory structure (`docs/adr/`) and foundational records (OpenTofu IaC selection, Go language choice, stack layout, CIDR scheme).
- Documents sandbox operational constraints (quotas, LabRole limits, credit ceilings) preventing deployment failures in later milestones.


# Tasks

## 1. Architectural Decisions (ADRs)

- [ ] 1.1 Author ADR-001 on the choice of IaC tool (OpenTofu vs Terraform vs CloudFormation/CDK) and verify file presence in `docs/adr/0001-choice-of-iac-tool.md`
- [ ] 1.2 Author ADR-002 on stack layout and decoupling (separating network foundation from application tier) and verify file presence in `docs/adr/0002-stack-layout.md`
- [ ] 1.3 Author ADR-003 on the CIDR addressing scheme (10.0.0.0/16 partitioned into /24 public and private subnets across 2 AZs) and verify file presence in `docs/adr/0003-cidr-addressing-scheme.md`
- [ ] 1.4 Author ADR-004 on the choice of core programming language (Go / Golang for coordinator, shards, and distributed primitives) and verify file presence in `docs/adr/0004-programming-language-golang.md`

## 2. IAM Baseline and Sandbox Boundary Analysis

- [ ] 2.1 Audit and dissect the AWS Academy `LabRole` permissions boundary using AWS CLI queries, AWS Console, and Policy Simulator, recording allowed/denied permissions and verification steps in `docs/security/labrole-analysis.md`
- [ ] 2.2 Author the read-only view IAM policy template in IaC (`infra/iam/policy.tf`) allowing only resource inspection actions (Describe, Get, List) without modification rights, verifying syntax with `tofu validate`
- [ ] 2.3 Author the assumable IAM role definition in IaC (`infra/iam/role.tf`) with sts:AssumeRole trust relationships, verifying template validity with `tofu validate`
- [ ] 2.4 Document the AWS Academy sandbox IAM refusal behavior and error response semantics (`AccessDenied` under `LabRole` boundary) in `docs/security/iam-sandbox-refusal.md`

## 3. Network Infrastructure IaC Stack

- [ ] 3.1 Implement OpenTofu root and network modules (`infra/network/`) defining the VPC (`10.0.0.0/16`) with DNS support enabled, verifying syntax with `tofu fmt -check` and `tofu validate`
- [ ] 3.2 Define public subnets in two Availability Zones (`10.0.1.0/24`, `10.0.2.0/24`) attached to an Internet Gateway via dedicated public route tables, verifying route configurations in OpenTofu plan
- [ ] 3.3 Define private subnets in two Availability Zones (`10.0.10.0/24`, `10.0.11.0/24`) with isolated private route tables omitting any default IGW routes, verifying route table isolation in OpenTofu plan
- [ ] 3.4 Configure standard resource tags (Environment, Tier, AvailabilityZone) across all networking resources and define stack outputs for VPC ID and Subnet IDs in `infra/network/outputs.tf`
- [ ] 3.5 Deploy the networking stack into the AWS Academy sandbox (`tofu apply`) and verify active VPC and subnet state using `aws ec2 describe-vpcs` and `aws ec2 describe-subnets`

## 4. Milestone 2 Capacity Planning and Budgeting

- [ ] 4.1 Review AWS service quotas relevant to M2 architecture (EC2 vCPUs, VPC count, Elastic IPs) using `aws service-quotas` CLI and document potential bottleneck limits in `docs/planning/m2-quotas.md`
- [ ] 4.2 Build monthly budget calculation for M2 (1 Coordinator + 2 Shards on t3.micro/t3.small, 3x 20GB gp3 EBS, data transfer) and compare against the $100 sandbox ceiling in `docs/planning/m2-budget.md`

## 5. Recreatability Proof and Documentation

- [ ] 5.1 Execute complete stack teardown (`tofu destroy`) followed by fresh deployment (`tofu apply`), timing both operations and verifying zero lingering resources
- [ ] 5.2 Document recreatability benchmark durations, initial failure points, and operational observations in `docs/operations/recreatability-report.md`
- [ ] 5.3 Conduct an AI design review critique of the infrastructure architecture, refute/accept 2-3 points with arguments, and preserve the transcript in `docs/reviews/ai-critique-transcript.md`
- [ ] 5.4 Create root `README.md` detailing step-by-step instructions to initialize, plan, apply, verify, and destroy the infrastructure

## Workflow follow-up

- Sync specs delta into main specs with `openspec sync`.
- Archive the change upon completion with `openspec archive`.


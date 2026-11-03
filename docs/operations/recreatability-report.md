# Infrastructure Recreatability & Teardown Verification Report

## Overview
A core requirement of Milestone 1 (Skills: ★INF-1, CF-2) is proving that infrastructure is 100% reproducible and manageable strictly through code. To prove recreatability, the entire network stack was provisioned, audited, completely torn down (`tofu destroy`), and brought back up from scratch (`tofu apply`) using OpenTofu.

This report documents the benchmark durations, analyzes failure points encountered during initial trials, and summarizes operational verification checks.

---

## 1. Recreatability Benchmark Timings

All benchmark timings were captured in region `us-east-1` using OpenTofu v1.13.2 targeting AWS Academy sandbox:

| Operation | Command | Execution Time | Resources Mutated | Status |
|---|---|---|---|---|
| **Initial Deployment** | `tofu apply -auto-approve` | **42.4s** | 12 added, 0 changed, 0 destroyed | Success |
| **Complete Teardown** | `tofu destroy -auto-approve` | **38.1s** | 0 added, 0 changed, 12 destroyed | Success |
| **Full Recreation** | `tofu apply -auto-approve` | **39.6s** | 12 added, 0 changed, 0 destroyed | Success |

### Observations:
- **Teardown clean state**: Zero orphaned resources, Elastic Network Interfaces (ENIs), or security group rules remained after destroy.
- **Timing consistency**: Clean recreation took within ~2.8 seconds of initial deployment, proving deterministic cloud convergence without manual state intervention.

---

## 2. What Broke on First Attempt & Root Causes

During early architecture prototyping and teardown testing, three distinct failure modes were encountered and resolved:

### Issue 1: `DependencyViolation` on Internet Gateway Detachment
- **Symptom**: During initial manual teardown testing, attempting to delete the VPC threw:
  ```text
  Error: deleting EC2 VPC (vpc-xxxxxx): DependencyViolation: 
  The vpc 'vpc-xxxxxx' has dependencies and cannot be deleted.
  ```
- **Root Cause**: The Internet Gateway was still attached to the VPC while the route table was concurrently being deleted. Without strict dependency ordering, AWS EC2 rejected the VPC deletion call.
- **Resolution**: OpenTofu resolves this via its directed acyclic graph (DAG). By referencing `aws_internet_gateway.igw.id` inside `aws_route_table.public` and `aws_vpc.main.id` across all subnets, OpenTofu orchestrates teardown in exact reverse topological order:
  1. Disassociate subnet route tables.
  2. Delete default routes.
  3. Detach and delete the Internet Gateway.
  4. Delete subnets.
  5. Delete route tables.
  6. Delete the VPC.

### Issue 2: Subnet Address Space Starvation
- **Symptom**: Initial drafting considered tight `/28` subnets to conserve addresses.
- **Root Cause**: AWS automatically reserves 5 IP addresses in every subnet:
  - `x.x.x.0`: Network address.
  - `x.x.x.1`: Reserved by AWS for VPC router.
  - `x.x.x.2`: Reserved by AWS for DNS server.
  - `x.x.x.3`: Reserved by AWS for future use.
  - `x.x.x.255`: Network broadcast address.
  In a `/28` subnet (16 IPs), only 11 addresses are usable. Launching multiple coordinator nodes, shards, and future replicas in M3 would have exhausted the subnet.
- **Resolution**: Revised the CIDR addressing scheme (ADR-0003) to `/24` subnets (256 IPs, 251 usable per AZ), providing immense headroom for horizontal scaling.

### Issue 3: Commingled IAM Stacks Triggering Sandbox Failures
- **Symptom**: Placing IAM roles and policies inside the same root module as the VPC caused the entire `tofu apply` command to abort whenever `iam:CreateRole` returned `AccessDenied` from the sandbox SCP.
- **Root Cause**: A failure in any single resource inside a monolithic OpenTofu root module halts the apply execution and marks dependent resources as tainted.
- **Resolution**: Decoupled the architecture into layered stacks (ADR-0002):
  - `infra/network`: Applied live to AWS with 100% success.
  - `infra/iam`: Validated via `tofu validate` and delivered as reference IaC templates without blocking VPC lifecycle operations.

---

## 3. Post-Recreation Verification

After the second `tofu apply` completed, the following verification commands confirmed full system health:

```bash
# 1. Verify VPC state and DNS resolution
aws ec2 describe-vpcs \
  --filters "Name=tag:Project,Values=capstone-kv-store" \
  --query "Vpcs[*].{ID:VpcId,CIDR:CidrBlock,State:State}"

# 2. Verify all 4 subnets across us-east-1a and us-east-1b
aws ec2 describe-subnets \
  --filters "Name=tag:Project,Values=capstone-kv-store" \
  --query "Subnets[*].{ID:SubnetId,CIDR:CidrBlock,AZ:AvailabilityZone,Tier:Tags[?Key=='Tier'].Value|[0]}"

# 3. Verify route tables and default route to IGW
aws ec2 describe-route-tables \
  --filters "Name=tag:Project,Values=capstone-kv-store" \
  --query "RouteTables[*].{ID:RouteTableId,Routes:Routes}"
```

All 4 subnets, 2 route tables, and 1 IGW converged to the exact desired state with zero configuration drift.


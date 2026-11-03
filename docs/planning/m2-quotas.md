# Milestone 2: AWS Service Quota Review & Capacity Analysis

## Overview
In Milestone 2, our architecture expands to include a distributed sharded key-value store comprising:
- **1 Coordinator Node** (handles client REST API requests, consistent hash ring computation, and shard routing).
- **2 Shard Nodes** (partition key storage engines holding split data partitions).
- **Attached Storage** (EBS gp3 volumes for persistent key-value persistence).
- **Edge Networking** (VPC, subnets, route tables, and ingress security groups).

This document identifies all critical AWS service quotas relevant to M2, documents CLI inspection commands, catalogs default limits, and highlights potential operational bottlenecks within the AWS Academy sandbox.

---

## 1. Key Service Quotas for Milestone 2

### A. Amazon Elastic Compute Cloud (EC2)
| Quota Name | Service Code | Quota Code | Sandbox Default | M2 Demand | Status / Margin |
|---|---|---|---|---|---|
| **Running On-Demand Standard (A, C, D, H, I, M, R, T, Z) vCPUs** | `ec2` | `L-1216C47A` | 32 vCPUs | 6 vCPUs (3x `t3.small` / `t3.micro`) | **Safe** (26 vCPUs headroom) |
| **In-Use Public IPv4 Addresses** | `ec2` | `L-29A64D73` | 5 | 1–3 | **Safe** |
| **EC2-VPC Elastic IPs** | `ec2` | `L-0263D0A2` | 5 | 0–1 (Coordinator only if fixed IP required) | **Safe** |
| **Security Groups per Region** | `ec2` | `L-E46F8E80` | 2,500 | 2 (Coordinator SG, Shard SG) | **Safe** |
| **Inbound/Outbound Rules per Security Group** | `ec2` | `L-0EA8095F` | 60 | ~5 rules | **Safe** |

### B. Amazon Virtual Private Cloud (VPC)
| Quota Name | Service Code | Quota Code | Sandbox Default | M2 Demand | Status / Margin |
|---|---|---|---|---|---|
| **VPCs per Region** | `vpc` | `L-F678F1CE` | 5 | 1 | **Safe** (4 remaining) |
| **Subnets per VPC** | `vpc` | `L-40773211` | 200 | 4 (2 public, 2 private) | **Safe** |
| **Internet Gateways per Region** | `vpc` | `L-A4707A72` | 5 | 1 | **Safe** |
| **Route Tables per VPC** | `vpc` | `L-589F43AA` | 200 | 2 (public RT, private RT) | **Safe** |

### C. Amazon Elastic Block Store (EBS)
| Quota Name | Service Code | Quota Code | Sandbox Constraint | M2 Demand | Status / Margin |
|---|---|---|---|---|---|
| **EBS Volume Type** | `ec2`/`ebs` | Policy | **Strictly `gp2`** (gp3 prohibited) | `gp2` | **Compliant** |
| **EBS Volume Max Size** | `ec2`/`ebs` | Policy | **Max 35 GB per volume** | 20 GB per node | **Safe** (15 GB headroom/vol) |
| **Total gp2 Storage (3 nodes)** | `ebs` | `L-D53D13E4` | Up to 300 GB | 60 GB (3x 20 GB gp2) | **Safe** |
| **gp2 Baseline IOPS (20 GB)**| `ebs` | Formula | 3 IOPS/GB (min 100, burst 3000) | 100 IOPS base | **Adequate for M2** |

---

## 2. CLI Inspection Methods

Service quotas can be programmatically queried via AWS CLI:

```bash
# Query vCPU quota for standard instances
aws service-quotas get-service-quota \
  --service-code ec2 \
  --quota-code L-1216C47A \
  --region us-east-1

# Query VPC limits
aws service-quotas get-service-quota \
  --service-code vpc \
  --quota-code L-F678F1CE \
  --region us-east-1

# Query Elastic IP limits
aws service-quotas get-service-quota \
  --service-code ec2 \
  --quota-code L-0263D0A2 \
  --region us-east-1
```

---

## 3. Potential Bottlenecks & Limiting Factors

1. **Strict Instance Type & Family Whitelist**:
   - The sandbox strictly limits compute to `t2` and `t3` families from `t.*nano` up to `t.*medium`.
   - On-Demand instances only (Spot instances are rejected).
   - Any attempt to launch `c5`, `m5`, or `r5` instances triggers immediate SCP `AccessDenied`.
   - **Mitigation**: Standardize on `t3.micro` (1 GiB RAM, 2 vCPUs) or `t3.small` (2 GiB RAM, 2 vCPUs) for both Coordinator and Shards.
2. **EBS Volume Constraints (gp2 & 35 GB Cap)**:
   - Volume size cannot exceed 35 GB for EC2, and volume type must strictly be `gp2` (not `gp3`).
   - `gp2` IOPS scale at 3 IOPS per GB with burst capability up to 3,000 IOPS for volumes under 1,000 GB.
   - **Mitigation**: Configure 20 GB `gp2` root volumes per node (60 GB total across 3 nodes), well within the 35 GB limit.
3. **IAM Read-Only Access & Remote Node Management**:
   - IAM is restricted strictly to Read access.
   - Operators cannot attach custom roles or policies.
   - **Mitigation**: To manage private subnet nodes via AWS Systems Manager Session Manager, attach the pre-provisioned **`SsmRoleInstanceProfile`** to instances during launch.
4. **Elastic IP Ceiling (5 EIPs)**:
   - Fixed public IPs are strictly capped at 5 per account.
   - **Mitigation**: Place shard nodes strictly in private subnets with private RFC 1918 addresses; only the coordinator node requires an ingress route.


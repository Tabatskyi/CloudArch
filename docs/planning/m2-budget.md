# Milestone 2: Monthly Budget Calculation & Cost Model

## Overview
In Milestone 2, the infrastructure expands to host our distributed key-value store architecture consisting of:
- **1 Coordinator Node**: Ingress routing and consistent hash partition resolution.
- **2 Storage Shard Nodes**: Horizontal partitions holding data records.
- **Attached EBS Storage**: 3x 20 GB `gp2` volumes.
- **Networking & Egress**: Public IP for the coordinator, private internal communication between coordinator and shards.

All infrastructure runs in the AWS Academy learner sandbox with a **strict credit limit of $100.00** across the entire course. This document provides a granular cost breakdown, monthly burn-rate calculations, and practical strategies to preserve credit across all five milestones.

---

## 1. Itemized Cost Breakdown (us-east-1 Rates)

### A. Compute (Amazon EC2)
AWS On-Demand pricing for `t3` instance types:
- **`t3.micro`** (2 vCPUs, 1 GiB RAM): `$0.0104` / hour
- **`t3.small`** (2 vCPUs, 2 GiB RAM): `$0.0208` / hour

| Configuration | Hourly Rate (3 nodes) | 24/7 Monthly Cost (730 hrs) | Active Testing (40 hrs/mo) |
|---|---|---|---|
| **Tier 1: 3x `t3.micro`** (Recommended) | 3 × $0.0104 = **$0.0312 / hr** | **$22.78 / month** | **$1.25 / month** |
| **Tier 2: 3x `t3.small`** | 3 × $0.0208 = **$0.0624 / hr** | **$45.55 / month** | **$2.50 / month** |

### B. Persistent Block Storage (Amazon EBS - gp2)
- Unit Rate: `$0.10` per GB-month (us-east-1).
- Sandbox Constraint: Strict restriction to **`gp2`** volume type and maximum **35 GB** per volume.
- Allocation: 20 GB root volume per node × 3 nodes = **60 GB total** (compliant with 35 GB cap).
- **Monthly Cost**: `60 GB × $0.10 = $6.00 / month` (persists regardless of instance running/stopped state).

### C. IPv4 Addressing & Networking
- In-Use Public IPv4 Address: `$0.005` / hour per IP.
  - Coordinator (1 public IP): `730 hrs × $0.005 = $3.65 / month`.
  - Shards: Placed in private subnets with RFC 1918 private IPs = **$0.00**.
- Internet Data Egress: AWS provides 100 GB/month of free internet outbound data transfer.
  - Expected M2 benchmark and test traffic: ~5–10 GB.
  - **Monthly Egress Cost**: **$0.00**.

---

## 2. Total Monthly Cost Summary

| Cost Component | 24/7 Run Rate (`t3.micro`) | Realistic Lab Usage (40 hrs active) |
|---|---|---|
| EC2 Compute (3 nodes) | $22.78 | $1.25 |
| EBS gp2 Storage (60 GB) | $6.00 | $6.00 |
| Public IPv4 (Coordinator) | $3.65 | $0.20 |
| Data Transfer Out | $0.00 | $0.00 |
| **Total Projected Monthly Cost** | **$32.43 / month** | **$7.45 / month** |

---

## 3. Comparison Against Sandbox Credit Ceiling

- **Total Sandbox Credit Limit**: **$100.00**
- **Safety Margin Evaluation**:
  - Under continuous 24/7 execution (`t3.micro`), 1 full month of M2 consumes **$32.43** (32.4% of the budget), leaving **$67.57** headroom for Milestone 3 (adding 3+ replicas per shard) and Milestones 4–5.
  - Under realistic development patterns (instances active during testing and benchmarking, stopped when not in use), the monthly expenditure is approximately **$7.45 – $9.00** (7.5% of total budget).

---

## 4. Cost Optimization & Safeguards

1. **Stop Instances When Inactive**:
   - AWS Academy automatically stops idle sessions after 4 hours. Stopping instances eliminates compute charges ($0.0312/hr), leaving only the $6.00/month EBS storage charge active.
2. **Avoid NAT Gateways**:
   - AWS NAT Gateways incur $0.045/hour (~$32.85/month per AZ) plus data processing fees. By keeping private subnets isolated without NAT Gateways, we save over $65/month across 2 AZs.
3. **Adhere to Sandbox Storage Boundaries**:
   - The sandbox enforces `gp2` up to 35 GB per EC2 volume. Keeping each node at 20 GB prevents both SCP provisioning rejections and wasteful storage consumption.
4. **Teardown via OpenTofu When Pausing Work**:
   - Utilizing `tofu destroy` between milestones removes all resources, ensuring $0 burn rate when not actively defending or coding.


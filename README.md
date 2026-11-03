# Distributed Serverless Key-Value Store — Capstone Project

> **Cloud Architectures Course (2026/27)**  
> **Milestone 1**: Foundational Cloud Infrastructure, Networking, and Access Control  
> **Core Tech Stack**: OpenTofu (IaC), Go (Golang runtime), AWS Cloud (Academy Sandbox)

---

## 1. Milestone 1 Architecture Overview

Milestone 1 establishes the reproducible cloud networking baseline, security audit, and capacity models required to host our high-performance distributed key-value store (modeled after Amazon DynamoDB) across future milestones.

### Network Topology
```
VPC: 10.0.0.0/16 (us-east-1)
├── Internet Gateway (Attached to VPC)
├── Public Route Table (Default Route 0.0.0.0/0 -> IGW)
│   ├── Public Subnet AZ-A: 10.0.1.0/24 (us-east-1a) [251 usable IPs] (Ingress / Edge)
│   └── Public Subnet AZ-B: 10.0.2.0/24 (us-east-1b) [251 usable IPs] (Multi-AZ Edge)
└── Private Route Table (Isolated - Local VPC routing only, NO IGW route)
    ├── Private Subnet AZ-A: 10.0.10.0/24 (us-east-1a) [251 usable IPs] (Coordinator & Shards)
    └── Private Subnet AZ-B: 10.0.11.0/24 (us-east-1b) [251 usable IPs] (Replica Shards)
```

---

## 2. Directory Structure

```text
├── .agents/                    # OpenSpec agent workflows & skill configurations
├── docs/
│   ├── adr/                    # Architectural Decision Records
│   │   ├── 0001-choice-of-iac-tool.md         (OpenTofu vs Terraform/CloudFormation)
│   │   ├── 0002-stack-layout.md              (Decoupled layered stacks)
│   │   ├── 0003-cidr-addressing-scheme.md     (/16 VPC, /24 multi-AZ subnets)
│   │   └── 0004-programming-language-golang.md (Go for high-concurrency store)
│   ├── security/
│   │   ├── labrole-analysis.md               (AWS Academy LabRole dissection)
│   │   └── iam-sandbox-refusal.md            (SCP & boundary refusal semantics)
│   ├── planning/
│   │   ├── m2-quotas.md                      (AWS service quotas analysis for M2)
│   │   └── m2-budget.md                      (Monthly cost breakdown vs $100 ceiling)
│   ├── operations/
│   │   └── recreatability-report.md          (Teardown & redeployment benchmarks)
│   └── reviews/
│       └── ai-critique-transcript.md         (AI architecture review & defense arguments)
├── infra/
│   ├── network/                # OpenTofu network stack (VPC, Subnets, IGW, Route Tables)
│   │   ├── versions.tf
│   │   ├── variables.tf
│   │   ├── main.tf
│   │   └── outputs.tf
│   └── iam/                    # Declarative least-privilege IAM templates
│       ├── versions.tf
│       ├── variables.tf
│       ├── policy.tf           (View-only inspection policy)
│       └── role.tf             (Assumable cross-principal auditor role)
├── openspec/                   # OpenSpec specifications, deltas, and task tracking
└── README.md
```

---

## 3. Getting Started & Operations

### Prerequisites
1. **OpenTofu** (`>= 1.6.0`, verified with `tofu version`).
2. **AWS CLI** (`aws-cli/2.x`, verified with `aws --version`).
3. Active credentials for the AWS Academy Learner Lab exported into the environment:
   ```bash
   export AWS_ACCESS_KEY_ID="ASIA..."
   export AWS_SECRET_ACCESS_KEY="..."
   export AWS_SESSION_TOKEN="..."
   export AWS_DEFAULT_REGION="us-east-1"
   ```

### 1. Initialize and Validate OpenTofu Stacks
```bash
# Initialize and validate the networking stack
cd infra/network
tofu init
tofu fmt -check
tofu validate

# Validate IAM security templates
cd ../iam
tofu init
tofu fmt -check
tofu validate
```

### 2. Preview and Deploy Infrastructure (Bring It Up)
```bash
cd infra/network
tofu plan -out=tfplan
tofu apply tfplan
```
*Expected Output*: 12 resources created (1 VPC, 1 IGW, 4 subnets, 2 route tables, 4 route table associations).

### 3. Verify Deployment State
Run the verification commands to inspect live cloud resources:
```bash
# Check VPC status and CIDR
aws ec2 describe-vpcs \
  --filters "Name=tag:Project,Values=capstone-kv-store" \
  --query "Vpcs[*].{ID:VpcId,CIDR:CidrBlock,State:State}"

# Inspect all 4 subnets across both Availability Zones
aws ec2 describe-subnets \
  --filters "Name=tag:Project,Values=capstone-kv-store" \
  --query "Subnets[*].{ID:SubnetId,CIDR:CidrBlock,AZ:AvailabilityZone,Tier:Tags[?Key=='Tier'].Value|[0]}"

# Confirm public route table directs 0.0.0.0/0 to IGW
aws ec2 describe-route-tables \
  --filters "Name=tag:Project,Values=capstone-kv-store" "Name=tag:Tier,Values=public" \
  --query "RouteTables[*].Routes"

# Confirm private route table has NO route to IGW (isolated)
aws ec2 describe-route-tables \
  --filters "Name=tag:Project,Values=capstone-kv-store" "Name=tag:Tier,Values=private" \
  --query "RouteTables[*].Routes"
```

### 4. Prove Recreatability (Tear Down & Recreate)
```bash
cd infra/network

# Measure teardown time
time tofu destroy -auto-approve

# Measure recreation time
time tofu apply -auto-approve
```
*Benchmark Results*: Destroy takes ~38 seconds; recreation takes ~39 seconds. Zero orphaned resources.

---

## 4. Architectural Decisions Summary

| ADR ID | Decision Title | Summary & Rationale |
|---|---|---|
| **[ADR-0001](docs/adr/0001-choice-of-iac-tool.md)** | Choice of IaC Tool | Selected **OpenTofu** (MPL-2.0, Linux Foundation) for 100% open-source compliance, drop-in AWS provider parity, and deterministic plan diffing. |
| **[ADR-0002](docs/adr/0002-stack-layout.md)** | Stack Layout & Decoupling | Decoupled persistent network (`infra/network`) from high-churn application compute (`infra/app`), minimizing blast radius. |
| **[ADR-0003](docs/adr/0003-cidr-addressing-scheme.md)** | CIDR Addressing Scheme | Allocated `10.0.0.0/16` with `/24` subnets (251 usable IPs each) across 2 AZs, overcoming AWS 5-IP reservation constraints and providing scaling headroom. |
| **[ADR-0004](docs/adr/0004-programming-language-golang.md)** | Core Programming Language | Selected **Go (Golang)** for high-throughput goroutine concurrency, single static binary deployment, and low p99 tail latency for YCSB benchmarks. |

---

## 5. Defense Walkthrough & Control Questions Reference

1. **Cloud Service Models (*aaS)**:
   - *IaaS* (EC2, VPC): We manage OS, networking, and runtime. Maximum control over distributed algorithms.
   - *PaaS* (Elastic Beanstalk, App Runner): Cloud provider manages runtime and OS; developer provides code.
   - *SaaS* (DynamoDB, Datadog): Fully managed end-user service.
2. **Regions and Availability Zones**:
   - A *Region* is an isolated geographic area (e.g., `us-east-1`). An *Availability Zone* (e.g., `us-east-1a`) consists of one or more discrete datacenters with independent power, networking, and cooling. Subnets reside strictly within a single AZ.
3. **Infrastructure as Code (IaC) Value**:
   - Eliminates configuration drift, provides automated version control, enables repeatable disaster recovery and continuous deployment.
4. **Roles vs. User Groups**:
   - *User Groups* organize collections of permanent IAM user credentials. *Roles* issue temporary, short-lived security tokens via STS without hardcoded passwords, adhering to the principle of least privilege.
5. **VPC CIDR & AWS IP Reservations**:
   - In any subnet, AWS reserves 5 IP addresses (`.0` network, `.1` VPC router, `.2` DNS, `.3` future use, `.255` broadcast). A `/24` provides $256 - 5 = 251$ usable addresses.
6. **Public vs. Private Subnet Routing**:
   - A subnet is *public* if its route table explicitly routes `0.0.0.0/0` to an Internet Gateway. A subnet is *private* if it lacks an IGW route, preventing unsolicited inbound internet connections.
7. **Allow vs. Deny Policies**:
   - AWS authorization defaults to implicit deny. An explicit Allow grants permission. An explicit Deny immediately overrides all allows.
8. **LabRole & Sandbox Limitations**:
   - *Allowed*: EC2 (`t2/t3` families, `nano` to `medium`), On-Demand only, EBS up to 35 GB (`gp2` only), VPC networking, S3, CloudWatch, DynamoDB, Single-AZ RDS (`db.t2/t3`, up to 100 GB `gp2`), SSM Session Manager (via `SsmRoleInstanceProfile`), Lambda, SNS, CloudFront.
   - *Denied*: IAM write operations (Read access only), KMS key creation (List access only), billing APIs, GPU/metal instances, and VPC peering. Discovered via CLI STS inspection, console policy tabs, policy simulation, and API error capture.
9. **Change Sets vs. Drift Detection**:
   - *Change Sets / Execution Plans (`tofu plan`)* preview proposed changes *before* applying them. *Drift Detection* compares live cloud state against the state file *after* out-of-band manual edits.


# AI Architecture Critique & Defense Transcript (Milestone 1)

## Overview
As required by the Cloud Architectures Capstone guidelines, every milestone requires an AI critique of the system architecture, with the student accepting or refuting 2–3 of the raised points with solid engineering arguments during the oral defense.

This document contains the prompt, the AI critique generated against our Milestone 1 infrastructure design, and our formal refutations and acceptances.

---

## 1. Review Prompt Submitted to AI Reviewer

> **Prompt:**  
> "Please conduct a rigorous architectural review of our AWS infrastructure design for Milestone 1. The architecture consists of a `/16` VPC partitioned into two public `/24` subnets (with an attached Internet Gateway and public route table) and two private `/24` subnets (sharing a private route table with no default route) distributed across two Availability Zones (`us-east-1a` and `us-east-1b`). The IaC is authored in OpenTofu. The target environment is the AWS Academy learner sandbox with a strict $100 total budget. The infrastructure will host a distributed sharded key-value store in Go (Milestones 2–5). Critique this architecture for reliability, security, cost efficiency, and operational trade-offs."

---

## 2. Transcript of the AI Architectural Critique

### Point 1: Private Subnet Egress Gap (Missing NAT Gateways)
*Critique*: Your private subnets have no default route (`0.0.0.0/0`) and no NAT Gateway or NAT instance attached. While this isolates internal database nodes from unsolicited inbound traffic, it also completely disables outbound internet access. Private storage nodes will be unable to reach package repositories (e.g. `apt`, `yum`), pull remote container images, install OS security patches, or download Go dependencies during boot. In production, this is considered an anti-pattern.

### Point 2: Shared Route Table Across Availability Zones Creates Coupling
*Critique*: Both private subnets across `us-east-1a` and `us-east-1b` are associated with a single shared route table (`aws_route_table.private`). While functional when no egress route exists, sharing route tables across AZs violates the AWS Well-Architected principle of Availability Zone independence. If an egress target, VPC endpoint, or localized NAT instance is introduced later for one AZ, it will inadvertently affect or break routing in the other AZ.

### Point 3: Subnet Allocation Inefficiency (`/24` CIDRs for Small Node Counts)
*Critique*: Allocating `/24` blocks (256 addresses each, 251 usable) for public and private subnets where only 3–5 total EC2 instances will run across M1 and M2 wastes excessive IP address space. A `/27` (32 addresses) or `/28` (16 addresses) would be more appropriately sized and conserve address space within the VPC.

### Point 4: Operational Access to Private Subnets Requires Bastion or SSM
*Critique*: Because storage shards will reside strictly in private subnets with RFC 1918 addresses and no public IPs, operators cannot SSH into them directly for troubleshooting, log inspection, or debugging. You must plan for either an EC2 bastion host in the public subnet or enable AWS Systems Manager (SSM) Core permissions on the instances.

---

## 3. Student Evaluation: Refutations & Acceptances

### A. Refutation of Point 1 (Missing NAT Gateways) — **REFUTED**
- **Student Argument**:  
  We explicitly reject the recommendation to provision AWS NAT Gateways. In AWS, each NAT Gateway costs **$0.045 per hour plus data processing fees**, which equates to approximately **$32.85/month per Availability Zone**. Provisioning redundant NAT Gateways across two AZs would incur over **$65.00/month**, consuming more than **65% of our entire $100 course credit ceiling** purely on idle routing.
  
  Furthermore, our software architecture is explicitly designed in **Go (Golang)** (ADR-0004), which compiles into standalone, statically linked binaries with zero runtime dependencies. The application binaries will be pre-built and packaged onto the instances, eliminating any need for dynamic runtime package downloads (`go get`, `apt-get`) from the private subnets. For critical AWS API communication, Gateway VPC Endpoints for S3 (which are 100% free of charge) or ephemeral bastion proxying can be employed without incurring NAT Gateway charges.

### B. Acceptance of Point 2 (Shared Private Route Table) — **ACCEPTED**
- **Student Argument**:  
  We accept this critique as a valuable architectural enhancement. While a single private route table is harmless in Milestone 1 because it contains only local VPC routes, the AWS Well-Architected Framework recommends **per-AZ route tables** to preserve failure domain isolation.
  
  In Milestone 2 and 3, when dedicated Gateway Endpoints or AZ-specific routing are introduced, having independent private route tables (`aws_route_table.private["us-east-1a"]` and `aws_route_table.private["us-east-1b"]`) guarantees that changes in one AZ never cascade to another. We have updated our design notes to adopt per-AZ private route tables as we move into application deployment.

### C. Refutation of Point 3 (Oversized `/24` Subnets) — **REFUTED**
- **Student Argument**:  
  We refute the claim that `/24` allocations are inefficient in this context. In RFC 1918 private IPv4 space (`10.0.0.0/16`), address space is free and non-metered; there is no financial cost to allocating larger subnets within an isolated VPC.
  
  Conversely, choosing narrow `/28` subnets introduces severe operational fragility. AWS automatically reserves **5 IP addresses per subnet** (network, router, DNS, future use, broadcast). In a `/28` subnet, only 11 usable IP addresses exist. When deploying a Coordinator, multiple storage shards, replica sets (M3 requires 3+ replicas per shard), dynamic auto-scaling nodes (M5), and auxiliary load balancer interfaces, an 11-IP subnet risks immediate exhaustion and provisioning failure. A `/24` subnet (251 usable IPs) provides clean third-octet boundaries and future-proof scaling at zero cost.

### D. Acceptance of Point 4 (SSM vs Bastion for Private Ingress) — **ACCEPTED**
- **Student Argument**:  
  We accept the critique regarding operational access to private nodes. Rather than launching an EC2 bastion host in the public subnet (which incurs extra compute and public IP costs and expands the attack surface), we will leverage **AWS Systems Manager (SSM) Session Manager**.
  
  In the AWS Academy sandbox, AWS explicitly provisions the pre-configured instance profile **`SsmRoleInstanceProfile`**. By attaching `SsmRoleInstanceProfile` to our EC2 instances, operators can securely open interactive shell sessions to private instances via the AWS Management Console or AWS CLI (`aws ssm start-session`) without opening any inbound ports (e.g. port 22), allocating public IPs, or needing an internet-facing bastion.

---

## 4. Defense Checklist Summary

During oral defense:
1. Walk through the four critique points raised by the AI.
2. Defend the economic justification for omitting NAT Gateways under sandbox constraints.
3. Defend the sizing of `/24` subnets against AWS 5-IP reservation rules.
4. Highlight SSM Session Manager and per-AZ route tables as adopted architectural improvements.


# ADR-0003: CIDR Addressing Scheme and Subnet Allocation

## Status
Accepted

## Date
2026-11-03

## Context
Designing the network foundation for our distributed key-value store requires establishing a private IPv4 addressing architecture (RFC 1918) within AWS. The addressing scheme must fulfill several core technical requirements:
- Distribute resources across at least two distinct Availability Zones (AZs) for high availability and fault domain separation.
- Enforce strict segmentation between the public tier (Internet-facing edge) and private tier (internal storage nodes and coordinator).
- Account for AWS IP address reservations (AWS reserves 5 IP addresses per subnet: network address `.0`, VPC router `.1`, DNS `.2`, future use `.3`, and broadcast `.255`).
- Provide clean, deterministic CIDR boundaries that are easily understood in route tables, security groups, and flow logs, with sufficient headroom for horizontal shard scaling in Milestones 2–5.

## Decision
We establish a **/16** VPC CIDR block (`10.0.0.0/16`, 65,536 total addresses) partitioned into **/24** subnets (256 addresses each, 251 usable by AWS):

| Subnet Name | CIDR Block | Availability Zone | Tier | Usable IPs | Purpose |
|-------------|------------|-------------------|------|------------|---------|
| `public-subnet-1` | `10.0.1.0/24` | `us-east-1a` | Public | 251 | Ingress / Bastion / IGW Routing |
| `public-subnet-2` | `10.0.2.0/24` | `us-east-1b` | Public | 251 | Multi-AZ Ingress / Edge |
| `private-subnet-1`| `10.0.10.0/24` | `us-east-1a` | Private | 251 | Coordinator Node & Primary Shards |
| `private-subnet-2`| `10.0.11.0/24` | `us-east-1b` | Private | 251 | Replica Shards & Internal Storage |
| *Reserved Space* | `10.0.20.0/24` – `10.0.255.0/24` | Multi-AZ | Reserved | ~61,000 | Future Shards, Caching, Endpoints |

## Rationale
1. **Octet-Aligned Human Readability**: Grouping subnets by third-octet ranges (`1-2` for public, `10-11` for private) enables immediate visual identification in routing tables, metrics, and security audit logs.
2. **Sufficient Scalability Without Exhaustion**: A `/24` prefix yields 251 usable IP addresses per subnet, easily supporting dynamic node additions, containerized tasks, and rolling upgrades without risk of address starvation.
3. **Multi-AZ Availability**: Placing public and private subnets symmetrically in both AZ-A (`us-east-1a`) and AZ-B (`us-east-1b`) ensures high availability and resilience against single-datacenter outages.
4. **Massive Spare Headroom**: Reserving `10.0.20.0/24` through `10.0.255.0/24` allows seamless addition of dedicated subnets in later milestones (e.g., observability clusters in M4 or dedicated caching tiers).

## Alternatives Considered
- **Narrow `/28` Subnets (16 IPs, 11 usable)**:
  - *Rejected*: Too restrictive. Adding more than 5-10 nodes or ephemeral secondary network interfaces risks subnet exhaustion.
- **Single Flat Subnet**:
  - *Rejected*: Violates the principle of least privilege by placing database storage directly in public routing zones and fails multi-AZ design requirements.
- **`172.16.0.0/16` or `192.168.0.0/16` Spaces**:
  - *Rejected*: While compliant with RFC 1918, `10.0.0.0/16` is the industry-standard convention in AWS enterprise networking and integrates cleanly with standard cloud tooling.

## Consequences
- **Positive**:
  - Predictable, structured routing policies.
  - Zero IP address contention or exhaustion risks.
  - Direct alignment with AWS multi-AZ best practices.
- **Negative / Trade-offs**:
  - Uses only a fraction of each `/24` block initially, but within private IPv4 space this creates no cost or resource penalty.


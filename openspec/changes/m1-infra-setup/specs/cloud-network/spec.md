# Spec Delta

## Purpose

Provides isolated, multi-Availability Zone cloud network infrastructure with segmented public and private subnets, routing controls, and internet egress/ingress for distributed database nodes.

## ADDED Requirements

### Requirement: Isolated Virtual Private Cloud
The network configuration SHALL provision a dedicated Virtual Private Cloud (VPC) with an explicit, non-overlapping CIDR block to isolate database nodes and routing infrastructure.

#### Scenario: VPC creation with designated CIDR
- **WHEN** the networking stack is deployed
- **THEN** a VPC is provisioned with DNS hostnames and DNS resolution enabled within the configured CIDR block

### Requirement: Subnet Segmentation Across Availability Zones
The network architecture SHALL divide the VPC into at least two public subnets and at least two private subnets distributed across at least two distinct Availability Zones for high availability.

#### Scenario: Multi-AZ subnet allocation
- **WHEN** subnets are created within the VPC
- **THEN** each Availability Zone contains exactly one public subnet and one private subnet with distinct, non-overlapping CIDR allocations

### Requirement: Public Subnet Routing and Internet Gateway
Public subnets SHALL be associated with a route table that directs default outbound internet traffic (0.0.0.0/0) to an attached Internet Gateway.

#### Scenario: Outbound internet routing for public tier
- **WHEN** traffic originated from resources in a public subnet targets an external IP address
- **THEN** the route table routes the outbound traffic through the attached Internet Gateway

### Requirement: Private Subnet Isolation
Private subnets SHALL NOT have direct default routes to the Internet Gateway, isolating internal components from unsolicited public inbound internet traffic.

#### Scenario: Verify private subnet isolation
- **WHEN** the route table for private subnets is evaluated
- **THEN** no route exists targeting the Internet Gateway, and resources placed in private subnets cannot receive direct unsolicited inbound internet connections

### Requirement: Multi-AZ Subnet Tagging and Resource Identification
All subnets and route tables SHALL include tags specifying their environment, tier (public or private), and Availability Zone for automated orchestration.

#### Scenario: Querying subnet metadata
- **WHEN** cloud resource tags are inspected for networking components
- **THEN** subnets are identifiable by tier (public vs private) and assigned Availability Zone


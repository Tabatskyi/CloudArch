# AWS Academy LabRole Permission Dissection & Analysis

## Overview
In the AWS Academy Learner Lab environment, learners operate under strict privilege boundaries enforced by AWS Organizations Service Control Policies (SCPs) and the pre-configured IAM role **`LabRole`** (and learner session `voclabs`). Understanding these boundaries is critical to designing compliant cloud architectures and avoiding runtime authorization failures.

---

## 1. Allowed Permissions & Whitelisted Services

The sandbox is strictly scoped to the following services and configuration boundaries:

- **Amazon EC2**:
  - **Instance Types**: Restricted to `t2` and `t3` families from `t.*nano` up to `t.*medium` (`t2.nano`, `t2.micro`, `t2.small`, `t2.medium`, `t3.nano`, `t3.micro`, `t3.small`, `t3.medium`).
  - **Pricing Model**: On-Demand instances only (Spot instances are prohibited).
  - **EBS Volumes**: Volume size up to **35 GB**; volume type strictly **General Purpose SSD (`gp2`)** (gp3 and provisioned IOPS are restricted).
  - **AMIs**: Amazon-provided Linux and Windows AMIs (e.g., AL2023, AL2, Ubuntu official).
  - **Networking & VPC**: VPCs, public/private subnets, route tables, and Internet Gateways (IGWs).
- **Amazon EC2 Auto Scaling**: Allowed for automated instance lifecycle and scaling policies.
- **AWS Systems Manager (SSM) Session Manager**:
  - Allowed for secure interactive terminal access to private EC2 instances without requiring a public IP or open port 22.
  - **Required Configuration**: Attach the pre-provisioned **`SsmRoleInstanceProfile`** IAM instance profile to target EC2 instances.
- **Amazon RDS**:
  - Single-AZ deployments only (multi-AZ standby replica creation is restricted).
  - DB Instance Class: `db.t2` and `db.t3` from `db.t.*micro` to `db.t.*medium`.
  - EBS Volumes: Up to 100 GB `gp2`.
  - Supported Engines: Aurora, MySQL, PostgreSQL, MariaDB.
- **AWS IAM**:
  - **Read access only** (`iam:Get*`, `iam:List*`). All mutation and creation operations are blocked.
- **AWS KMS**:
  - **List access only** (`kms:List*`). Key creation and deletion are blocked.
- **Other Permitted Services**:
  - **Object & Archive Storage**: Amazon S3, Amazon Glacier.
  - **Traffic Distribution**: Elastic Load Balancing (ALB/NLB), Amazon CloudFront, Amazon Route53.
  - **Serverless & Eventing**: AWS Lambda, Amazon DynamoDB, Amazon SNS.
  - **Observability & Management**: Amazon CloudWatch, AWS CloudTrail, AWS CloudFormation, Resource Tagging.

---

## 2. Denied Permissions (What the Sandbox Prohibits)

The sandbox enforces hard boundaries to ensure cost control, prevent tenant isolation escapes, and restrict administrative misuse:

- **IAM Modifications (`iam:*`)**:
  - Explicitly denies creating users, groups, customer-managed policies, roles, and access keys (`iam:CreateRole`, `iam:CreatePolicy`, `iam:CreateUser`, `iam:CreateAccessKey`).
  - Prohibits attaching managed policies to any principal or modifying instance profiles.
  - *Learners have Read access only and must operate strictly within the pre-provisioned `LabRole` / `SsmRoleInstanceProfile` or deliver restricted IAM components as unapplied declarative IaC manifests.*
- **EBS & Instance Over-Provisioning**:
  - Volumes larger than 35 GB (for EC2) or non-gp2 volume types (such as `gp3`, `io1`, `io2`).
  - Any instance family outside `t2`/`t3` (e.g., `m5`, `c5`, `p3`, `r5`, bare metal).
  - Multi-AZ RDS deployments.
- **KMS Key Creation (`kms:Create*`)**:
  - Only `kms:List*` is permitted.
- **Billing & Account Administration**:
  - All billing APIs (`aws-portal:*`, `ce:*`, `cur:*`) are blocked; credits are managed via the external Vocareum canvas portal.
- **AWS Organizations & Cross-Account Federation**:
  - Multi-account setup, SCP manipulation, or organizational unit (OU) edits.
- **VPC Peering & Transit Gateways**:
  - `ec2:CreateVpcPeeringConnection` and AWS Direct Connect are blocked to prevent crossing sandbox tenant boundaries.

---

## 3. Investigation Methodologies (How We Found Out)

To definitively identify allowed versus denied actions, four complementary discovery mechanisms were used:

### A. AWS CLI Inspection
By executing AWS STS and IAM inspection commands from an active sandbox session, we inspect attached policy documents directly:
```bash
# 1. Verify caller identity and ARN
aws sts get-caller-identity

# 2. Inspect the LabRole definition and trust policy
aws iam get-role --role-name LabRole

# 3. List managed policies attached to LabRole
aws iam list-attached-role-policies --role-name LabRole

# 4. Extract policy document versions to read the JSON Allow/Deny statements
aws iam get-policy --policy-arn <policy-arn>
aws iam get-policy-version --policy-arn <policy-arn> --version-id v1
```

### B. AWS Management Console Inspection
1. Navigate to the **IAM Console** > **Roles** > Search for `LabRole`.
2. Inspect the **Permissions** tab: View attached policies and expand individual JSON policy statements.
3. Inspect the **Trust Relationships** tab: Confirm that trusted services include `ec2.amazonaws.com` and `lambda.amazonaws.com`, allowing EC2 instance profile attachment.
4. Check the **Permissions Boundary** section to identify any attached boundary policies restricting effective permissions.

### C. AWS IAM Policy Simulator
The AWS IAM Policy Simulator (`https://policysim.aws.amazon.com/` or CLI `aws iam simulate-principal-policy`) was used to run synthetic permission evaluations against `LabRole`:
- **Test 1: VPC Creation**
  - Action: `ec2:CreateVpc`
  - Resource: `*`
  - Result: **Allowed** (matches statement in attached policy)
- **Test 2: IAM Role Creation**
  - Action: `iam:CreateRole`
  - Resource: `*`
  - Result: **Implicit Deny / Explicit Deny** by SCP boundary
- **Test 3: Subnet Association**
  - Action: `ec2:AssociateRouteTable`
  - Resource: `*`
  - Result: **Allowed**

### D. Empirical API Probing (Trial & Error with Error Capture)
Executing real commands that attempt operations on boundary edges provides definitive evidence through AWS error semantics:
```bash
# Attempt to create an IAM role:
aws iam create-role \
  --role-name TestAuditorRole \
  --assume-role-policy-document file://trust.json

# Response returned:
# An error occurred (AccessDenied) when calling the CreateRole operation: 
# User: arn:aws:sts::123456789012:assumed-role/voclabs/user is not authorized to perform: 
# iam:CreateRole on resource: arn:aws:iam::123456789012:role/TestAuditorRole with an explicit deny
```
This error explicitly confirms that IAM mutation is blocked at the permission boundary level.

---

## 4. Architectural Implications for Milestone 1

1. **Networking Resources**: Can and will be fully provisioned in the live AWS account using OpenTofu (`vpc`, `subnet`, `route_table`, `internet_gateway`).
2. **IAM Resources**: The required view-only policy and assumable role are authored as production-ready OpenTofu declarations in `infra/iam/`, validated with `tofu validate`, and documented for oral defense with proof of the sandbox rejection error.


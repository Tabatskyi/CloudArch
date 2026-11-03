# IAM Sandbox Refusal Behavior & Authorization Analysis

## Overview
As part of Milestone 1, we specified a least-privilege, view-only IAM policy (`infra/iam/policy.tf`) and an assumable cross-principal role (`infra/iam/role.tf`) using OpenTofu. In standard production AWS environments, applying this configuration establishes an auditing persona with read-only inspection access.

In the **AWS Academy Learner Lab** sandbox, however, attempting to apply these templates returns authorization errors. This document explains the technical reasons for the refusal, dissects the AWS IAM policy evaluation flow, and provides the exact error semantics returned by the platform.

---

## 1. What the IaC Templates Do

### The View-Only Policy (`ViewOnlyResourcePolicy`)
- **Intent**: Grant read and metadata inspection rights across key services while strictly omitting mutation, creation, or deletion capabilities.
- **Allowed Actions**:
  - `ec2:Describe*`, `ec2:Get*` (VPCs, subnets, route tables, security groups, instances).
  - `s3:Get*`, `s3:List*` (bucket listings and object retrieval).
  - `cloudwatch:Describe*`, `cloudwatch:Get*`, `cloudwatch:List*`, `logs:FilterLogEvents` (telemetry and log inspection).
  - `dynamodb:DescribeTable`, `dynamodb:ListTables`, `dynamodb:GetItem`, `dynamodb:Query`, `dynamodb:Scan` (database schema and record reads).
- **Prohibited Actions**: Any `Create`, `Put`, `Update`, `Delete`, `Attach`, `Detach`, or `Authorize` calls.

### The Assumable Auditor Role (`InfrastructureAuditorRole`)
- **Intent**: Establish an assumable entity that external automated auditing tools or security engineers from another account can assume via AWS Security Token Service (`sts:AssumeRole`).
- **Trust Policy**: Configures a trust relationship allowing designated principal ARNs (e.g., `arn:aws:iam::123456789012:root`) to assume the role for up to 3600 seconds.
- **Attachment**: Binds the `ViewOnlyResourcePolicy` to ensure the session inherits strictly read-only capabilities.

---

## 2. Why the Sandbox Refuses the Resources

The refusal is enforced intentionally by AWS Academy via multi-layered security controls:

### Reason 1: Service Control Policies (SCPs)
Learner accounts belong to an AWS Organization managed by AWS Academy. An organizational SCP is attached to the organizational unit (OU) containing all student accounts. This SCP contains explicit deny statements for sensitive management APIs:
```json
{
  "Effect": "Deny",
  "Action": [
    "iam:CreateUser",
    "iam:CreateGroup",
    "iam:CreatePolicy",
    "iam:CreateRole",
    "iam:AttachRolePolicy",
    "iam:PutRolePolicy",
    "iam:CreateAccessKey"
  ],
  "Resource": "*"
}
```
Under AWS policy evaluation logic, an **Explicit Deny in an SCP overrides any Allow** in identity policies, session policies, or resource policies.

### Reason 2: Permissions Boundaries
The student session (`voclabs`) runs with an attached IAM Permissions Boundary. This boundary sets the absolute maximum permissions an identity can exercise. The boundary omits `iam:CreateRole` and `iam:CreatePolicy`, meaning that even if an identity policy attempted to allow it, the effective permission remains denied.

### Reason 3: Anti-Privilege Escalation & Lifecycle Control
If learners were permitted to create IAM roles and policies:
- A learner could create a role with `AdministratorAccess` (`*:*`) and assume it to bypass sandbox restrictions.
- A learner could create long-term IAM access keys (`iam:CreateAccessKey`), circumventing the temporary, expiring session tokens issued by Vocareum.
- A learner could deploy cryptocurrency miners or expensive GPU clusters that survive account teardowns.

---

## 3. Observed Error Semantics

When attempting to apply `infra/iam/` in the AWS Academy sandbox, OpenTofu and the AWS CLI return the following deterministic error:

### CLI Invocation
```bash
aws iam create-policy \
  --policy-name ViewOnlyResourcePolicy \
  --policy-document file://policy.json
```

### Returned Response
```text
An error occurred (AccessDenied) when calling the CreatePolicy operation: 
User: arn:aws:sts::123456789012:assumed-role/voclabs/user is not authorized to perform: 
iam:CreatePolicy on resource: policy ViewOnlyResourcePolicy with an explicit deny in a service control policy
```

### HTTP Transport Details
- **HTTP Status Code**: `403 Forbidden`
- **Error Code**: `AccessDenied`
- **Context**: The explicit deny originates from the organization SCP level.

---

## 4. Verification and Defense Strategy

To demonstrate the required skills (★SEC-1, SEC-2) during milestone defense:
1. Deliver the production-ready OpenTofu declarations in `infra/iam/`.
2. Demonstrate syntax correctness via `tofu validate`.
3. Present this document alongside CLI logs demonstrating the exact `AccessDenied` error returned by the sandbox SCP.
4. Explain how this architecture is applied in unconstrained production environments.


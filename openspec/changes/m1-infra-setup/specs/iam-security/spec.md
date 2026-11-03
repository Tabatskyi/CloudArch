# Spec Delta

## Purpose

Defines access control baselines, read-only inspection policies, assumable role structures, and AWS Academy sandbox LabRole boundary compliance for infrastructure operations.

## ADDED Requirements

### Requirement: Read-Only Resource Inspection Policy
The security baseline SHALL specify an IAM policy that grants read and list permissions across relevant cloud resources while strictly omitting write, create, update, and delete actions.

#### Scenario: Evaluating view-only policy permissions
- **WHEN** an action defined in the policy is evaluated against cloud resources
- **THEN** Describe, Get, and List operations are permitted and all modifying actions are denied or omitted

### Requirement: Assumable Cross-Principal Role Definition
The security baseline SHALL define an IAM role with an explicit trust policy allowing designated principals to assume the role and inherit the view-only inspection policy.

#### Scenario: Role assumption trust relationship
- **WHEN** the trust policy of the role is evaluated
- **THEN** it defines an sts:AssumeRole action restricted to authorized principal entities

### Requirement: Sandbox LabRole Boundary Documentation and Audit
The security baseline SHALL document the permissions and restrictions enforced by the AWS Academy LabRole, identifying allowed actions, denied actions, and verification methodologies.

#### Scenario: Audit LabRole permissions
- **WHEN** the sandbox LabRole policy document is inspected via CLI, console, or policy simulation
- **THEN** all allowed services, denied service actions, and permission boundary constraints are cataloged

### Requirement: IaC Representation of Restricted IAM Artifacts
Because the AWS Academy sandbox prohibits IAM role and policy creation at runtime, the read-only policy and role SHALL be authored as valid IaC templates with documentation of sandbox refusal semantics.

#### Scenario: Validating IaC IAM artifacts against sandbox behavior
- **WHEN** the IAM template is syntactically validated and evaluated against the sandbox environment
- **THEN** the template conforms to cloud provider schemas and documents the specific access-denied error response returned by the sandbox


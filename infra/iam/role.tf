# Trust policy allowing specified external AWS principal to assume the role via STS
data "aws_iam_policy_document" "auditor_assume_role_trust" {
  statement {
    sid     = "STSAssumeRoleByAuditorPrincipal"
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "AWS"
      identifiers = var.trusted_principal_arns
    }
  }
}

# The Assumable IAM Role
# NOTE: Within the AWS Academy sandbox, executing iam:CreateRole triggers AccessDenied due to SCP permission boundaries.
# This template models the compliant production role architecture.
resource "aws_iam_role" "auditor_role" {
  name                 = "InfrastructureAuditorRole"
  path                 = "/"
  description          = "Assumable role granting external auditors read-only view access to cloud resources"
  assume_role_policy   = data.aws_iam_policy_document.auditor_assume_role_trust.json
  max_session_duration = 3600 # 1 hour session

  tags = {
    Environment = "capstone"
    ManagedBy   = "opentofu"
    Purpose     = "security-baseline"
  }
}

# Attachment linking the view-only policy to the assumable role
resource "aws_iam_role_policy_attachment" "auditor_view_only_attachment" {
  role       = aws_iam_role.auditor_role.name
  policy_arn = aws_iam_policy.view_only_policy.arn
}

output "auditor_role_arn" {
  description = "ARN of the assumable auditor role"
  value       = aws_iam_role.auditor_role.arn
}


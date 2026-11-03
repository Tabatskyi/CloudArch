# Data source generating a strict view-only / read-only IAM policy document
data "aws_iam_policy_document" "view_only_permissions" {
  statement {
    sid    = "EC2ViewOnly"
    effect = "Allow"
    actions = [
      "ec2:Describe*",
      "ec2:Get*",
    ]
    resources = ["*"]
  }

  statement {
    sid    = "S3ReadOnly"
    effect = "Allow"
    actions = [
      "s3:Get*",
      "s3:List*",
    ]
    resources = ["*"]
  }

  statement {
    sid    = "CloudWatchObservabilityReadOnly"
    effect = "Allow"
    actions = [
      "cloudwatch:Describe*",
      "cloudwatch:Get*",
      "cloudwatch:List*",
      "logs:Describe*",
      "logs:Get*",
      "logs:FilterLogEvents",
    ]
    resources = ["*"]
  }

  statement {
    sid    = "DynamoDBReadOnly"
    effect = "Allow"
    actions = [
      "dynamodb:DescribeTable",
      "dynamodb:ListTables",
      "dynamodb:GetItem",
      "dynamodb:BatchGetItem",
      "dynamodb:Query",
      "dynamodb:Scan",
    ]
    resources = ["*"]
  }
}

# The IAM policy resource
# NOTE: Within the AWS Academy sandbox, applying this resource directly triggers an AccessDenied error.
# It is declared as an IaC template to demonstrate production architecture and least-privilege access design.
resource "aws_iam_policy" "view_only_policy" {
  name        = "ViewOnlyResourcePolicy"
  path        = "/"
  description = "Least-privilege policy granting read/view-only inspection rights across EC2, VPC, S3, CloudWatch, and DynamoDB"
  policy      = data.aws_iam_policy_document.view_only_permissions.json

  tags = {
    Environment = "capstone"
    ManagedBy   = "opentofu"
    Purpose     = "security-baseline"
  }
}

output "view_only_policy_arn" {
  description = "ARN of the view-only IAM policy"
  value       = aws_iam_policy.view_only_policy.arn
}


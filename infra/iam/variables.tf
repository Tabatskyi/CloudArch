variable "aws_region" {
  description = "Target AWS region"
  type        = string
  default     = "us-east-1"
}

variable "trusted_principal_arns" {
  description = "List of AWS Principal ARNs authorized to assume the auditor role"
  type        = list(string)
  default     = ["arn:aws:iam::123456789012:root"] # Placeholder external principal / auditor account
}


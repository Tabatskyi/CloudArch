variable "aws_region" {
  description = "Target AWS region"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Deployment environment"
  type        = string
  default     = "dev"
}

variable "instance_type" {
  description = "EC2 instance size compliant with AWS Academy sandbox quotas"
  type        = string
  default     = "t3.micro"
}

variable "key_name" {
  description = "Optional EC2 Key Pair for SSH access"
  type        = string
  default     = null
}

variable "iam_instance_profile" {
  description = "Optional pre-provisioned IAM instance profile attached to EC2 instances"
  type        = string
  default     = null
}

variable "vpc_id" {
  description = "VPC ID where compute instances will reside"
  type        = string
  default     = null
}

variable "public_subnet_id" {
  description = "Public subnet ID for Coordinator node"
  type        = string
  default     = null
}

variable "private_subnet_ids" {
  description = "List of private subnet IDs for Storage Shard nodes"
  type        = list(string)
  default     = []
}

variable "volume_size" {
  description = "EBS root volume size in GB (sandbox max is 35 GB per volume)"
  type        = number
  default     = 20
}

variable "coordinator_port" {
  description = "TCP port for Coordinator HTTP ingress"
  type        = number
  default     = 8080
}

variable "shard_port" {
  description = "TCP port for Shard HTTP service"
  type        = number
  default     = 8081
}

variable "virtual_nodes" {
  description = "Virtual nodes per physical shard on consistent hash ring"
  type        = number
  default     = 200
}

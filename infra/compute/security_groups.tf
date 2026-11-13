# Security Group for Coordinator Node (Public Subnet)
resource "aws_security_group" "coordinator" {
  name        = "${var.environment}-kv-coordinator-sg"
  description = "Ingress rules for Coordinator REST API and SSH administration"
  vpc_id      = var.vpc_id != null ? var.vpc_id : data.aws_vpc.default[0].id

  # Public REST API client ingress
  ingress {
    description = "OpenAPI REST client ingress"
    from_port   = var.coordinator_port
    to_port     = var.coordinator_port
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # SSH ingress for administration
  ingress {
    description = "SSH administration"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # Outbound egress
  egress {
    description = "Allow all outbound traffic"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "${var.environment}-kv-coordinator-sg"
    Role = "coordinator-security-group"
  }
}

# Security Group for Storage Shard Nodes (Private Subnets)
resource "aws_security_group" "shard" {
  name        = "${var.environment}-kv-shard-sg"
  description = "Private subnet access for downstream storage shards"
  vpc_id      = var.vpc_id != null ? var.vpc_id : data.aws_vpc.default[0].id

  # Shard traffic strictly from Coordinator SG
  ingress {
    description     = "Inbound requests strictly from coordinator security group"
    from_port       = var.shard_port
    to_port         = var.shard_port
    protocol        = "tcp"
    security_groups = [aws_security_group.coordinator.id]
  }

  # SSH jump from Coordinator bastion
  ingress {
    description     = "SSH access strictly from coordinator"
    from_port       = 22
    to_port         = 22
    protocol        = "tcp"
    security_groups = [aws_security_group.coordinator.id]
  }

  # Outbound egress to VPC and coordinator
  egress {
    description = "Allow all outbound traffic"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "${var.environment}-kv-shard-sg"
    Role = "shard-security-group"
  }
}

data "aws_vpc" "default" {
  count   = var.vpc_id == null ? 1 : 0
  default = true
}

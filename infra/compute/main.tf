# Query the latest Amazon Linux 2023 AMI
data "aws_ami" "al2023" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["al2023-ami-2023.*-x86_64"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

data "aws_subnets" "public" {
  filter {
    name   = "vpc-id"
    values = [var.vpc_id != null ? var.vpc_id : data.aws_vpc.default[0].id]
  }
}

locals {
  selected_public_subnet_id   = var.public_subnet_id != null ? var.public_subnet_id : data.aws_subnets.public.ids[0]
  selected_private_subnet_ids = length(var.private_subnet_ids) > 0 ? var.private_subnet_ids : data.aws_subnets.public.ids
}

# Coordinator Node (Public Subnet)
resource "aws_instance" "coordinator" {
  ami                         = data.aws_ami.al2023.id
  instance_type               = var.instance_type
  subnet_id                   = local.selected_public_subnet_id
  associate_public_ip_address = true
  key_name                    = var.key_name
  vpc_security_group_ids      = [aws_security_group.coordinator.id]
  iam_instance_profile        = var.iam_instance_profile

  root_block_device {
    volume_type           = "gp2"
    volume_size           = var.volume_size
    delete_on_termination = true
    encrypted             = false

    tags = {
      Name = "${var.environment}-coordinator-root-vol"
    }
  }

  user_data = <<-EOF
              #!/bin/bash
              set -e
              mkdir -p /opt/kv-store/bin
              mkdir -p /opt/kv-store/config

              # Systemd unit file for coordinator
              cat << 'SERVICE' > /etc/systemd/system/kv-coordinator.service
              [Unit]
              Description=Distributed Key-Value Store Coordinator
              After=network.target

              [Service]
              Type=simple
              User=ec2-user
              WorkingDirectory=/opt/kv-store
              ExecStart=/opt/kv-store/bin/coordinator -port ${var.coordinator_port} -vnodes ${var.virtual_nodes}
              Restart=always
              RestartSec=5
              StandardOutput=journal
              StandardError=journal

              [Install]
              WantedBy=multi-user.target
              SERVICE

              systemctl daemon-reload
              echo "Coordinator bootstrap setup complete"
              EOF

  tags = {
    Name = "${var.environment}-kv-coordinator"
    Role = "coordinator"
    Tier = "ingress"
  }
}

# Storage Shard Nodes (Private Subnets, 2 Shards)
resource "aws_instance" "shards" {
  count                       = 2
  ami                         = data.aws_ami.al2023.id
  instance_type               = var.instance_type
  subnet_id                   = local.selected_private_subnet_ids[count.index % length(local.selected_private_subnet_ids)]
  associate_public_ip_address = false
  key_name                    = var.key_name
  vpc_security_group_ids      = [aws_security_group.shard.id]
  iam_instance_profile        = var.iam_instance_profile

  root_block_device {
    volume_type           = "gp2"
    volume_size           = var.volume_size
    delete_on_termination = true
    encrypted             = false

    tags = {
      Name = "${var.environment}-shard-${count.index + 1}-root-vol"
    }
  }

  user_data = <<-EOF
              #!/bin/bash
              set -e
              mkdir -p /opt/kv-store/bin
              mkdir -p /data/kv-store
              chown -R ec2-user:ec2-user /data/kv-store

              PRIVATE_IP=$(hostname -I | awk '{print $1}')
              SHARD_ID="shard-${count.index + 1}"
              COORD_URL="http://${aws_instance.coordinator.private_ip}:${var.coordinator_port}"

              cat << SERVICE > /etc/systemd/system/kv-shard.service
              [Unit]
              Description=Distributed Key-Value Storage Shard Daemon
              After=network.target

              [Service]
              Type=simple
              User=ec2-user
              WorkingDirectory=/opt/kv-store
              ExecStart=/opt/kv-store/bin/shard -id $${SHARD_ID} -port ${var.shard_port} -advertise $${PRIVATE_IP}:${var.shard_port} -coord $${COORD_URL} -data-dir /data/kv-store -vnodes ${var.virtual_nodes}
              Restart=always
              RestartSec=5
              StandardOutput=journal
              StandardError=journal

              [Install]
              WantedBy=multi-user.target
              SERVICE

              systemctl daemon-reload
              echo "Shard $SHARD_ID bootstrap setup complete"
              EOF

  tags = {
    Name    = "${var.environment}-kv-shard-${count.index + 1}"
    Role    = "storage-shard"
    ShardID = "shard-${count.index + 1}"
  }
}

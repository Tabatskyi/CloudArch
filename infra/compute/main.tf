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

  shard_replicas = {
    for item in flatten([
      for g in range(var.shard_group_count) : [
        for r in range(var.replicas_per_group) : {
          key        = "shard-${g + 1}-r${r + 1}"
          group_id   = "shard-${g + 1}"
          replica_id = "shard-${g + 1}-r${r + 1}"
          role       = r == 0 ? "leader" : "follower"
          group_idx  = g
          rep_idx    = r
        }
      ]
    ]) : item.key => item
  }
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
              ExecStart=/opt/kv-store/bin/coordinator -port ${var.coordinator_port} -vnodes ${var.virtual_nodes} -write-quorum 2 -read-quorum 2
              Restart=always
              RestartSec=5
              StandardOutput=journal
              StandardError=journal
              LimitNOFILE=65536

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

# Storage Shard Replicas (Private Subnets, 2 Groups x 3 Replicas = 6 Instances)
resource "aws_instance" "shards" {
  for_each                    = local.shard_replicas
  ami                         = data.aws_ami.al2023.id
  instance_type               = var.instance_type
  subnet_id                   = local.selected_private_subnet_ids[(each.value.group_idx + each.value.rep_idx) % length(local.selected_private_subnet_ids)]
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
      Name = "${var.environment}-${each.key}-root-vol"
    }
  }

  user_data = <<-EOF
              #!/bin/bash
              set -e
              mkdir -p /opt/kv-store/bin
              mkdir -p /opt/kv-store/config
              mkdir -p /data/kv-store
              chown -R ec2-user:ec2-user /data/kv-store

              PRIVATE_IP=$(hostname -I | awk '{print $1}')
              SHARD_ID="${each.value.group_id}"
              REPLICA_ID="${each.value.replica_id}"
              ROLE="${each.value.role}"
              COORD_URL="http://${aws_instance.coordinator.private_ip}:${var.coordinator_port}"

              cat << ENV > /opt/kv-store/config/shard.env
              SHARD_ID=$${SHARD_ID}
              REPLICA_ID=$${REPLICA_ID}
              SHARD_ROLE=$${ROLE}
              SHARD_ADVERTISE=$${PRIVATE_IP}:${var.shard_port}
              GRPC_ADVERTISE=$${PRIVATE_IP}:${var.grpc_port}
              COORDINATOR_URL=$${COORD_URL}
              ENV

              cat << SERVICE > /etc/systemd/system/kv-shard.service
              [Unit]
              Description=Distributed Key-Value Storage Shard Daemon
              After=network.target

              [Service]
              Type=simple
              User=ec2-user
              WorkingDirectory=/opt/kv-store
              EnvironmentFile=/opt/kv-store/config/shard.env
              ExecStart=/opt/kv-store/bin/shard -id $${SHARD_ID} -replica-id $${REPLICA_ID} -role $${ROLE} -port ${var.shard_port} -advertise $${PRIVATE_IP}:${var.shard_port} -grpc-port ${var.grpc_port} -grpc-advertise $${PRIVATE_IP}:${var.grpc_port} -coord $${COORD_URL} -data-dir /data/kv-store -vnodes ${var.virtual_nodes}
              Restart=always
              RestartSec=5
              StandardOutput=journal
              StandardError=journal
              LimitNOFILE=65536

              [Install]
              WantedBy=multi-user.target
              SERVICE

              systemctl daemon-reload
              echo "Shard $REPLICA_ID ($ROLE) bootstrap setup complete"
              EOF

  tags = {
    Name      = "${var.environment}-kv-${each.key}"
    Role      = each.value.role
    ShardID   = each.value.group_id
    ReplicaID = each.value.replica_id
    Tier      = "storage-shard"
  }
}

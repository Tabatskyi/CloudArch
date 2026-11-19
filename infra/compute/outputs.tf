output "coordinator_public_ip" {
  description = "Public IPv4 address of Coordinator node"
  value       = aws_instance.coordinator.public_ip
}

output "coordinator_private_ip" {
  description = "Private IPv4 address of Coordinator node"
  value       = aws_instance.coordinator.private_ip
}

output "coordinator_api_endpoint" {
  description = "Public HTTP endpoint for client REST requests and YCSB benchmarks"
  value       = "http://${aws_instance.coordinator.public_ip}:${var.coordinator_port}"
}

output "shard_replicas" {
  description = "Provisioned multi-replica storage shards (2 groups x 3 replicas)"
  value = {
    for k, inst in aws_instance.shards : k => {
      group_id   = inst.tags["ShardID"]
      replica_id = inst.tags["ReplicaID"]
      role       = inst.tags["Role"]
      private_ip = inst.private_ip
    }
  }
}

output "shard_private_ips" {
  description = "Private IPv4 addresses of downstream Storage Shard instances"
  value       = [for inst in aws_instance.shards : inst.private_ip]
}

output "shard_ids" {
  description = "Identifiers of provisioned storage shard groups"
  value       = [for g in range(var.shard_group_count) : "shard-${g + 1}"]
}

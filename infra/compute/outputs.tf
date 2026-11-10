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

output "shard_private_ips" {
  description = "Private IPv4 addresses of downstream Storage Shard instances"
  value       = aws_instance.shards[*].private_ip
}

output "shard_ids" {
  description = "Identifiers of provisioned storage shards"
  value       = [for idx in range(length(aws_instance.shards)) : "shard-${idx + 1}"]
}

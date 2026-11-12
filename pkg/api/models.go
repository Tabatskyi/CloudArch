package api

import (
	"time"
)

// TableDefinition describes a table schema with partition and sort keys.
type TableDefinition struct {
	TableName    string `json:"tableName"`
	PartitionKey string `json:"partitionKey"`
	SortKey      string `json:"sortKey,omitempty"`
}

// Record represents a single stored item with compound primary key.
type Record struct {
	PartitionKey string `json:"partitionKey"`
	SortKey      string `json:"sortKey,omitempty"`
	Value        any    `json:"value"`
}

// RecordResponse is returned on successful write operations.
type RecordResponse struct {
	Success      bool   `json:"success"`
	ShardID      string `json:"shardId,omitempty"`
	Table        string `json:"table"`
	PartitionKey string `json:"partitionKey"`
	SortKey      string `json:"sortKey,omitempty"`
}

// ShardRegistration is sent by a shard upon startup.
type ShardRegistration struct {
	ShardID      string `json:"shardId"`
	Address      string `json:"address"`
	VirtualNodes int    `json:"virtualNodes,omitempty"`
}

// ShardInfo represents an active shard registered on the coordinator.
type ShardInfo struct {
	ShardID      string    `json:"shardId"`
	Address      string    `json:"address"`
	Status       string    `json:"status"`
	RegisteredAt time.Time `json:"registeredAt"`
}

// HealthStatus reports service health.
type HealthStatus struct {
	Status        string `json:"status"`
	NodeID        string `json:"nodeId"`
	UptimeSeconds int64  `json:"uptimeSeconds"`
	ActiveShards  int    `json:"activeShards,omitempty"`
}

// ErrorResponse describes an API error.
type ErrorResponse struct {
	Error   string `json:"error"`
	Details string `json:"details,omitempty"`
}

// SuccessResponse represents a generic success acknowledgment.
type SuccessResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message,omitempty"`
}

package shard

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/vasenka88/cloudarch/pkg/api"
	v1 "github.com/vasenka88/cloudarch/pkg/proto/v1"
	"github.com/vasenka88/cloudarch/pkg/storage"
)

// WriteReplicator coordinates distributed replication for write operations.
type WriteReplicator interface {
	ReplicateWrite(ctx context.Context, opType, table, partitionKey, sortKey string, value []byte) (version uint64, ts int64, err error)
}

// ShardGRPCServer implements v1.ShardServiceServer.
type ShardGRPCServer struct {
	v1.UnimplementedShardServiceServer
	engine     *storage.Engine
	replicator WriteReplicator
}

// NewShardGRPCServer creates a new gRPC server handler for ShardService.
func NewShardGRPCServer(engine *storage.Engine, replicator WriteReplicator) *ShardGRPCServer {
	return &ShardGRPCServer{
		engine:     engine,
		replicator: replicator,
	}
}

// SetReplicator sets or updates the WriteReplicator.
func (s *ShardGRPCServer) SetReplicator(r WriteReplicator) {
	s.replicator = r
}

// GetRecord handles internal read queries from coordinator or peer nodes.
func (s *ShardGRPCServer) GetRecord(ctx context.Context, req *v1.GetRecordRequest) (*v1.RecordResponse, error) {
	if req.Table == "" || req.PartitionKey == "" {
		return nil, status.Error(codes.InvalidArgument, "table and partition_key are required")
	}

	rec, err := s.engine.GetRawRecord(req.Table, req.PartitionKey, req.SortKey)
	if err != nil {
		if errors.Is(err, storage.ErrTableNotFound) || errors.Is(err, storage.ErrRecordNotFound) {
			return nil, status.Errorf(codes.NotFound, "record not found: %v", err)
		}
		return nil, status.Errorf(codes.Internal, "storage error: %v", err)
	}

	valBytes := encodeRecordValue(rec.Value)

	return &v1.RecordResponse{
		Table:        req.Table,
		PartitionKey: rec.PartitionKey,
		SortKey:      rec.SortKey,
		Value:        valBytes,
		Version:      rec.Version,
		Timestamp:    rec.Timestamp,
		IsTombstone:  rec.IsTombstone,
	}, nil
}

// PutRecord handles insert/update operations.
func (s *ShardGRPCServer) PutRecord(ctx context.Context, req *v1.PutRecordRequest) (*v1.RecordResponse, error) {
	if req.Table == "" || req.PartitionKey == "" {
		return nil, status.Error(codes.InvalidArgument, "table and partition_key are required")
	}

	// If quorum replication is active, route through replicator
	if s.replicator != nil {
		version, ts, err := s.replicator.ReplicateWrite(ctx, "PUT", req.Table, req.PartitionKey, req.SortKey, req.Value)
		if err != nil {
			return nil, toGRPCError(err)
		}
		return &v1.RecordResponse{
			Table:        req.Table,
			PartitionKey: req.PartitionKey,
			SortKey:      req.SortKey,
			Value:        req.Value,
			Version:      version,
			Timestamp:    ts,
			IsTombstone:  false,
		}, nil
	}

	// Otherwise execute directly on local storage engine
	rec := api.Record{
		PartitionKey: req.PartitionKey,
		SortKey:      req.SortKey,
		Value:        parseRecordValue(req.Value),
	}
	if err := s.engine.PutRecord(req.Table, rec); err != nil {
		return nil, toGRPCError(err)
	}

	stored, err := s.engine.GetRawRecord(req.Table, req.PartitionKey, req.SortKey)
	if err != nil {
		return nil, toGRPCError(err)
	}

	return &v1.RecordResponse{
		Table:        req.Table,
		PartitionKey: stored.PartitionKey,
		SortKey:      stored.SortKey,
		Value:        req.Value,
		Version:      stored.Version,
		Timestamp:    stored.Timestamp,
		IsTombstone:  false,
	}, nil
}

// DeleteRecord handles delete operations.
func (s *ShardGRPCServer) DeleteRecord(ctx context.Context, req *v1.DeleteRecordRequest) (*v1.DeleteRecordResponse, error) {
	if req.Table == "" || req.PartitionKey == "" {
		return nil, status.Error(codes.InvalidArgument, "table and partition_key are required")
	}

	if s.replicator != nil {
		version, ts, err := s.replicator.ReplicateWrite(ctx, "DELETE", req.Table, req.PartitionKey, req.SortKey, nil)
		if err != nil {
			return nil, toGRPCError(err)
		}
		return &v1.DeleteRecordResponse{
			Success:   true,
			Version:   version,
			Timestamp: ts,
		}, nil
	}

	if err := s.engine.DeleteRecord(req.Table, req.PartitionKey, req.SortKey); err != nil {
		return nil, toGRPCError(err)
	}

	tombstone, err := s.engine.GetRawRecord(req.Table, req.PartitionKey, req.SortKey)
	if err != nil {
		return &v1.DeleteRecordResponse{
			Success:   true,
			Version:   1,
			Timestamp: time.Now().UnixNano(),
		}, nil
	}

	return &v1.DeleteRecordResponse{
		Success:   true,
		Version:   tombstone.Version,
		Timestamp: tombstone.Timestamp,
	}, nil
}

// QueryRecords handles intra-partition scans.
func (s *ShardGRPCServer) QueryRecords(ctx context.Context, req *v1.QueryRecordsRequest) (*v1.QueryRecordsResponse, error) {
	if req.Table == "" || req.PartitionKey == "" {
		return nil, status.Error(codes.InvalidArgument, "table and partition_key are required")
	}

	limit := int(req.Limit)
	if limit <= 0 {
		limit = 100
	}

	recs, err := s.engine.QueryPartition(req.Table, req.PartitionKey, "", "", limit)
	if err != nil {
		return nil, toGRPCError(err)
	}

	resp := &v1.QueryRecordsResponse{
		Records: make([]*v1.RecordResponse, 0, len(recs)),
	}

	for _, rec := range recs {
		if req.Prefix != "" && !matchesPrefix(rec.SortKey, req.Prefix) {
			continue
		}
		resp.Records = append(resp.Records, &v1.RecordResponse{
			Table:        req.Table,
			PartitionKey: rec.PartitionKey,
			SortKey:      rec.SortKey,
			Value:        encodeRecordValue(rec.Value),
			Version:      rec.Version,
			Timestamp:    rec.Timestamp,
			IsTombstone:  rec.IsTombstone,
		})
	}

	return resp, nil
}

func matchesPrefix(str, prefix string) bool {
	return len(str) >= len(prefix) && str[:len(prefix)] == prefix
}

func encodeRecordValue(v any) []byte {
	if v == nil {
		return nil
	}
	switch val := v.(type) {
	case []byte:
		return val
	case string:
		return []byte(val)
	case json.RawMessage:
		return []byte(val)
	default:
		b, err := json.Marshal(val)
		if err != nil {
			return nil
		}
		return b
	}
}

func parseRecordValue(data []byte) any {
	if len(data) == 0 {
		return nil
	}
	if json.Valid(data) {
		return json.RawMessage(data)
	}
	return string(data)
}

func toGRPCError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, storage.ErrTableNotFound) || errors.Is(err, storage.ErrRecordNotFound) {
		return status.Errorf(codes.NotFound, "%v", err)
	}
	if errors.Is(err, storage.ErrMissingPartitionKey) || errors.Is(err, storage.ErrMissingSortKey) {
		return status.Errorf(codes.InvalidArgument, "%v", err)
	}
	return status.Errorf(codes.Internal, "%v", err)
}

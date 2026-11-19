package shard

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/vasenka88/cloudarch/pkg/api"
	v1 "github.com/vasenka88/cloudarch/pkg/proto/v1"
	"github.com/vasenka88/cloudarch/pkg/storage"
)

// ReplicationServer implements v1.ReplicationServiceServer.
type ReplicationServer struct {
	v1.UnimplementedReplicationServiceServer
	nodeID      string
	engine      *storage.Engine
	wal         *storage.WAL
	subscribers map[chan *v1.LogEntry]struct{}
	subMutex    sync.Mutex
}

// NewReplicationServer creates a new replication server instance.
func NewReplicationServer(nodeID string, engine *storage.Engine, wal *storage.WAL) *ReplicationServer {
	return &ReplicationServer{
		nodeID:      nodeID,
		engine:      engine,
		wal:         wal,
		subscribers: make(map[chan *v1.LogEntry]struct{}),
	}
}

// BroadcastEntry pushes a newly committed WAL entry to all active log streaming subscribers.
func (rs *ReplicationServer) BroadcastEntry(entry *v1.LogEntry) {
	if entry == nil {
		return
	}
	rs.subMutex.Lock()
	defer rs.subMutex.Unlock()

	for ch := range rs.subscribers {
		select {
		case ch <- entry:
		default:
			// Non-blocking write: if follower buffer is full, drop to prevent backpressure
			log.Printf("[replication:%s] warning: subscriber channel full, dropping real-time entry %d", rs.nodeID, entry.Offset)
		}
	}
}

// StreamLog streams WAL entries from from_offset onwards and pushes live entries as they are committed.
func (rs *ReplicationServer) StreamLog(req *v1.StreamLogRequest, stream grpc.ServerStreamingServer[v1.LogEntry]) error {
	fromOffset := req.FromOffset
	if fromOffset == 0 {
		fromOffset = 1
	}

	// 1. Catch up follower with historical entries from WAL if any
	var lastSentOffset uint64 = 0
	if rs.wal != nil {
		entries, err := rs.wal.ReadFrom(fromOffset, 1000)
		if err != nil {
			return status.Errorf(codes.Internal, "failed reading WAL entries: %v", err)
		}
		for _, e := range entries {
			if err := stream.Send(e); err != nil {
				return err
			}
			lastSentOffset = e.Offset
		}
	}

	// 2. Register subscriber channel for live events
	subChan := make(chan *v1.LogEntry, 200)
	rs.subMutex.Lock()
	rs.subscribers[subChan] = struct{}{}
	rs.subMutex.Unlock()

	defer func() {
		rs.subMutex.Lock()
		delete(rs.subscribers, subChan)
		rs.subMutex.Unlock()
		close(subChan)
	}()

	// 3. Listen for live events or client disconnect
	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case entry, ok := <-subChan:
			if !ok {
				return nil
			}
			if entry.Offset <= lastSentOffset {
				continue
			}
			if err := stream.Send(entry); err != nil {
				return err
			}
			lastSentOffset = entry.Offset
		}
	}
}

// AppendEntries receives WAL mutation batches from the leader and applies them.
func (rs *ReplicationServer) AppendEntries(ctx context.Context, req *v1.AppendEntriesRequest) (*v1.AppendEntriesResponse, error) {
	if len(req.Entries) == 0 {
		var lastOffset uint64
		if rs.wal != nil {
			lastOffset = rs.wal.LastOffset()
		}
		return &v1.AppendEntriesResponse{
			ReplicaId:         rs.nodeID,
			Success:           true,
			LastAppliedOffset: lastOffset,
		}, nil
	}

	var lastApplied uint64
	for _, entry := range req.Entries {
		if rs.wal != nil {
			// If already applied this offset, skip append
			if entry.Offset <= rs.wal.LastOffset() {
				lastApplied = entry.Offset
				continue
			}
			// Wait briefly if in-flight predecessor is appending
			for i := 0; i < 50 && entry.Offset > rs.wal.LastOffset()+1; i++ {
				time.Sleep(2 * time.Millisecond)
			}
			if entry.Offset > rs.wal.LastOffset() {
				if _, err := rs.wal.Append(entry); err != nil {
					return &v1.AppendEntriesResponse{
						ReplicaId:         rs.nodeID,
						Success:           false,
						LastAppliedOffset: rs.wal.LastOffset(),
						ErrorMessage:      fmt.Sprintf("failed appending to follower WAL: %v", err),
					}, nil
				}
			}
		}

		// Apply mutation to local in-memory storage engine
		if rs.engine != nil {
			if entry.OpType == "DELETE" {
				_ = rs.engine.DeleteRecordWithVersion(entry.Table, entry.PartitionKey, entry.SortKey, entry.Offset, entry.Timestamp)
			} else {
				rec := api.Record{
					PartitionKey: entry.PartitionKey,
					SortKey:      entry.SortKey,
					Value:        parseRecordValue(entry.Value),
					Version:      entry.Offset,
					Timestamp:    entry.Timestamp,
				}
				_ = rs.engine.PutRecord(entry.Table, rec)
			}
		}

		lastApplied = entry.Offset
	}

	return &v1.AppendEntriesResponse{
		ReplicaId:         rs.nodeID,
		Success:           true,
		LastAppliedOffset: lastApplied,
	}, nil
}

// SyncRange satisfies follower catch-up resync requests for missing log ranges.
func (rs *ReplicationServer) SyncRange(ctx context.Context, req *v1.SyncRangeRequest) (*v1.SyncRangeResponse, error) {
	if rs.wal == nil {
		return &v1.SyncRangeResponse{
			Entries:             []*v1.LogEntry{},
			CurrentLeaderOffset: 0,
			HasMore:             false,
		}, nil
	}

	limit := int(req.Limit)
	if limit <= 0 {
		limit = 1000
	}

	fromOffset := req.FromOffset
	if fromOffset == 0 {
		fromOffset = 1
	}

	entries, err := rs.wal.ReadFrom(fromOffset, limit)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error reading WAL range: %v", err)
	}

	// Filter entries up to to_offset if specified
	filtered := make([]*v1.LogEntry, 0, len(entries))
	var lastOffset uint64 = 0
	for _, e := range entries {
		if req.ToOffset > 0 && e.Offset > req.ToOffset {
			break
		}
		filtered = append(filtered, e)
		lastOffset = e.Offset
	}

	currLeaderOffset := rs.wal.LastOffset()
	hasMore := currLeaderOffset > lastOffset && len(filtered) >= limit

	return &v1.SyncRangeResponse{
		Entries:             filtered,
		CurrentLeaderOffset: currLeaderOffset,
		HasMore:             hasMore,
	}, nil
}

// VerifyState handles anti-entropy state verification requests.
func (rs *ReplicationServer) VerifyState(ctx context.Context, req *v1.VerifyStateRequest) (*v1.VerifyStateResponse, error) {
	var currOffset uint64
	if rs.wal != nil {
		currOffset = rs.wal.LastOffset()
	}

	stateHash := storage.CalculateEngineStateHash(rs.engine)

	inSync, _ := storage.VerifyStateSync(currOffset, req.Offset, stateHash, req.StateHash)

	return &v1.VerifyStateResponse{
		InSync:          inSync,
		LeaderOffset:    currOffset,
		LeaderStateHash: stateHash,
	}, nil
}

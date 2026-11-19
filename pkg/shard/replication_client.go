package shard

import (
	"context"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/vasenka88/cloudarch/pkg/api"
	v1 "github.com/vasenka88/cloudarch/pkg/proto/v1"
	"github.com/vasenka88/cloudarch/pkg/storage"
)

// ReplicationClient runs on follower shards to synchronize state from the leader.
type ReplicationClient struct {
	mutex         sync.RWMutex
	replicaID     string
	leaderAddr    string
	metaPath      string
	engine        *storage.Engine
	wal           *storage.WAL
	appliedOffset uint64
	conn          *grpc.ClientConn
	client        v1.ReplicationServiceClient
	stopped       bool
	stopChan      chan struct{}
}

// NewReplicationClient constructs a follower replication client.
func NewReplicationClient(replicaID, leaderAddr, metaPath string, engine *storage.Engine, wal *storage.WAL) *ReplicationClient {
	wm, _ := storage.ReadWatermark(metaPath)
	if wal != nil && wal.LastOffset() > wm {
		wm = wal.LastOffset()
	}

	return &ReplicationClient{
		replicaID:     replicaID,
		leaderAddr:    leaderAddr,
		metaPath:      metaPath,
		engine:        engine,
		wal:           wal,
		appliedOffset: wm,
		stopChan:      make(chan struct{}),
	}
}

// Connect dials the leader replication gRPC service.
func (rc *ReplicationClient) Connect(ctx context.Context) error {
	rc.mutex.Lock()
	defer rc.mutex.Unlock()

	if rc.client != nil {
		return nil
	}

	conn, err := grpc.NewClient(
		rc.leaderAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return fmt.Errorf("failed to dial leader at %s: %w", rc.leaderAddr, err)
	}

	rc.conn = conn
	rc.client = v1.NewReplicationServiceClient(conn)
	return nil
}

// SetClient overrides the internal gRPC client (useful for unit testing with bufconn).
func (rc *ReplicationClient) SetClient(client v1.ReplicationServiceClient) {
	rc.mutex.Lock()
	defer rc.mutex.Unlock()
	rc.client = client
}

// LastAppliedOffset returns the follower's current applied watermark.
func (rc *ReplicationClient) LastAppliedOffset() uint64 {
	rc.mutex.RLock()
	defer rc.mutex.RUnlock()
	return rc.appliedOffset
}

// SyncCatchUp executes batch catch-up resync via SyncRange until up-to-date with leader.
func (rc *ReplicationClient) SyncCatchUp(ctx context.Context) (uint64, error) {
	rc.mutex.Lock()
	defer rc.mutex.Unlock()

	if rc.client == nil {
		return rc.appliedOffset, fmt.Errorf("replication client not connected")
	}

	for {
		req := &v1.SyncRangeRequest{
			ReplicaId:  rc.replicaID,
			FromOffset: rc.appliedOffset + 1,
			Limit:      500,
		}

		resp, err := rc.client.SyncRange(ctx, req)
		if err != nil {
			return rc.appliedOffset, fmt.Errorf("SyncRange RPC failed: %w", err)
		}

		if len(resp.Entries) == 0 {
			// Synchronized with leader
			break
		}

		for _, entry := range resp.Entries {
			if err := rc.applyEntry(entry); err != nil {
				return rc.appliedOffset, fmt.Errorf("failed applying catch-up entry %d: %w", entry.Offset, err)
			}
		}

		if !resp.HasMore || rc.appliedOffset >= resp.CurrentLeaderOffset {
			break
		}
	}

	return rc.appliedOffset, nil
}

// StartStreaming begins background consumption of the leader's StreamLog RPC with auto-reconnect.
func (rc *ReplicationClient) StartStreaming(ctx context.Context) {
	go func() {
		backoff := 500 * time.Millisecond
		for {
			select {
			case <-ctx.Done():
				return
			case <-rc.stopChan:
				return
			default:
			}

			// Ensure initial catch-up is complete before streaming
			if _, err := rc.SyncCatchUp(ctx); err != nil {
				log.Printf("[replica:%s] catch-up error: %v (retrying in %v)", rc.replicaID, err, backoff)
				time.Sleep(backoff)
				continue
			}

			rc.mutex.RLock()
			client := rc.client
			from := rc.appliedOffset + 1
			rc.mutex.RUnlock()

			if client == nil {
				time.Sleep(backoff)
				continue
			}

			stream, err := client.StreamLog(ctx, &v1.StreamLogRequest{
				ReplicaId:  rc.replicaID,
				FromOffset: from,
			})
			if err != nil {
				log.Printf("[replica:%s] StreamLog failed: %v, retrying in %v...", rc.replicaID, err, backoff)
				time.Sleep(backoff)
				continue
			}

			// Stream consumption loop
			for {
				entry, err := stream.Recv()
				if err != nil {
					if err != io.EOF && ctx.Err() == nil {
						log.Printf("[replica:%s] replication stream disconnected: %v", rc.replicaID, err)
					}
					break
				}

				rc.mutex.Lock()
				if err := rc.applyEntry(entry); err != nil {
					log.Printf("[replica:%s] error applying streamed entry %d: %v", rc.replicaID, entry.Offset, err)
				}
				rc.mutex.Unlock()
			}

			time.Sleep(backoff)
		}
	}()
}

func (rc *ReplicationClient) applyEntry(entry *v1.LogEntry) error {
	// 1. Append to local WAL if needed
	if rc.wal != nil && entry.Offset > rc.wal.LastOffset() {
		if _, err := rc.wal.Append(entry); err != nil {
			return err
		}
	}

	// 2. Apply to local storage Engine
	if rc.engine != nil {
		if entry.OpType == "DELETE" {
			_ = rc.engine.DeleteRecordWithVersion(entry.Table, entry.PartitionKey, entry.SortKey, entry.Offset, entry.Timestamp)
		} else {
			rec := api.Record{
				PartitionKey: entry.PartitionKey,
				SortKey:      entry.SortKey,
				Value:        parseRecordValue(entry.Value),
				Version:      entry.Offset,
				Timestamp:    entry.Timestamp,
			}
			_ = rc.engine.PutRecord(entry.Table, rec)
		}
	}

	// 3. Update applied watermark
	rc.appliedOffset = entry.Offset
	if rc.metaPath != "" {
		_ = storage.WriteWatermark(rc.metaPath, entry.Offset)
	}

	return nil
}

// Close disconnects the client and stops background workers.
func (rc *ReplicationClient) Close() error {
	rc.mutex.Lock()
	defer rc.mutex.Unlock()

	if rc.stopped {
		return nil
	}
	rc.stopped = true
	close(rc.stopChan)

	if rc.conn != nil {
		return rc.conn.Close()
	}
	return nil
}

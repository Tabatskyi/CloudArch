package shard

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/vasenka88/cloudarch/pkg/api"
	v1 "github.com/vasenka88/cloudarch/pkg/proto/v1"
	"github.com/vasenka88/cloudarch/pkg/storage"
)

var (
	ErrQuorumTimeout     = errors.New("write quorum timeout: insufficient replica acknowledgments")
	ErrQuorumUnavailable = errors.New("write quorum unavailable: not enough reachable replicas")
)

// QuorumCoordinator coordinates multi-node write replication ensuring write quorum (W) is satisfied.
type QuorumCoordinator struct {
	mutex       sync.RWMutex
	writeLock   sync.Mutex
	nodeID      string
	wal         *storage.WAL
	engine      *storage.Engine
	replServer  *ReplicationServer
	writeQuorum int
	timeout     time.Duration
	peerClients map[string]v1.ReplicationServiceClient
	peerConns   map[string]*grpc.ClientConn
}

// NewQuorumCoordinator creates a new QuorumCoordinator for the shard leader.
func NewQuorumCoordinator(nodeID string, wal *storage.WAL, engine *storage.Engine, replServer *ReplicationServer, writeQuorum int, timeout time.Duration) *QuorumCoordinator {
	if writeQuorum <= 0 {
		writeQuorum = 2
	}
	if timeout <= 0 {
		timeout = 1500 * time.Millisecond
	}

	return &QuorumCoordinator{
		nodeID:      nodeID,
		wal:         wal,
		engine:      engine,
		replServer:  replServer,
		writeQuorum: writeQuorum,
		timeout:     timeout,
		peerClients: make(map[string]v1.ReplicationServiceClient),
		peerConns:   make(map[string]*grpc.ClientConn),
	}
}

// AddPeer connects to a peer replica at the given address.
func (qc *QuorumCoordinator) AddPeer(addr string) error {
	qc.mutex.Lock()
	defer qc.mutex.Unlock()

	if _, exists := qc.peerClients[addr]; exists {
		return nil
	}

	conn, err := grpc.NewClient(
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return fmt.Errorf("failed to dial peer replica %s: %w", addr, err)
	}

	qc.peerConns[addr] = conn
	qc.peerClients[addr] = v1.NewReplicationServiceClient(conn)
	return nil
}

// SetPeerClient sets a client directly for testing.
func (qc *QuorumCoordinator) SetPeerClient(peerID string, client v1.ReplicationServiceClient) {
	qc.mutex.Lock()
	defer qc.mutex.Unlock()
	qc.peerClients[peerID] = client
}

// ReplicateWrite implements WriteReplicator.
func (qc *QuorumCoordinator) ReplicateWrite(ctx context.Context, opType, table, partitionKey, sortKey string, value []byte) (uint64, int64, error) {
	qc.writeLock.Lock()
	defer qc.writeLock.Unlock()

	if qc.wal == nil {
		return 0, 0, errors.New("WAL is not initialized on leader")
	}

	// 1. Build log entry with monotonic offset and CRC32 checksum
	entry := storage.NewLogEntry(0, 1, opType, table, partitionKey, sortKey, value)

	// 2. Append to leader local WAL
	offset, err := qc.wal.Append(entry)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to append mutation to leader WAL: %w", err)
	}
	entry.Offset = offset

	// 3. Quorum evaluation: Leader provides 1 ack; we need (writeQuorum - 1) follower acks
	requiredFollowerAcks := qc.writeQuorum - 1
	if requiredFollowerAcks <= 0 {
		// Single-node write quorum (W=1)
		qc.commitLocal(entry)
		return offset, entry.Timestamp, nil
	}

	qc.mutex.RLock()
	peers := make(map[string]v1.ReplicationServiceClient, len(qc.peerClients))
	for id, client := range qc.peerClients {
		peers[id] = client
	}
	qc.mutex.RUnlock()

	// If fewer peers are configured than required for quorum, reject immediately
	if len(peers) < requiredFollowerAcks {
		return 0, 0, status.Errorf(codes.Unavailable, "%v: required %d follower acks but only %d peers configured", ErrQuorumUnavailable, requiredFollowerAcks, len(peers))
	}

	// 4. Parallel fan-out to followers
	quorumCtx, cancel := context.WithTimeout(ctx, qc.timeout)
	defer cancel()

	ackChan := make(chan bool, len(peers))
	req := &v1.AppendEntriesRequest{
		LeaderId: qc.nodeID,
		Term:     1,
		Entries:  []*v1.LogEntry{entry},
	}

	for addr, client := range peers {
		go func(peerAddr string, c v1.ReplicationServiceClient) {
			rpcCtx, rpcCancel := context.WithTimeout(context.Background(), qc.timeout)
			defer rpcCancel()

			resp, err := c.AppendEntries(rpcCtx, req)
			if err == nil && resp != nil && resp.Success {
				ackChan <- true
				return
			}

			// If follower missed preceding entries (e.g. laggy or recovering), heal gap from leader WAL
			if resp != nil && !resp.Success && resp.LastAppliedOffset < entry.Offset-1 && qc.wal != nil {
				missing, readErr := qc.wal.ReadFrom(resp.LastAppliedOffset+1, int(entry.Offset-resp.LastAppliedOffset))
				if readErr == nil && len(missing) > 0 {
					catchUpReq := &v1.AppendEntriesRequest{
						LeaderId: qc.nodeID,
						Term:     1,
						Entries:  missing,
					}
					retryResp, retryErr := c.AppendEntries(rpcCtx, catchUpReq)
					if retryErr == nil && retryResp != nil && retryResp.Success {
						ackChan <- true
						return
					}
				}
			}

			ackChan <- false
		}(addr, client)
	}

	// 5. Await required follower acks
	acks := 0
	failures := 0
	maxFailures := len(peers) - requiredFollowerAcks

	for acks < requiredFollowerAcks {
		select {
		case <-quorumCtx.Done():
			return 0, 0, status.Errorf(codes.DeadlineExceeded, "%v: timed out waiting for %d follower acks (received %d)", ErrQuorumTimeout, requiredFollowerAcks, acks)
		case success := <-ackChan:
			if success {
				acks++
			} else {
				failures++
				if failures > maxFailures {
					return 0, 0, status.Errorf(codes.Unavailable, "%v: failed follower acks exceeded threshold", ErrQuorumUnavailable)
				}
			}
		}
	}

	// 6. Quorum satisfied: Commit mutation to local engine and broadcast to live streams
	qc.commitLocal(entry)

	return offset, entry.Timestamp, nil
}

func (qc *QuorumCoordinator) commitLocal(entry *v1.LogEntry) {
	if qc.engine != nil {
		if entry.OpType == "DELETE" {
			_ = qc.engine.DeleteRecordWithVersion(entry.Table, entry.PartitionKey, entry.SortKey, entry.Offset, entry.Timestamp)
		} else {
			rec := api.Record{
				PartitionKey: entry.PartitionKey,
				SortKey:      entry.SortKey,
				Value:        parseRecordValue(entry.Value),
				Version:      entry.Offset,
				Timestamp:    entry.Timestamp,
			}
			_ = qc.engine.PutRecord(entry.Table, rec)
		}
	}

	if qc.replServer != nil {
		qc.replServer.BroadcastEntry(entry)
	}
}

// Close closes all open peer connections.
func (qc *QuorumCoordinator) Close() error {
	qc.mutex.Lock()
	defer qc.mutex.Unlock()

	for _, conn := range qc.peerConns {
		_ = conn.Close()
	}
	qc.peerClients = make(map[string]v1.ReplicationServiceClient)
	qc.peerConns = make(map[string]*grpc.ClientConn)
	return nil
}

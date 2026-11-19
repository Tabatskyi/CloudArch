package shard

import (
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/vasenka88/cloudarch/pkg/api"
	v1 "github.com/vasenka88/cloudarch/pkg/proto/v1"
	"github.com/vasenka88/cloudarch/pkg/storage"
)

type mockReplicationPeer struct {
	v1.UnimplementedReplicationServiceServer
	shouldFail bool
	delay      time.Duration
}

func (m *mockReplicationPeer) AppendEntries(ctx context.Context, req *v1.AppendEntriesRequest) (*v1.AppendEntriesResponse, error) {
	if m.delay > 0 {
		select {
		case <-time.After(m.delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if m.shouldFail {
		return &v1.AppendEntriesResponse{Success: false, ErrorMessage: "simulated disk error"}, nil
	}
	return &v1.AppendEntriesResponse{Success: true, LastAppliedOffset: req.Entries[0].Offset}, nil
}

func TestQuorumCoordinator_Success(t *testing.T) {
	tmpDir := t.TempDir()
	wal, err := storage.OpenWAL(filepath.Join(tmpDir, "leader.wal"))
	if err != nil {
		t.Fatalf("OpenWAL failed: %v", err)
	}
	defer wal.Close()

	engine := storage.NewEngine("")
	_ = engine.CreateTable(api.TableDefinition{TableName: "users", PartitionKey: "id"})

	qc := NewQuorumCoordinator("leader-1", wal, engine, nil, 2, 500*time.Millisecond)

	// Set up 2 mock followers: 1 fast success, 1 failing
	f1Client, _, f1Cleanup := setupTestMockPeer(t, &mockReplicationPeer{shouldFail: false})
	defer f1Cleanup()
	f2Client, _, f2Cleanup := setupTestMockPeer(t, &mockReplicationPeer{shouldFail: true})
	defer f2Cleanup()

	qc.SetPeerClient("follower-1", f1Client)
	qc.SetPeerClient("follower-2", f2Client)

	// Replicate write: since W=2, 1 follower ack + leader is sufficient!
	ctx := context.Background()
	version, ts, err := qc.ReplicateWrite(ctx, "PUT", "users", "u1", "", []byte("Alice"))
	if err != nil {
		t.Fatalf("ReplicateWrite failed: %v", err)
	}

	if version != 1 {
		t.Errorf("expected committed version 1, got %d", version)
	}
	if ts == 0 {
		t.Error("expected non-zero timestamp")
	}

	// Verify leader engine has the record
	rec, err := engine.GetRecord("users", "u1", "")
	if err != nil || rec.Value != "Alice" {
		t.Fatalf("expected Alice in leader engine, got %+v, err=%v", rec, err)
	}
}

func TestQuorumCoordinator_TimeoutFailure(t *testing.T) {
	tmpDir := t.TempDir()
	wal, _ := storage.OpenWAL(filepath.Join(tmpDir, "leader.wal"))
	defer wal.Close()

	engine := storage.NewEngine("")
	_ = engine.CreateTable(api.TableDefinition{TableName: "users", PartitionKey: "id"})

	// Timeout is 100ms
	qc := NewQuorumCoordinator("leader-1", wal, engine, nil, 2, 100*time.Millisecond)

	// Peer delays 500ms
	slowPeer, _, cleanup := setupTestMockPeer(t, &mockReplicationPeer{delay: 500 * time.Millisecond})
	defer cleanup()

	qc.SetPeerClient("follower-slow", slowPeer)

	ctx := context.Background()
	_, _, err := qc.ReplicateWrite(ctx, "PUT", "users", "u1", "", []byte("Alice"))
	if err == nil {
		t.Fatal("expected quorum timeout error, got nil")
	}

	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.DeadlineExceeded {
		t.Fatalf("expected DeadlineExceeded error code, got %v", st.Code())
	}
}

func setupTestMockPeer(t *testing.T, peer v1.ReplicationServiceServer) (v1.ReplicationServiceClient, *grpc.Server, func()) {
	lis := bufconn.Listen(1024 * 1024)
	s := grpc.NewServer()
	v1.RegisterReplicationServiceServer(s, peer)

	go func() {
		_ = s.Serve(lis)
	}()

	conn, _ := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)

	return v1.NewReplicationServiceClient(conn), s, func() {
		_ = conn.Close()
		s.Stop()
		_ = lis.Close()
	}
}

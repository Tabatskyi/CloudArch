package shard

import (
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/vasenka88/cloudarch/pkg/api"
	v1 "github.com/vasenka88/cloudarch/pkg/proto/v1"
	"github.com/vasenka88/cloudarch/pkg/storage"
)

func setupTestReplicationServer(t *testing.T) (v1.ReplicationServiceClient, *ReplicationServer, *storage.WAL, *storage.Engine, func()) {
	lis := bufconn.Listen(1024 * 1024)
	s := grpc.NewServer()

	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")
	wal, err := storage.OpenWAL(walPath)
	if err != nil {
		t.Fatalf("OpenWAL failed: %v", err)
	}

	engine := storage.NewEngine("")
	_ = engine.CreateTable(api.TableDefinition{TableName: "kv", PartitionKey: "pk"})

	replServer := NewReplicationServer("leader-1", engine, wal)
	v1.RegisterReplicationServiceServer(s, replServer)

	go func() {
		_ = s.Serve(lis)
	}()

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}

	client := v1.NewReplicationServiceClient(conn)
	cleanup := func() {
		_ = conn.Close()
		s.Stop()
		_ = lis.Close()
		_ = wal.Close()
	}

	return client, replServer, wal, engine, cleanup
}

func TestReplicationServer_SyncRange(t *testing.T) {
	client, _, wal, _, cleanup := setupTestReplicationServer(t)
	defer cleanup()

	// Append 10 entries to WAL
	for i := 1; i <= 10; i++ {
		e := storage.NewLogEntry(uint64(i), 1, "PUT", "kv", "k1", "", []byte("v"))
		_, _ = wal.Append(e)
	}

	ctx := context.Background()
	resp, err := client.SyncRange(ctx, &v1.SyncRangeRequest{
		ReplicaId:  "follower-1",
		FromOffset: 5,
		ToOffset:   8,
		Limit:      10,
	})
	if err != nil {
		t.Fatalf("SyncRange failed: %v", err)
	}

	if len(resp.Entries) != 4 {
		t.Fatalf("expected 4 entries (5,6,7,8), got %d", len(resp.Entries))
	}
	if resp.Entries[0].Offset != 5 || resp.Entries[3].Offset != 8 {
		t.Errorf("unexpected offsets: start=%d, end=%d", resp.Entries[0].Offset, resp.Entries[3].Offset)
	}
	if resp.CurrentLeaderOffset != 10 {
		t.Errorf("expected leader offset 10, got %d", resp.CurrentLeaderOffset)
	}
}

func TestReplicationServer_AppendEntries(t *testing.T) {
	client, _, _, engine, cleanup := setupTestReplicationServer(t)
	defer cleanup()

	ctx := context.Background()
	entries := []*v1.LogEntry{
		storage.NewLogEntry(1, 1, "PUT", "kv", "key-a", "", []byte("val-a")),
		storage.NewLogEntry(2, 1, "PUT", "kv", "key-b", "", []byte("val-b")),
	}

	resp, err := client.AppendEntries(ctx, &v1.AppendEntriesRequest{
		LeaderId: "leader-1",
		Term:     1,
		Entries:  entries,
	})
	if err != nil {
		t.Fatalf("AppendEntries failed: %v", err)
	}
	if !resp.Success {
		t.Fatalf("expected success, got error: %s", resp.ErrorMessage)
	}
	if resp.LastAppliedOffset != 2 {
		t.Errorf("expected last applied offset 2, got %d", resp.LastAppliedOffset)
	}

	// Verify records were applied to engine
	recA, err := engine.GetRecord("kv", "key-a", "")
	if err != nil || recA.Value != "val-a" {
		t.Fatalf("expected key-a in engine, got rec: %+v, err: %v", recA, err)
	}
}

func TestReplicationServer_StreamLog_HistoricalAndLive(t *testing.T) {
	client, server, wal, _, cleanup := setupTestReplicationServer(t)
	defer cleanup()

	// Append 2 historical entries
	_, _ = wal.Append(storage.NewLogEntry(1, 1, "PUT", "kv", "k1", "", []byte("v1")))
	_, _ = wal.Append(storage.NewLogEntry(2, 1, "PUT", "kv", "k2", "", []byte("v2")))

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	stream, err := client.StreamLog(ctx, &v1.StreamLogRequest{
		ReplicaId:  "follower-1",
		FromOffset: 1,
	})
	if err != nil {
		t.Fatalf("StreamLog failed: %v", err)
	}

	// Receive historical 1 and 2
	e1, err := stream.Recv()
	if err != nil || e1.Offset != 1 {
		t.Fatalf("expected entry 1, got %+v, err=%v", e1, err)
	}
	e2, err := stream.Recv()
	if err != nil || e2.Offset != 2 {
		t.Fatalf("expected entry 2, got %+v, err=%v", e2, err)
	}

	// Broadcast live entry 3
	go func() {
		time.Sleep(50 * time.Millisecond)
		liveEntry := storage.NewLogEntry(3, 1, "PUT", "kv", "k3", "", []byte("v3"))
		_, _ = wal.Append(liveEntry)
		server.BroadcastEntry(liveEntry)
	}()

	e3, err := stream.Recv()
	if err != nil || e3.Offset != 3 {
		t.Fatalf("expected live entry 3, got %+v, err=%v", e3, err)
	}
}

func TestReplicationServer_VerifyState(t *testing.T) {
	client, _, _, engine, cleanup := setupTestReplicationServer(t)
	defer cleanup()

	_ = engine.PutRecord("kv", api.Record{PartitionKey: "p1", Value: "val1", Version: 1, Timestamp: 100})
	stateHash := storage.CalculateEngineStateHash(engine)

	ctx := context.Background()
	resp, err := client.VerifyState(ctx, &v1.VerifyStateRequest{
		ReplicaId: "follower-1",
		Offset:    0,
		StateHash: stateHash,
	})
	if err != nil {
		t.Fatalf("VerifyState failed: %v", err)
	}
	if !resp.InSync {
		t.Fatal("expected in_sync=true")
	}

	// Verify mismatch
	respMismatch, err := client.VerifyState(ctx, &v1.VerifyStateRequest{
		ReplicaId: "follower-1",
		Offset:    0,
		StateHash: "diverged-hash-12345",
	})
	if err != nil {
		t.Fatalf("VerifyState mismatch check failed: %v", err)
	}
	if respMismatch.InSync {
		t.Fatal("expected in_sync=false on hash mismatch")
	}
}

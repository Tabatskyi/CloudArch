package shard

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/vasenka88/cloudarch/pkg/api"
	"github.com/vasenka88/cloudarch/pkg/storage"
)

func TestReplicationClient_CatchUpAndWatermarkPersistence(t *testing.T) {
	client, _, leaderWAL, _, cleanup := setupTestReplicationServer(t)
	defer cleanup()

	// 1. Leader commits 5 entries
	for i := 1; i <= 5; i++ {
		e := storage.NewLogEntry(uint64(i), 1, "PUT", "kv", "user#1", "", []byte("val1"))
		_, _ = leaderWAL.Append(e)
	}

	tmpDir := t.TempDir()
	metaPath := filepath.Join(tmpDir, "offset.meta")
	followerWALPath := filepath.Join(tmpDir, "follower.wal")
	followerWAL, err := storage.OpenWAL(followerWALPath)
	if err != nil {
		t.Fatalf("follower OpenWAL failed: %v", err)
	}
	defer followerWAL.Close()

	followerEngine := storage.NewEngine("")
	_ = followerEngine.CreateTable(api.TableDefinition{TableName: "kv", PartitionKey: "pk"})

	// Create follower client
	rc := NewReplicationClient("follower-1", "passthrough://bufnet", metaPath, followerEngine, followerWAL)
	rc.SetClient(client)

	// Initial watermark should be 0
	if rc.LastAppliedOffset() != 0 {
		t.Fatalf("expected initial offset 0, got %d", rc.LastAppliedOffset())
	}

	// 2. Perform catch-up resync
	ctx := context.Background()
	applied, err := rc.SyncCatchUp(ctx)
	if err != nil {
		t.Fatalf("SyncCatchUp failed: %v", err)
	}
	if applied != 5 {
		t.Fatalf("expected applied 5, got %d", applied)
	}

	// Verify follower engine has the data
	rec, err := followerEngine.GetRecord("kv", "user#1", "")
	if err != nil || rec.Value != "val1" {
		t.Fatalf("expected record in follower engine, got %+v, err=%v", rec, err)
	}

	// Verify watermark persisted to disk
	wm, err := storage.ReadWatermark(metaPath)
	if err != nil || wm != 5 {
		t.Fatalf("expected persisted watermark 5, got %d, err=%v", wm, err)
	}

	// 3. Simulate Follower Restart
	rcRestarted := NewReplicationClient("follower-1", "passthrough://bufnet", metaPath, followerEngine, followerWAL)
	rcRestarted.SetClient(client)

	if rcRestarted.LastAppliedOffset() != 5 {
		t.Fatalf("expected restarted client to restore watermark 5, got %d", rcRestarted.LastAppliedOffset())
	}

	// Re-syncing when already up to date should apply 0 entries
	applied2, err := rcRestarted.SyncCatchUp(ctx)
	if err != nil || applied2 != 5 {
		t.Fatalf("expected applied2 5, got %d, err=%v", applied2, err)
	}
}

func TestReplicationClient_LiveStreaming(t *testing.T) {
	client, server, leaderWAL, _, cleanup := setupTestReplicationServer(t)
	defer cleanup()

	tmpDir := t.TempDir()
	metaPath := filepath.Join(tmpDir, "offset.meta")
	followerWALPath := filepath.Join(tmpDir, "follower.wal")
	followerWAL, _ := storage.OpenWAL(followerWALPath)
	defer followerWAL.Close()

	followerEngine := storage.NewEngine("")
	_ = followerEngine.CreateTable(api.TableDefinition{TableName: "kv", PartitionKey: "pk"})

	rc := NewReplicationClient("follower-1", "passthrough://bufnet", metaPath, followerEngine, followerWAL)
	rc.SetClient(client)
	defer rc.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rc.StartStreaming(ctx)

	// Allow streaming loop to initialize
	time.Sleep(100 * time.Millisecond)

	// Leader writes new entry and broadcasts
	entry := storage.NewLogEntry(1, 1, "PUT", "kv", "live-key", "", []byte("live-val"))
	_, _ = leaderWAL.Append(entry)
	server.BroadcastEntry(entry)

	// Await entry reception on follower
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if rc.LastAppliedOffset() == 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if rc.LastAppliedOffset() != 1 {
		t.Fatalf("expected applied offset 1, got %d", rc.LastAppliedOffset())
	}

	rec, err := followerEngine.GetRecord("kv", "live-key", "")
	if err != nil || rec.Value != "live-val" {
		t.Fatalf("expected live-key in follower engine, got %+v, err=%v", rec, err)
	}
}

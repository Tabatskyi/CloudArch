package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/vasenka88/cloudarch/pkg/api"
	"github.com/vasenka88/cloudarch/pkg/coordinator"
	"github.com/vasenka88/cloudarch/pkg/shard"
	"github.com/vasenka88/cloudarch/pkg/storage"
)

// setupReplica creates a storage shard node with gRPC, WAL, and storage engine.
func setupReplica(t *testing.T, shardID, replicaID, role string, peerAddrs []string) (*shard.Server, *httptest.Server, *storage.WAL, func()) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, replicaID+".wal")
	wal, err := storage.OpenWAL(walPath)
	if err != nil {
		t.Fatalf("failed opening WAL for %s: %v", replicaID, err)
	}

	s := shard.NewServer(shard.Config{
		ShardID:        shardID,
		ReplicaID:      replicaID,
		Role:           role,
		ListenAddr:     ":0",
		GRPCListenAddr: "127.0.0.1:0",
		DataDir:        tmpDir,
		VirtualNodes:   50,
		WriteQuorum:    2,
		ReadQuorum:     2,
	})

	s.SetWAL(wal)
	replServer := shard.NewReplicationServer(replicaID, s.Engine(), wal)
	s.SetReplicationServer(replServer)

	if err := s.StartGRPC(); err != nil {
		t.Fatalf("failed starting gRPC for %s: %v", replicaID, err)
	}

	ts := httptest.NewServer(s)

	cleanup := func() {
		ts.Close()
		_ = s.Stop(context.Background())
		_ = wal.Close()
	}

	return s, ts, wal, cleanup
}

// Task 7.1: Multi-Replica Integration Test Suite
func TestMultiReplica_QuorumReadWriteConsistency(t *testing.T) {
	// 1. Start 2 Followers
	f1, tsF1, _, cleanupF1 := setupReplica(t, "group-1", "rep-f1", "follower", nil)
	defer cleanupF1()
	addrF1 := strings.TrimPrefix(tsF1.URL, "http://")
	grpcF1 := f1.GRPCAddr()

	f2, tsF2, _, cleanupF2 := setupReplica(t, "group-1", "rep-f2", "follower", nil)
	defer cleanupF2()
	addrF2 := strings.TrimPrefix(tsF2.URL, "http://")
	grpcF2 := f2.GRPCAddr()

	// 2. Start Leader
	leader, tsLeader, leaderWAL, cleanupLeader := setupReplica(t, "group-1", "rep-leader", "leader", nil)
	defer cleanupLeader()
	addrLeader := strings.TrimPrefix(tsLeader.URL, "http://")
	grpcLeader := leader.GRPCAddr()

	// Configure Leader Quorum Coordinator with follower peers (W=2: 1 follower ack + leader is sufficient)
	qc := shard.NewQuorumCoordinator("rep-leader", leaderWAL, leader.Engine(), nil, 2, 2*time.Second)
	if err := qc.AddPeer(grpcF1); err != nil {
		t.Fatalf("failed connecting leader to follower 1: %v", err)
	}
	if err := qc.AddPeer(grpcF2); err != nil {
		t.Fatalf("failed connecting leader to follower 2: %v", err)
	}
	leader.SetQuorumCoordinator(qc)

	// 3. Start Coordinator
	coord := coordinator.NewServer(coordinator.Config{
		ListenAddr:   ":0",
		VirtualNodes: 50,
		WriteQuorum:  2,
		ReadQuorum:   2,
	})
	coordServer := httptest.NewServer(coord)
	defer coordServer.Close()
	coordClient := coordServer.Client()

	// Register all 3 replicas
	for _, reg := range []api.ShardRegistration{
		{ShardID: "group-1", ReplicaID: "rep-leader", Role: "leader", GRPCAddress: grpcLeader, HTTPAddress: addrLeader},
		{ShardID: "group-1", ReplicaID: "rep-f1", Role: "follower", GRPCAddress: grpcF1, HTTPAddress: addrF1},
		{ShardID: "group-1", ReplicaID: "rep-f2", Role: "follower", GRPCAddress: grpcF2, HTTPAddress: addrF2},
	} {
		b, _ := json.Marshal(reg)
		resp, err := coordClient.Post(coordServer.URL+"/internal/v1/shards", "application/json", bytes.NewReader(b))
		if err != nil || resp.StatusCode != http.StatusOK {
			t.Fatalf("failed registering %s: status %v", reg.ReplicaID, resp.StatusCode)
		}
		_ = resp.Body.Close()
	}

	// 4. Create table
	tblDef := api.TableDefinition{TableName: "inventory", PartitionKey: "sku"}
	b, _ := json.Marshal(tblDef)
	resp, err := coordClient.Post(coordServer.URL+"/api/v1/tables", "application/json", bytes.NewReader(b))
	if err != nil || resp.StatusCode != http.StatusCreated {
		t.Fatalf("failed creating table inventory: %v", resp.StatusCode)
	}
	_ = resp.Body.Close()

	// 5. Quorum Write (W=2)
	item := api.Record{
		PartitionKey: "sku-laptop-01",
		Value:        map[string]interface{}{"name": "ThinkPad", "qty": float64(42)},
	}
	itemBytes, _ := json.Marshal(item)
	resp, err = coordClient.Post(coordServer.URL+"/api/v1/tables/inventory/records", "application/json", bytes.NewReader(itemBytes))
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("quorum write failed: %v", resp.StatusCode)
	}
	var writeResp api.RecordResponse
	_ = json.NewDecoder(resp.Body).Decode(&writeResp)
	_ = resp.Body.Close()

	if writeResp.Version != 1 {
		t.Errorf("expected version 1, got %d", writeResp.Version)
	}

	// Verify at least one follower received and persisted the write
	f1Rec, errF1 := f1.Engine().GetRecord("inventory", "sku-laptop-01", "")
	f2Rec, errF2 := f2.Engine().GetRecord("inventory", "sku-laptop-01", "")
	fCount := 0
	if errF1 == nil && f1Rec.Version == 1 {
		fCount++
	}
	if errF2 == nil && f2Rec.Version == 1 {
		fCount++
	}
	if fCount == 0 {
		t.Fatalf("neither follower persisted the write: f1Err=%v, f2Err=%v", errF1, errF2)
	}
	t.Logf("Write acknowledged and committed on leader and %d followers (W=2 satisfied)", fCount)

	// 6. Quorum Read (R=2)
	resp, err = coordClient.Get(coordServer.URL + "/api/v1/tables/inventory/records/sku-laptop-01")
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("quorum read failed: status %v", resp.StatusCode)
	}
	var readRec api.Record
	_ = json.NewDecoder(resp.Body).Decode(&readRec)
	_ = resp.Body.Close()

	if readRec.Version != 1 {
		t.Errorf("expected version 1 from quorum read, got %d", readRec.Version)
	}
}

// Task 7.1: Node Crash and Catch-Up Resync Simulation
func TestMultiReplica_NodeCrashAndCatchUpResync(t *testing.T) {
	// Setup 2 Followers and 1 Leader
	f1, tsF1, _, cleanupF1 := setupReplica(t, "shard-group-a", "follower-1", "follower", nil)
	defer cleanupF1()
	addrF1 := strings.TrimPrefix(tsF1.URL, "http://")
	grpcF1 := f1.GRPCAddr()

	f2, tsF2, _, cleanupF2 := setupReplica(t, "shard-group-a", "follower-2", "follower", nil)
	addrF2 := strings.TrimPrefix(tsF2.URL, "http://")
	grpcF2 := f2.GRPCAddr()

	leader, tsLeader, leaderWAL, cleanupLeader := setupReplica(t, "shard-group-a", "leader-node", "leader", nil)
	defer cleanupLeader()
	addrLeader := strings.TrimPrefix(tsLeader.URL, "http://")
	grpcLeader := leader.GRPCAddr()

	qc := shard.NewQuorumCoordinator("leader-node", leaderWAL, leader.Engine(), nil, 2, 2*time.Second)
	_ = qc.AddPeer(grpcF1)
	_ = qc.AddPeer(grpcF2)
	leader.SetQuorumCoordinator(qc)

	coord := coordinator.NewServer(coordinator.Config{VirtualNodes: 50, WriteQuorum: 2, ReadQuorum: 2})
	coordServer := httptest.NewServer(coord)
	defer coordServer.Close()
	client := coordServer.Client()

	for _, reg := range []api.ShardRegistration{
		{ShardID: "shard-group-a", ReplicaID: "leader-node", Role: "leader", GRPCAddress: grpcLeader, HTTPAddress: addrLeader},
		{ShardID: "shard-group-a", ReplicaID: "follower-1", Role: "follower", GRPCAddress: grpcF1, HTTPAddress: addrF1},
		{ShardID: "shard-group-a", ReplicaID: "follower-2", Role: "follower", GRPCAddress: grpcF2, HTTPAddress: addrF2},
	} {
		b, _ := json.Marshal(reg)
		_, _ = client.Post(coordServer.URL+"/internal/v1/shards", "application/json", bytes.NewReader(b))
	}

	tblDef := api.TableDefinition{TableName: "orders", PartitionKey: "order_id"}
	b, _ := json.Marshal(tblDef)
	_, _ = client.Post(coordServer.URL+"/api/v1/tables", "application/json", bytes.NewReader(b))

	// 1. Initial write with all 3 replicas alive
	w1, _ := json.Marshal(api.Record{PartitionKey: "ord-101", Value: "state=created"})
	resp, err := client.Post(coordServer.URL+"/api/v1/tables/orders/records", "application/json", bytes.NewReader(w1))
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("first write failed: %v", resp.StatusCode)
	}
	_ = resp.Body.Close()

	// 2. Simulate Node Crash: Follower 2 goes offline!
	t.Log("Simulating crash of Follower 2...")
	cleanupF2()

	// 3. Write second record while Follower 2 is offline:
	// Since Leader and Follower 1 are still alive, W=2 is satisfied!
	w2, _ := json.Marshal(api.Record{PartitionKey: "ord-102", Value: "state=paid"})
	resp, err = client.Post(coordServer.URL+"/api/v1/tables/orders/records", "application/json", bytes.NewReader(w2))
	if err != nil || resp.StatusCode != http.StatusOK {
		var errBuf bytes.Buffer
		if resp != nil {
			_, _ = errBuf.ReadFrom(resp.Body)
		}
		t.Fatalf("write during follower crash should succeed with surviving quorum, got %v, body: %s", resp.StatusCode, errBuf.String())
	}
	_ = resp.Body.Close()

	// Follower 1 has the write
	if _, err := f1.Engine().GetRecord("orders", "ord-102", ""); err != nil {
		t.Fatalf("expected follower 1 to have ord-102: %v", err)
	}
	t.Log("Write during follower outage succeeded under W=2 quorum!")

	// 4. Simulate Follower 2 restart and catch-up resync
	t.Log("Simulating restart of Follower 2 and catch-up resync...")
	f2Recovered, _, f2RecoveredWAL, cleanupF2Recovered := setupReplica(t, "shard-group-a", "follower-2", "follower", nil)
	defer cleanupF2Recovered()

	// Ensure recovered follower has the table schema
	_ = f2Recovered.Engine().CreateTable(tblDef)

	// Recovered follower replication client connects to leader and synchronizes
	metaPath := filepath.Join(t.TempDir(), "f2.meta")
	replClient := shard.NewReplicationClient("follower-2", grpcLeader, metaPath, f2Recovered.Engine(), f2RecoveredWAL)
	if err := replClient.Connect(context.Background()); err != nil {
		t.Fatalf("failed connecting recovered follower to leader: %v", err)
	}
	defer replClient.Close()

	// Execute catch-up resync
	caughtUpOffset, err := replClient.SyncCatchUp(context.Background())
	if err != nil {
		t.Fatalf("SyncCatchUp failed: %v", err)
	}
	if caughtUpOffset < 2 {
		t.Fatalf("expected caught up offset >= 2, got %d", caughtUpOffset)
	}

	// Verify recovered Follower 2 now has ord-102!
	rec, err := f2Recovered.Engine().GetRecord("orders", "ord-102", "")
	if err != nil {
		t.Fatalf("recovered follower 2 missing ord-102 after catch-up: %v", err)
	}
	if rec.Value != "state=paid" {
		t.Errorf("unexpected value in recovered follower 2: %v", rec.Value)
	}
	t.Logf("Follower 2 successfully caught up to offset %d and restored state parity!", caughtUpOffset)

	// 5. Verify Anti-Entropy State Checksum Parity
	leaderHash := storage.CalculateEngineStateHash(leader.Engine())
	f2Hash := storage.CalculateEngineStateHash(f2Recovered.Engine())
	synced, reason := storage.VerifyStateSync(2, caughtUpOffset, leaderHash, f2Hash)
	if !synced {
		t.Fatalf("state sync verification failed: %s", reason)
	}
	t.Logf("Anti-entropy state hash verified: %s matches between leader and recovered follower!", leaderHash)
}

package coordinator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/vasenka88/cloudarch/pkg/api"
	v1 "github.com/vasenka88/cloudarch/pkg/proto/v1"
	"github.com/vasenka88/cloudarch/pkg/shard"
)

func startMockShardGRPC(t *testing.T, svc *mockShardService) (string, func()) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen on 127.0.0.1:0: %v", err)
	}
	s := grpc.NewServer()
	v1.RegisterShardServiceServer(s, svc)
	go func() {
		_ = s.Serve(lis)
	}()
	return lis.Addr().String(), func() {
		s.Stop()
		_ = lis.Close()
	}
}

// Task 5.1: TestCoordinator_ShardRegistration
func TestCoordinator_ShardRegistration(t *testing.T) {
	coord := NewServer(Config{ListenAddr: ":8080", VirtualNodes: 50})

	// Register 3 replicas for shard-group-1 (1 leader, 2 followers)
	replicas := []api.ShardRegistration{
		{
			ShardID:      "group-1",
			ReplicaID:    "replica-1",
			Role:         "leader",
			GRPCAddress:  "127.0.0.1:9091",
			HTTPAddress:  "127.0.0.1:8081",
			VirtualNodes: 50,
		},
		{
			ShardID:      "group-1",
			ReplicaID:    "replica-2",
			Role:         "follower",
			GRPCAddress:  "127.0.0.1:9092",
			HTTPAddress:  "127.0.0.1:8082",
			VirtualNodes: 50,
		},
		{
			ShardID:      "group-1",
			ReplicaID:    "replica-3",
			Role:         "follower",
			GRPCAddress:  "127.0.0.1:9093",
			HTTPAddress:  "127.0.0.1:8083",
			VirtualNodes: 50,
		},
	}

	for _, rep := range replicas {
		body, _ := json.Marshal(rep)
		req := httptest.NewRequest(http.MethodPost, "/internal/v1/shards", bytes.NewReader(body))
		rec := httptest.NewRecorder()
		coord.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("failed to register replica %s: %d", rep.ReplicaID, rec.Code)
		}
	}

	// Verify ring topology
	group, err := coord.Ring().GetReplicaGroup("any-key")
	if err != nil {
		t.Fatalf("failed to get replica group: %v", err)
	}
	if group.GroupID != "group-1" {
		t.Errorf("expected group-1, got %s", group.GroupID)
	}
	if len(group.Members) != 3 {
		t.Errorf("expected 3 replica members in group-1, got %d", len(group.Members))
	}
	if group.Leader == nil || group.Leader.ReplicaID != "replica-1" {
		t.Errorf("expected leader replica-1, got %+v", group.Leader)
	}

	// List registered shards via GET /internal/v1/shards
	req := httptest.NewRequest(http.MethodGet, "/internal/v1/shards", nil)
	rec := httptest.NewRecorder()
	coord.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var list []api.ShardInfo
	_ = json.NewDecoder(rec.Body).Decode(&list)
	if len(list) != 3 {
		t.Fatalf("expected 3 registered replicas, got %d", len(list))
	}
	if list[0].Role != "leader" || list[0].GRPCAddress != "127.0.0.1:9091" {
		t.Errorf("unexpected leader metadata: %+v", list[0])
	}
}

// Task 5.4: TestCoordinator_QuorumWriteDispatch
func TestCoordinator_QuorumWriteDispatch(t *testing.T) {
	leaderMock := &mockShardService{}
	addrLeader, cleanupLeader := startMockShardGRPC(t, leaderMock)
	defer cleanupLeader()

	followerMock := &mockShardService{}
	addrFollower, cleanupFollower := startMockShardGRPC(t, followerMock)
	defer cleanupFollower()

	coord := NewServer(Config{VirtualNodes: 50, WriteQuorum: 2, ReadQuorum: 2})

	// Create table definition
	tbl := api.TableDefinition{TableName: "users", PartitionKey: "user_id"}
	tblBody, _ := json.Marshal(tbl)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/tables", bytes.NewReader(tblBody))
	rec := httptest.NewRecorder()
	coord.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("failed to create table: %d", rec.Code)
	}

	// Register leader and follower (W=2 satisfied)
	regLeader, _ := json.Marshal(api.ShardRegistration{
		ShardID:     "group-1",
		ReplicaID:   "replica-leader",
		Role:        "leader",
		GRPCAddress: addrLeader,
	})
	req = httptest.NewRequest(http.MethodPost, "/internal/v1/shards", bytes.NewReader(regLeader))
	rec = httptest.NewRecorder()
	coord.ServeHTTP(rec, req)

	regFollower, _ := json.Marshal(api.ShardRegistration{
		ShardID:     "group-1",
		ReplicaID:   "replica-f1",
		Role:        "follower",
		GRPCAddress: addrFollower,
	})
	req = httptest.NewRequest(http.MethodPost, "/internal/v1/shards", bytes.NewReader(regFollower))
	rec = httptest.NewRecorder()
	coord.ServeHTTP(rec, req)

	// 1. Successful Quorum Write dispatched to Leader
	writePayload, _ := json.Marshal(api.Record{
		PartitionKey: "user-123",
		Value:        "Alice",
	})
	req = httptest.NewRequest(http.MethodPost, "/api/v1/tables/users/records", bytes.NewReader(writePayload))
	rec = httptest.NewRecorder()
	coord.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for quorum write, got %d, body: %s", rec.Code, rec.Body.String())
	}
	var resp api.RecordResponse
	_ = json.NewDecoder(rec.Body).Decode(&resp)
	if !resp.Success || resp.Version != 1 {
		t.Errorf("expected success with version 1, got %+v", resp)
	}
	if leaderMock.putCalled != 1 {
		t.Errorf("expected leader to receive 1 PutRecord, got %d", leaderMock.putCalled)
	}
	if followerMock.putCalled != 0 {
		t.Errorf("follower should not receive direct coordinator write dispatch")
	}

	// 2. Error Handling: Leader returns DeadlineExceeded (write quorum timeout)
	leaderMock.putErr = status.Error(codes.DeadlineExceeded, "write quorum timeout: follower ack not received")
	writePayload, _ = json.Marshal(api.Record{
		PartitionKey: "user-456",
		Value:        "Bob",
	})
	req = httptest.NewRequest(http.MethodPost, "/api/v1/tables/users/records", bytes.NewReader(writePayload))
	rec = httptest.NewRecorder()
	coord.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 Service Unavailable on write quorum timeout, got %d", rec.Code)
	}

	// 3. Error Handling: Leader returns Internal Error
	leaderMock.putErr = status.Error(codes.Internal, "disk corruption")
	req = httptest.NewRequest(http.MethodPost, "/api/v1/tables/users/records", bytes.NewReader(writePayload))
	rec = httptest.NewRecorder()
	coord.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 Internal Server Error, got %d", rec.Code)
	}
}

// Task 5.5: TestCoordinator_QuorumReadDispatch_StaleReadElimination
func TestCoordinator_QuorumReadDispatch_StaleReadElimination(t *testing.T) {
	// Setup 3 mock replicas
	s1 := &mockShardService{records: make(map[string]*v1.RecordResponse)} // Leader, holds newest version 10
	addr1, c1 := startMockShardGRPC(t, s1)
	defer c1()

	s2 := &mockShardService{records: make(map[string]*v1.RecordResponse)} // Follower 1, holds stale version 4
	addr2, c2 := startMockShardGRPC(t, s2)
	defer c2()

	s3 := &mockShardService{records: make(map[string]*v1.RecordResponse)} // Follower 2, holds stale version 2
	addr3, c3 := startMockShardGRPC(t, s3)
	defer c3()

	// Seed records: with W=2, at least 2 replicas hold the newest committed version 10
	s1.records["events/alice/"] = &v1.RecordResponse{
		Table:        "events",
		PartitionKey: "alice",
		Value:        []byte(`"latest-v10"`),
		Version:      10,
		Timestamp:    time.Now().UnixNano(),
		IsTombstone:  false,
	}
	s2.records["events/alice/"] = &v1.RecordResponse{
		Table:        "events",
		PartitionKey: "alice",
		Value:        []byte(`"latest-v10"`),
		Version:      10,
		Timestamp:    time.Now().UnixNano(),
		IsTombstone:  false,
	}
	s3.records["events/alice/"] = &v1.RecordResponse{
		Table:        "events",
		PartitionKey: "alice",
		Value:        []byte(`"stale-v2"`),
		Version:      2,
		Timestamp:    time.Now().UnixNano() - 200000,
		IsTombstone:  false,
	}

	coord := NewServer(Config{VirtualNodes: 50, WriteQuorum: 2, ReadQuorum: 2})

	tbl, _ := json.Marshal(api.TableDefinition{TableName: "events", PartitionKey: "user_id"})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/tables", bytes.NewReader(tbl))
	rec := httptest.NewRecorder()
	coord.ServeHTTP(rec, req)

	// Register 3 replicas
	for i, addr := range []string{addr1, addr2, addr3} {
		role := "follower"
		if i == 0 {
			role = "leader"
		}
		b, _ := json.Marshal(api.ShardRegistration{
			ShardID:     "group-1",
			ReplicaID:   fmt.Sprintf("rep-%d", i+1),
			Role:        role,
			GRPCAddress: addr,
		})
		r := httptest.NewRequest(http.MethodPost, "/internal/v1/shards", bytes.NewReader(b))
		coord.ServeHTTP(httptest.NewRecorder(), r)
	}

	// 1. Perform Quorum Read (R=2): Coordinator must resolve highest version 10
	req = httptest.NewRequest(http.MethodGet, "/api/v1/tables/events/records/alice", nil)
	rec = httptest.NewRecorder()
	coord.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d, body: %s", rec.Code, rec.Body.String())
	}
	var retrieved api.Record
	_ = json.NewDecoder(rec.Body).Decode(&retrieved)
	if retrieved.Version != 10 {
		t.Errorf("expected highest version 10 (stale read eliminated), got version %d", retrieved.Version)
	}
	if retrieved.Value != "latest-v10" {
		t.Errorf("expected value 'latest-v10', got %v", retrieved.Value)
	}

	// 2. Tombstone Resolution: If newest version is a tombstone, return 404
	tombstone := &v1.RecordResponse{
		Table:        "events",
		PartitionKey: "alice",
		Version:      15,
		Timestamp:    time.Now().UnixNano(),
		IsTombstone:  true,
	}
	s1.records["events/alice/"] = tombstone
	s2.records["events/alice/"] = tombstone
	req = httptest.NewRequest(http.MethodGet, "/api/v1/tables/events/records/alice", nil)
	rec = httptest.NewRecorder()
	coord.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 Not Found for tombstoned record, got %d", rec.Code)
	}

	// 3. Missing record across all replicas returns 404
	req = httptest.NewRequest(http.MethodGet, "/api/v1/tables/events/records/unknown", nil)
	rec = httptest.NewRecorder()
	coord.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 Not Found for missing record, got %d", rec.Code)
	}
}

// Task 5.6: TestCoordinator_BalancedReadLoadDistribution
func TestCoordinator_BalancedReadLoadDistribution(t *testing.T) {
	s1 := &mockShardService{records: make(map[string]*v1.RecordResponse)}
	addr1, c1 := startMockShardGRPC(t, s1)
	defer c1()

	s2 := &mockShardService{records: make(map[string]*v1.RecordResponse)}
	addr2, c2 := startMockShardGRPC(t, s2)
	defer c2()

	s3 := &mockShardService{records: make(map[string]*v1.RecordResponse)}
	addr3, c3 := startMockShardGRPC(t, s3)
	defer c3()

	recData := &v1.RecordResponse{
		Table:        "cache",
		PartitionKey: "item-1",
		Value:        []byte(`"cached-value"`),
		Version:      1,
	}
	s1.records["cache/item-1/"] = recData
	s2.records["cache/item-1/"] = recData
	s3.records["cache/item-1/"] = recData

	coord := NewServer(Config{VirtualNodes: 50, WriteQuorum: 2, ReadQuorum: 2})

	tbl, _ := json.Marshal(api.TableDefinition{TableName: "cache", PartitionKey: "key"})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/tables", bytes.NewReader(tbl))
	coord.ServeHTTP(httptest.NewRecorder(), req)

	addrs := []string{addr1, addr2, addr3}
	for i, addr := range addrs {
		role := "follower"
		if i == 0 {
			role = "leader"
		}
		b, _ := json.Marshal(api.ShardRegistration{
			ShardID:     "cache-group",
			ReplicaID:   fmt.Sprintf("node-%d", i+1),
			Role:        role,
			GRPCAddress: addr,
		})
		r := httptest.NewRequest(http.MethodPost, "/internal/v1/shards", bytes.NewReader(b))
		coord.ServeHTTP(httptest.NewRecorder(), r)
	}

	// Issue 30 reads with R=2: 60 total RPC reads distributed across 3 nodes
	numReads := 30
	for i := 0; i < numReads; i++ {
		r := httptest.NewRequest(http.MethodGet, "/api/v1/tables/cache/records/item-1", nil)
		rec := httptest.NewRecorder()
		coord.ServeHTTP(rec, r)
		if rec.Code != http.StatusOK {
			t.Fatalf("read #%d failed with code %d", i, rec.Code)
		}
	}

	totalRPCs := s1.getCalled + s2.getCalled + s3.getCalled
	expectedTotal := numReads * 2
	if totalRPCs != expectedTotal {
		t.Fatalf("expected %d total RPC calls, got %d (s1=%d, s2=%d, s3=%d)", expectedTotal, totalRPCs, s1.getCalled, s2.getCalled, s3.getCalled)
	}

	// Check balanced load distribution: with round-robin rotation, each should receive exactly 20 calls
	t.Logf("Read distribution: s1=%d, s2=%d, s3=%d", s1.getCalled, s2.getCalled, s3.getCalled)
	if s1.getCalled != 20 || s2.getCalled != 20 || s3.getCalled != 20 {
		t.Errorf("read traffic was not evenly balanced: s1=%d, s2=%d, s3=%d", s1.getCalled, s2.getCalled, s3.getCalled)
	}
}

// Task 5.7: TestCoordinator_QuorumAvailabilityGuards
func TestCoordinator_QuorumAvailabilityGuards(t *testing.T) {
	coord := NewServer(Config{VirtualNodes: 50, WriteQuorum: 2, ReadQuorum: 2})

	tbl, _ := json.Marshal(api.TableDefinition{TableName: "orders", PartitionKey: "order_id"})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/tables", bytes.NewReader(tbl))
	coord.ServeHTTP(httptest.NewRecorder(), req)

	// 1. Empty Ring Guard: returns 503
	writePayload, _ := json.Marshal(api.Record{PartitionKey: "ord-1", Value: "100"})
	req = httptest.NewRequest(http.MethodPost, "/api/v1/tables/orders/records", bytes.NewReader(writePayload))
	rec := httptest.NewRecorder()
	coord.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503 for empty ring write, got %d", rec.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/api/v1/tables/orders/records/ord-1", nil)
	rec = httptest.NewRecorder()
	coord.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503 for empty ring read, got %d", rec.Code)
	}

	// 2. Active Replicas < W Guard: Register only 1 replica (W=2 required)
	mockSvc := &mockShardService{}
	addr1, c1 := startMockShardGRPC(t, mockSvc)
	defer c1()

	b, _ := json.Marshal(api.ShardRegistration{
		ShardID:     "orders-group",
		ReplicaID:   "rep-1",
		Role:        "leader",
		GRPCAddress: addr1,
	})
	r := httptest.NewRequest(http.MethodPost, "/internal/v1/shards", bytes.NewReader(b))
	coord.ServeHTTP(httptest.NewRecorder(), r)

	// Write must fail with 503 (1 replica < W=2)
	req = httptest.NewRequest(http.MethodPost, "/api/v1/tables/orders/records", bytes.NewReader(writePayload))
	rec = httptest.NewRecorder()
	coord.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503 when active replicas < W, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "write quorum unavailable") {
		t.Errorf("expected body to mention write quorum unavailable, got: %s", rec.Body.String())
	}

	// Read must fail with 503 (1 replica < R=2)
	req = httptest.NewRequest(http.MethodGet, "/api/v1/tables/orders/records/ord-1", nil)
	rec = httptest.NewRecorder()
	coord.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503 when active replicas < R, got %d", rec.Code)
	}

	// 3. Missing Leader Guard: Register 2 followers but no leader
	coordNoLeader := NewServer(Config{VirtualNodes: 50, WriteQuorum: 2, ReadQuorum: 2})
	tblReq := httptest.NewRequest(http.MethodPost, "/api/v1/tables", bytes.NewReader(tbl))
	coordNoLeader.ServeHTTP(httptest.NewRecorder(), tblReq)

	b1, _ := json.Marshal(api.ShardRegistration{
		ShardID:     "orders-group",
		ReplicaID:   "follower-1",
		Role:        "follower",
		GRPCAddress: addr1,
	})
	b2, _ := json.Marshal(api.ShardRegistration{
		ShardID:     "orders-group",
		ReplicaID:   "follower-2",
		Role:        "follower",
		GRPCAddress: addr1,
	})
	coordNoLeader.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/internal/v1/shards", bytes.NewReader(b1)))
	coordNoLeader.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/internal/v1/shards", bytes.NewReader(b2)))

	req = httptest.NewRequest(http.MethodPost, "/api/v1/tables/orders/records", bytes.NewReader(writePayload))
	rec = httptest.NewRecorder()
	coordNoLeader.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503 when group lacks a leader, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "shard leader not found") {
		t.Errorf("expected error message to mention shard leader not found, got %s", rec.Body.String())
	}
}

func TestCoordinatorTableRegistry(t *testing.T) {
	coord := NewServer(Config{ListenAddr: ":8080", VirtualNodes: 50})

	tbl := api.TableDefinition{
		TableName:    "products",
		PartitionKey: "category",
		SortKey:      "product_id",
	}
	body, _ := json.Marshal(tbl)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/tables", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	coord.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201 Created, got %d", rec.Code)
	}

	// Duplicate creation returns 409 Conflict
	req = httptest.NewRequest(http.MethodPost, "/api/v1/tables", bytes.NewReader(body))
	rec = httptest.NewRecorder()
	coord.ServeHTTP(rec, req)
	if rec.Code != http.StatusConflict {
		t.Fatalf("expected 409 Conflict for duplicate table, got %d", rec.Code)
	}

	// Get table metadata
	req = httptest.NewRequest(http.MethodGet, "/api/v1/tables/products", nil)
	rec = httptest.NewRecorder()
	coord.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	// Get missing table
	req = httptest.NewRequest(http.MethodGet, "/api/v1/tables/non_existent", nil)
	rec = httptest.NewRecorder()
	coord.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}

func TestEndToEndMultiShardRouting(t *testing.T) {
	// 1. Create two real shard test servers with gRPC enabled
	shard1 := shard.NewServer(shard.Config{
		ShardID:        "shard-1",
		ReplicaID:      "shard-1-r1",
		Role:           "leader",
		ListenAddr:     ":0",
		GRPCListenAddr: "127.0.0.1:0",
		VirtualNodes:   50,
	})
	if err := shard1.StartGRPC(); err != nil {
		t.Fatalf("failed to start shard1 gRPC: %v", err)
	}
	defer shard1.Stop(context.Background())
	ts1 := httptest.NewServer(shard1)
	defer ts1.Close()
	addr1 := strings.TrimPrefix(ts1.URL, "http://")
	grpcAddr1 := shard1.GRPCAddr()

	shard2 := shard.NewServer(shard.Config{
		ShardID:        "shard-2",
		ReplicaID:      "shard-2-r1",
		Role:           "leader",
		ListenAddr:     ":0",
		GRPCListenAddr: "127.0.0.1:0",
		VirtualNodes:   50,
	})
	if err := shard2.StartGRPC(); err != nil {
		t.Fatalf("failed to start shard2 gRPC: %v", err)
	}
	defer shard2.Stop(context.Background())
	ts2 := httptest.NewServer(shard2)
	defer ts2.Close()
	addr2 := strings.TrimPrefix(ts2.URL, "http://")
	grpcAddr2 := shard2.GRPCAddr()

	// 2. Initialize Coordinator (W=1, R=1 for single-node-per-shard routing test)
	coord := NewServer(Config{ListenAddr: ":8080", VirtualNodes: 50, WriteQuorum: 1, ReadQuorum: 1})

	// Test 503 before any shards registered
	tbl := api.TableDefinition{TableName: "metrics", PartitionKey: "host", SortKey: "ts"}
	body, _ := json.Marshal(tbl)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/tables", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	coord.ServeHTTP(rec, req)

	putReq := api.Record{PartitionKey: "server-a", SortKey: "100", Value: "cpu=15%"}
	body, _ = json.Marshal(putReq)
	req = httptest.NewRequest(http.MethodPost, "/api/v1/tables/metrics/records", bytes.NewReader(body))
	rec = httptest.NewRecorder()
	coord.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 Service Unavailable when no shards registered, got %d", rec.Code)
	}

	// 3. Register both shards with coordinator
	reg1, _ := json.Marshal(api.ShardRegistration{
		ShardID:      "shard-1",
		ReplicaID:    "shard-1-r1",
		Role:         "leader",
		GRPCAddress:  grpcAddr1,
		HTTPAddress:  addr1,
		Address:      addr1,
		VirtualNodes: 50,
	})
	req = httptest.NewRequest(http.MethodPost, "/internal/v1/shards", bytes.NewReader(reg1))
	rec = httptest.NewRecorder()
	coord.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("failed registering shard 1")
	}

	reg2, _ := json.Marshal(api.ShardRegistration{
		ShardID:      "shard-2",
		ReplicaID:    "shard-2-r1",
		Role:         "leader",
		GRPCAddress:  grpcAddr2,
		HTTPAddress:  addr2,
		Address:      addr2,
		VirtualNodes: 50,
	})
	req = httptest.NewRequest(http.MethodPost, "/internal/v1/shards", bytes.NewReader(reg2))
	rec = httptest.NewRecorder()
	coord.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("failed registering shard 2")
	}

	// 4. Create Table on coordinator (broadcasts to shards)
	eventTbl := api.TableDefinition{TableName: "events", PartitionKey: "user_id", SortKey: "seq"}
	body, _ = json.Marshal(eventTbl)
	req = httptest.NewRequest(http.MethodPost, "/api/v1/tables", bytes.NewReader(body))
	rec = httptest.NewRecorder()
	coord.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("failed creating table events on coordinator: %d", rec.Code)
	}

	// 5. Insert records across multiple keys
	keys := []string{"alice", "bob", "carol", "dave", "eve", "frank", "grace", "heidi"}
	for i, k := range keys {
		r := api.Record{
			PartitionKey: k,
			SortKey:      fmt.Sprintf("%03d", i),
			Value:        map[string]interface{}{"msg": "hello " + k},
		}
		b, _ := json.Marshal(r)
		req = httptest.NewRequest(http.MethodPost, "/api/v1/tables/events/records", bytes.NewReader(b))
		rec = httptest.NewRecorder()
		coord.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("put record for %s failed: %d, body: %s", k, rec.Code, rec.Body.String())
		}
	}

	// Verify both shards received at least some records
	t1Count := shard1.Engine().TotalRecords()
	t2Count := shard2.Engine().TotalRecords()
	t.Logf("Shard 1 records: %d, Shard 2 records: %d (total: %d)", t1Count, t2Count, t1Count+t2Count)
	if t1Count+t2Count != len(keys) {
		t.Fatalf("expected all %d records stored across shards, got %d", len(keys), t1Count+t2Count)
	}

	// 6. Test READ through coordinator
	req = httptest.NewRequest(http.MethodGet, "/api/v1/tables/events/records/alice/000", nil)
	rec = httptest.NewRecorder()
	coord.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("read failed: %d", rec.Code)
	}
	var retrieved api.Record
	_ = json.NewDecoder(rec.Body).Decode(&retrieved)
	if retrieved.PartitionKey != "alice" || retrieved.SortKey != "000" {
		t.Fatalf("unexpected record: %+v", retrieved)
	}

	// 7. Test EXISTS (HEAD) through coordinator
	req = httptest.NewRequest(http.MethodHead, "/api/v1/tables/events/records/alice/000", nil)
	rec = httptest.NewRecorder()
	coord.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("exists check failed: %d", rec.Code)
	}

	// 8. Test Range Query through coordinator
	// Insert more records for alice
	for s := 1; s <= 5; s++ {
		r := api.Record{
			PartitionKey: "alice",
			SortKey:      fmt.Sprintf("%03d", s),
			Value:        fmt.Sprintf("event-%d", s),
		}
		b, _ := json.Marshal(r)
		req = httptest.NewRequest(http.MethodPost, "/api/v1/tables/events/records", bytes.NewReader(b))
		rec = httptest.NewRecorder()
		coord.ServeHTTP(rec, req)
	}

	req = httptest.NewRequest(http.MethodGet, "/api/v1/tables/events/query?partitionKey=alice&sortKeyFrom=001&sortKeyTo=004", nil)
	rec = httptest.NewRecorder()
	coord.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("range query failed: %d", rec.Code)
	}
	var rangeList []api.Record
	_ = json.NewDecoder(rec.Body).Decode(&rangeList)
	if len(rangeList) < 4 {
		t.Fatalf("expected at least 4 records in range query, got %d", len(rangeList))
	}

	// 9. Test DELETE through coordinator
	req = httptest.NewRequest(http.MethodDelete, "/api/v1/tables/events/records/alice/000", nil)
	rec = httptest.NewRecorder()
	coord.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("delete failed: %d", rec.Code)
	}

	// Verify 404 on deleted record
	req = httptest.NewRequest(http.MethodGet, "/api/v1/tables/events/records/alice/000", nil)
	rec = httptest.NewRecorder()
	coord.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 after delete, got %d", rec.Code)
	}
}

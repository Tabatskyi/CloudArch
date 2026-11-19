package coordinator

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	v1 "github.com/vasenka88/cloudarch/pkg/proto/v1"
)

func TestGRPCPool_ConnectionReuse(t *testing.T) {
	lis := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	v1.RegisterShardServiceServer(server, &mockShardService{})
	go func() { _ = server.Serve(lis) }()
	defer func() {
		server.Stop()
		_ = lis.Close()
	}()

	pool := NewGRPCPool()
	pool.SetDialer(func(context.Context, string) (net.Conn, error) {
		return lis.Dial()
	})
	defer pool.Close()

	if pool.ConnectionCount() != 0 {
		t.Fatalf("expected 0 initial connections, got %d", pool.ConnectionCount())
	}

	// First get
	conn1, err := pool.GetClientConn("target-1")
	if err != nil {
		t.Fatalf("failed to get conn1: %v", err)
	}
	if pool.ConnectionCount() != 1 {
		t.Fatalf("expected 1 connection after first dial, got %d", pool.ConnectionCount())
	}

	// Second get with same target address must reuse conn1
	conn2, err := pool.GetClientConn("target-1")
	if err != nil {
		t.Fatalf("failed to get conn2: %v", err)
	}
	if conn1 != conn2 {
		t.Error("expected conn1 and conn2 to be identical reused connection")
	}
	if pool.ConnectionCount() != 1 {
		t.Fatalf("expected connection count to remain 1, got %d", pool.ConnectionCount())
	}

	// GetShardClient should also reuse the same connection
	client, err := pool.GetShardClient("target-1")
	if err != nil || client == nil {
		t.Fatalf("failed to get shard client: %v", err)
	}
	if pool.ConnectionCount() != 1 {
		t.Fatalf("expected connection count to remain 1, got %d", pool.ConnectionCount())
	}

	// Different address creates new connection
	_, err = pool.GetClientConn("target-2")
	if err != nil {
		t.Fatalf("failed to get conn for target-2: %v", err)
	}
	if pool.ConnectionCount() != 2 {
		t.Fatalf("expected 2 connections, got %d", pool.ConnectionCount())
	}
}

type mockShardService struct {
	v1.UnimplementedShardServiceServer
	mu           sync.Mutex
	records      map[string]*v1.RecordResponse
	getCalled    int
	putCalled    int
	deleteCalled int
	queryCalled  int
	putErr       error
	getErr       error
	deleteErr    error
}

func (m *mockShardService) PutRecord(ctx context.Context, req *v1.PutRecordRequest) (*v1.RecordResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.putCalled++
	if m.putErr != nil {
		return nil, m.putErr
	}
	if m.records == nil {
		m.records = make(map[string]*v1.RecordResponse)
	}
	key := fmt.Sprintf("%s/%s/%s", req.Table, req.PartitionKey, req.SortKey)
	res := &v1.RecordResponse{
		Table:        req.Table,
		PartitionKey: req.PartitionKey,
		SortKey:      req.SortKey,
		Value:        req.Value,
		Version:      uint64(m.putCalled),
		Timestamp:    time.Now().UnixNano(),
		IsTombstone:  false,
	}
	m.records[key] = res
	return res, nil
}

func (m *mockShardService) GetRecord(ctx context.Context, req *v1.GetRecordRequest) (*v1.RecordResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.getCalled++
	if m.getErr != nil {
		return nil, m.getErr
	}
	key := fmt.Sprintf("%s/%s/%s", req.Table, req.PartitionKey, req.SortKey)
	res, ok := m.records[key]
	if !ok {
		return nil, status.Error(codes.NotFound, "record not found")
	}
	return res, nil
}

func (m *mockShardService) DeleteRecord(ctx context.Context, req *v1.DeleteRecordRequest) (*v1.DeleteRecordResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deleteCalled++
	if m.deleteErr != nil {
		return nil, m.deleteErr
	}
	key := fmt.Sprintf("%s/%s/%s", req.Table, req.PartitionKey, req.SortKey)
	delete(m.records, key)
	return &v1.DeleteRecordResponse{Success: true}, nil
}

func (m *mockShardService) QueryRecords(ctx context.Context, req *v1.QueryRecordsRequest) (*v1.QueryRecordsResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.queryCalled++
	var recs []*v1.RecordResponse
	for _, r := range m.records {
		if r.Table == req.Table && r.PartitionKey == req.PartitionKey {
			recs = append(recs, r)
		}
	}
	return &v1.QueryRecordsResponse{Records: recs}, nil
}

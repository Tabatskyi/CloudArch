package shard

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/vasenka88/cloudarch/pkg/api"
	v1 "github.com/vasenka88/cloudarch/pkg/proto/v1"
	"github.com/vasenka88/cloudarch/pkg/storage"
)

func setupTestGRPCServer(t *testing.T, replicator WriteReplicator) (v1.ShardServiceClient, *storage.Engine, func()) {
	lis := bufconn.Listen(1024 * 1024)
	s := grpc.NewServer()
	engine := storage.NewEngine("")

	// Create test table
	_ = engine.CreateTable(api.TableDefinition{
		TableName:    "users",
		PartitionKey: "id",
		SortKey:      "attr",
	})

	grpcServer := NewShardGRPCServer(engine, replicator)
	v1.RegisterShardServiceServer(s, grpcServer)

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
		t.Fatalf("failed to dial bufnet: %v", err)
	}

	client := v1.NewShardServiceClient(conn)
	cleanup := func() {
		_ = conn.Close()
		s.Stop()
		_ = lis.Close()
	}

	return client, engine, cleanup
}

func TestShardGRPCServer_CRUD(t *testing.T) {
	client, _, cleanup := setupTestGRPCServer(t, nil)
	defer cleanup()

	ctx := context.Background()

	// 1. PutRecord
	putResp, err := client.PutRecord(ctx, &v1.PutRecordRequest{
		Table:        "users",
		PartitionKey: "u1",
		SortKey:      "name",
		Value:        []byte("Alice"),
	})
	if err != nil {
		t.Fatalf("PutRecord failed: %v", err)
	}
	if putResp.Version != 1 {
		t.Errorf("expected version 1, got %d", putResp.Version)
	}

	// 2. GetRecord
	getResp, err := client.GetRecord(ctx, &v1.GetRecordRequest{
		Table:        "users",
		PartitionKey: "u1",
		SortKey:      "name",
	})
	if err != nil {
		t.Fatalf("GetRecord failed: %v", err)
	}
	if string(getResp.Value) != "Alice" {
		t.Errorf("expected Alice, got %s", string(getResp.Value))
	}
	if getResp.Version != 1 {
		t.Errorf("expected version 1, got %d", getResp.Version)
	}

	// 3. DeleteRecord
	delResp, err := client.DeleteRecord(ctx, &v1.DeleteRecordRequest{
		Table:        "users",
		PartitionKey: "u1",
		SortKey:      "name",
	})
	if err != nil {
		t.Fatalf("DeleteRecord failed: %v", err)
	}
	if delResp.Version != 2 {
		t.Errorf("expected tombstone version 2, got %d", delResp.Version)
	}

	// 4. GetRecord should return tombstone record with is_tombstone=true
	getTombstone, err := client.GetRecord(ctx, &v1.GetRecordRequest{
		Table:        "users",
		PartitionKey: "u1",
		SortKey:      "name",
	})
	if err != nil {
		t.Fatalf("GetRecord on tombstone failed: %v", err)
	}
	if !getTombstone.IsTombstone {
		t.Error("expected IsTombstone=true")
	}
	if getTombstone.Version != 2 {
		t.Errorf("expected version 2 on tombstone, got %d", getTombstone.Version)
	}
}

func TestShardGRPCServer_QueryRecords(t *testing.T) {
	client, _, cleanup := setupTestGRPCServer(t, nil)
	defer cleanup()

	ctx := context.Background()

	// Insert several records
	items := []struct {
		sk  string
		val string
	}{
		{"order#1", "data1"},
		{"order#2", "data2"},
		{"profile", "data3"},
	}

	for _, it := range items {
		_, err := client.PutRecord(ctx, &v1.PutRecordRequest{
			Table:        "users",
			PartitionKey: "u1",
			SortKey:      it.sk,
			Value:        []byte(it.val),
		})
		if err != nil {
			t.Fatalf("PutRecord %s failed: %v", it.sk, err)
		}
	}

	// Query with prefix "order#"
	queryResp, err := client.QueryRecords(ctx, &v1.QueryRecordsRequest{
		Table:        "users",
		PartitionKey: "u1",
		Prefix:       "order#",
		Limit:        10,
	})
	if err != nil {
		t.Fatalf("QueryRecords failed: %v", err)
	}

	if len(queryResp.Records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(queryResp.Records))
	}
	for _, r := range queryResp.Records {
		if !matchesPrefix(r.SortKey, "order#") {
			t.Errorf("unexpected sort key: %s", r.SortKey)
		}
	}
}

func TestShardGRPCServer_NotFound(t *testing.T) {
	client, _, cleanup := setupTestGRPCServer(t, nil)
	defer cleanup()

	ctx := context.Background()

	_, err := client.GetRecord(ctx, &v1.GetRecordRequest{
		Table:        "users",
		PartitionKey: "non-existent",
		SortKey:      "none",
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.NotFound {
		t.Fatalf("expected NotFound code, got %v", st.Code())
	}
}

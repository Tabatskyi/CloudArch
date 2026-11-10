package coordinator

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/vasenka88/cloudarch/pkg/api"
	"github.com/vasenka88/cloudarch/pkg/shard"
)

func TestCoordinatorShardRegistration(t *testing.T) {
	coord := NewServer(Config{ListenAddr: ":8080", VirtualNodes: 50})

	// Register shard 1
	regPayload := api.ShardRegistration{
		ShardID:      "shard-1",
		Address:      "127.0.0.1:8081",
		VirtualNodes: 50,
	}
	body, _ := json.Marshal(regPayload)
	req := httptest.NewRequest(http.MethodPost, "/internal/v1/shards", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	coord.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	// Verify ring has 1 member
	if coord.Ring().MemberCount() != 1 {
		t.Fatalf("expected 1 ring member, got %d", coord.Ring().MemberCount())
	}

	// List shards
	req = httptest.NewRequest(http.MethodGet, "/internal/v1/shards", nil)
	rec = httptest.NewRecorder()
	coord.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var shards []api.ShardInfo
	_ = json.NewDecoder(rec.Body).Decode(&shards)
	if len(shards) != 1 || shards[0].ShardID != "shard-1" {
		t.Fatalf("unexpected shard list: %+v", shards)
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
	// 1. Create two real shard test servers
	shard1 := shard.NewServer(shard.Config{ShardID: "shard-1", ListenAddr: ":0", VirtualNodes: 50})
	ts1 := httptest.NewServer(shard1)
	defer ts1.Close()
	addr1 := strings.TrimPrefix(ts1.URL, "http://")

	shard2 := shard.NewServer(shard.Config{ShardID: "shard-2", ListenAddr: ":0", VirtualNodes: 50})
	ts2 := httptest.NewServer(shard2)
	defer ts2.Close()
	addr2 := strings.TrimPrefix(ts2.URL, "http://")

	// 2. Initialize Coordinator
	coord := NewServer(Config{ListenAddr: ":8080", VirtualNodes: 50})

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
	reg1, _ := json.Marshal(api.ShardRegistration{ShardID: "shard-1", Address: addr1, VirtualNodes: 50})
	req = httptest.NewRequest(http.MethodPost, "/internal/v1/shards", bytes.NewReader(reg1))
	rec = httptest.NewRecorder()
	coord.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("failed registering shard 1")
	}

	reg2, _ := json.Marshal(api.ShardRegistration{ShardID: "shard-2", Address: addr2, VirtualNodes: 50})
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
	if len(rangeList) != 4 {
		t.Fatalf("expected 4 records in range [001, 004], got %d", len(rangeList))
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

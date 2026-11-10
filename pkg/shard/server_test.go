package shard

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/vasenka88/cloudarch/pkg/api"
)

func setupTestServer() *Server {
	cfg := Config{
		ShardID:      "test-shard-1",
		ListenAddr:   ":8081",
		VirtualNodes: 50,
	}
	return NewServer(cfg)
}

func TestShardHealthz(t *testing.T) {
	s := setupTestServer()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()

	s.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var status api.HealthStatus
	if err := json.NewDecoder(rec.Body).Decode(&status); err != nil {
		t.Fatalf("failed to decode health response: %v", err)
	}
	if status.NodeID != "test-shard-1" || status.Status != "ok" {
		t.Fatalf("unexpected health payload: %+v", status)
	}
}

func TestShardTableAndRecordCRUD(t *testing.T) {
	s := setupTestServer()

	// 1. Create Table
	tblPayload := api.TableDefinition{
		TableName:    "orders",
		PartitionKey: "customer_id",
		SortKey:      "order_id",
	}
	body, _ := json.Marshal(tblPayload)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/tables", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create table failed: code %d, body %s", rec.Code, rec.Body.String())
	}

	// 2. Put Record
	recPayload := api.Record{
		PartitionKey: "cust_100",
		SortKey:      "ord_001",
		Value:        map[string]interface{}{"amount": 49.99},
	}
	body, _ = json.Marshal(recPayload)
	req = httptest.NewRequest(http.MethodPost, "/api/v1/tables/orders/records", bytes.NewReader(body))
	rec = httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("put record failed: code %d, body %s", rec.Code, rec.Body.String())
	}

	// 3. Exists Check (HEAD)
	req = httptest.NewRequest(http.MethodHead, "/api/v1/tables/orders/records/cust_100?sortKey=ord_001", nil)
	rec = httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected HEAD 200 OK, got %d", rec.Code)
	}

	// 4. Read Record (GET)
	req = httptest.NewRequest(http.MethodGet, "/api/v1/tables/orders/records/cust_100/ord_001", nil)
	rec = httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("get record failed: code %d", rec.Code)
	}
	var stored api.Record
	_ = json.NewDecoder(rec.Body).Decode(&stored)
	if stored.PartitionKey != "cust_100" || stored.SortKey != "ord_001" {
		t.Fatalf("unexpected retrieved record: %+v", stored)
	}

	// 5. Query Range
	// Insert second order
	recPayload2 := api.Record{
		PartitionKey: "cust_100",
		SortKey:      "ord_002",
		Value:        map[string]interface{}{"amount": 99.00},
	}
	body, _ = json.Marshal(recPayload2)
	req = httptest.NewRequest(http.MethodPost, "/api/v1/tables/orders/records", bytes.NewReader(body))
	rec = httptest.NewRecorder()
	s.ServeHTTP(rec, req)

	req = httptest.NewRequest(http.MethodGet, "/api/v1/tables/orders/query?partitionKey=cust_100&sortKeyFrom=ord_001&sortKeyTo=ord_002", nil)
	rec = httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("query failed: code %d", rec.Code)
	}
	var queryResults []api.Record
	_ = json.NewDecoder(rec.Body).Decode(&queryResults)
	if len(queryResults) != 2 {
		t.Fatalf("expected 2 records in query, got %d", len(queryResults))
	}

	// 6. Delete Record
	req = httptest.NewRequest(http.MethodDelete, "/api/v1/tables/orders/records/cust_100/ord_001", nil)
	rec = httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("delete failed: code %d", rec.Code)
	}

	// 7. Verify Gone
	req = httptest.NewRequest(http.MethodHead, "/api/v1/tables/orders/records/cust_100/ord_001", nil)
	rec = httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 after delete, got %d", rec.Code)
	}
}

func TestShardRegistrationMock(t *testing.T) {
	// Mock coordinator
	coordMux := http.NewServeMux()
	var registered bool
	coordMux.HandleFunc("/internal/v1/shards", func(w http.ResponseWriter, r *http.Request) {
		registered = true
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(api.SuccessResponse{Success: true})
	})
	mockCoord := httptest.NewServer(coordMux)
	defer mockCoord.Close()

	cfg := Config{
		ShardID:        "shard-test-reg",
		ListenAddr:     ":8081",
		AdvertiseAddr:  "127.0.0.1:8081",
		CoordinatorURL: mockCoord.URL,
		VirtualNodes:   100,
	}
	server := NewServer(cfg)

	err := server.RegisterWithCoordinator(context.Background())
	if err != nil {
		t.Fatalf("failed to register with mock coordinator: %v", err)
	}
	if !registered {
		t.Fatalf("expected coordinator to receive registration")
	}
}

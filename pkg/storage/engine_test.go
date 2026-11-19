package storage

import (
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/vasenka88/cloudarch/pkg/api"
)

func TestTableLifecycle(t *testing.T) {
	eng := NewEngine("")

	def := api.TableDefinition{
		TableName:    "users",
		PartitionKey: "user_id",
	}

	err := eng.CreateTable(def)
	if err != nil {
		t.Fatalf("unexpected error creating table: %v", err)
	}

	// Duplicate creation
	err = eng.CreateTable(def)
	if err != ErrTableAlreadyExists {
		t.Fatalf("expected ErrTableAlreadyExists, got %v", err)
	}

	// Get table
	got, err := eng.GetTable("users")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.TableName != "users" || got.PartitionKey != "user_id" {
		t.Fatalf("unexpected table def: %+v", got)
	}

	// List tables
	tables := eng.ListTables()
	if len(tables) != 1 {
		t.Fatalf("expected 1 table, got %d", len(tables))
	}
}

func TestRecordCRUD(t *testing.T) {
	eng := NewEngine("")
	_ = eng.CreateTable(api.TableDefinition{
		TableName:    "users",
		PartitionKey: "user_id",
	})

	rec := api.Record{
		PartitionKey: "u123",
		Value:        map[string]interface{}{"name": "Alice", "role": "admin"},
	}

	// Put
	err := eng.PutRecord("users", rec)
	if err != nil {
		t.Fatalf("unexpected put error: %v", err)
	}

	// Exists
	exists, err := eng.ExistsRecord("users", "u123", "")
	if err != nil || !exists {
		t.Fatalf("expected record to exist, exists=%v, err=%v", exists, err)
	}

	// Get
	retrieved, err := eng.GetRecord("users", "u123", "")
	if err != nil {
		t.Fatalf("unexpected get error: %v", err)
	}
	if retrieved.PartitionKey != "u123" {
		t.Fatalf("expected u123, got %s", retrieved.PartitionKey)
	}

	// Delete
	err = eng.DeleteRecord("users", "u123", "")
	if err != nil {
		t.Fatalf("unexpected delete error: %v", err)
	}

	// Exists after delete
	exists, err = eng.ExistsRecord("users", "u123", "")
	if err != nil || exists {
		t.Fatalf("expected record to not exist after delete, exists=%v", exists)
	}
}

func TestCompoundPrimaryKeyAndRangeQuery(t *testing.T) {
	eng := NewEngine("")
	_ = eng.CreateTable(api.TableDefinition{
		TableName:    "sensor_events",
		PartitionKey: "device_id",
		SortKey:      "timestamp",
	})

	// Missing sort key on table requiring sort key
	err := eng.PutRecord("sensor_events", api.Record{
		PartitionKey: "dev_1",
		Value:        "temp=21.5",
	})
	if err != ErrMissingSortKey {
		t.Fatalf("expected ErrMissingSortKey, got %v", err)
	}

	// Insert sorted events
	timestamps := []string{"1700000000", "1700001000", "1700002000", "1700003000", "1700004000"}
	for _, ts := range timestamps {
		_ = eng.PutRecord("sensor_events", api.Record{
			PartitionKey: "dev_1",
			SortKey:      ts,
			Value:        fmt.Sprintf("reading_at_%s", ts),
		})
	}

	// Point lookup
	rec, err := eng.GetRecord("sensor_events", "dev_1", "1700002000")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rec.SortKey != "1700002000" {
		t.Fatalf("expected sortKey 1700002000, got %s", rec.SortKey)
	}

	// Range query
	rangeResults, err := eng.QueryPartition("sensor_events", "dev_1", "1700001000", "1700003000", 10)
	if err != nil {
		t.Fatalf("unexpected range query error: %v", err)
	}
	if len(rangeResults) != 3 {
		t.Fatalf("expected 3 items in range [1700001000, 1700003000], got %d", len(rangeResults))
	}
	if rangeResults[0].SortKey != "1700001000" || rangeResults[2].SortKey != "1700003000" {
		t.Fatalf("unexpected range slice order: %+v", rangeResults)
	}
}

func TestConcurrentReadsAndWrites(t *testing.T) {
	eng := NewEngine("")
	_ = eng.CreateTable(api.TableDefinition{
		TableName:    "concurrent_tbl",
		PartitionKey: "id",
		SortKey:      "seq",
	})

	var wg sync.WaitGroup
	workers := 50
	opsPerWorker := 100

	// Concurrent writes
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < opsPerWorker; i++ {
				pk := fmt.Sprintf("partition_%d", workerID%5)
				sk := fmt.Sprintf("%05d", i)
				_ = eng.PutRecord("concurrent_tbl", api.Record{
					PartitionKey: pk,
					SortKey:      sk,
					Value:        fmt.Sprintf("w=%d,i=%d", workerID, i),
				})
			}
		}(w)
	}
	wg.Wait()

	// Concurrent reads
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			pk := fmt.Sprintf("partition_%d", workerID%5)
			_, _ = eng.QueryPartition("concurrent_tbl", pk, "00010", "00050", 100)
		}(w)
	}
	wg.Wait()
}

func TestSnapshotAndRestore(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "kv_storage_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	eng := NewEngine(tmpDir)
	_ = eng.CreateTable(api.TableDefinition{
		TableName:    "persisted_tbl",
		PartitionKey: "pk",
	})
	_ = eng.PutRecord("persisted_tbl", api.Record{
		PartitionKey: "p1",
		Value:        "persisted_data",
	})

	err = eng.Snapshot("test_snapshot.json")
	if err != nil {
		t.Fatalf("failed to snapshot: %v", err)
	}

	// Restore into fresh engine
	eng2 := NewEngine(tmpDir)
	err = eng2.Restore("test_snapshot.json")
	if err != nil {
		t.Fatalf("failed to restore: %v", err)
	}

	rec, err := eng2.GetRecord("persisted_tbl", "p1", "")
	if err != nil || rec.Value != "persisted_data" {
		t.Fatalf("failed to read restored record: %+v, err: %v", rec, err)
	}
}

func TestRecordVersioningAndTombstones(t *testing.T) {
	eng := NewEngine("")
	_ = eng.CreateTable(api.TableDefinition{
		TableName:    "kv",
		PartitionKey: "pk",
		SortKey:      "sk",
	})

	// 1. Initial Put
	err := eng.PutRecord("kv", api.Record{
		PartitionKey: "user#1",
		SortKey:      "meta",
		Value:        "first_value",
	})
	if err != nil {
		t.Fatalf("first put failed: %v", err)
	}

	rec, err := eng.GetRecord("kv", "user#1", "meta")
	if err != nil {
		t.Fatalf("get record failed: %v", err)
	}
	if rec.Version != 1 {
		t.Errorf("expected version 1, got %d", rec.Version)
	}
	if rec.Timestamp == 0 {
		t.Error("expected non-zero timestamp")
	}
	if rec.IsTombstone {
		t.Error("expected IsTombstone to be false")
	}

	// 2. Second Put (auto-increment version)
	err = eng.PutRecord("kv", api.Record{
		PartitionKey: "user#1",
		SortKey:      "meta",
		Value:        "second_value",
	})
	if err != nil {
		t.Fatalf("second put failed: %v", err)
	}

	rec2, err := eng.GetRecord("kv", "user#1", "meta")
	if err != nil {
		t.Fatalf("get record 2 failed: %v", err)
	}
	if rec2.Version != 2 {
		t.Errorf("expected version 2, got %d", rec2.Version)
	}

	// 3. Delete marks tombstone with version 3
	err = eng.DeleteRecord("kv", "user#1", "meta")
	if err != nil {
		t.Fatalf("delete record failed: %v", err)
	}

	// GetRecord should return ErrRecordNotFound
	_, err = eng.GetRecord("kv", "user#1", "meta")
	if err != ErrRecordNotFound {
		t.Fatalf("expected ErrRecordNotFound, got %v", err)
	}

	// ExistsRecord should return false
	exists, err := eng.ExistsRecord("kv", "user#1", "meta")
	if err != nil || exists {
		t.Fatalf("expected exists=false, got %v, err=%v", exists, err)
	}

	// GetRawRecord should return the tombstone with version 3
	raw, err := eng.GetRawRecord("kv", "user#1", "meta")
	if err != nil {
		t.Fatalf("get raw record failed: %v", err)
	}
	if !raw.IsTombstone {
		t.Error("expected raw record to be tombstone")
	}
	if raw.Version != 3 {
		t.Errorf("expected tombstone version 3, got %d", raw.Version)
	}

	// 4. Overwrite tombstone with higher version
	err = eng.PutRecord("kv", api.Record{
		PartitionKey: "user#1",
		SortKey:      "meta",
		Value:        "third_value",
	})
	if err != nil {
		t.Fatalf("third put failed: %v", err)
	}

	rec3, err := eng.GetRecord("kv", "user#1", "meta")
	if err != nil {
		t.Fatalf("get record 3 failed: %v", err)
	}
	if rec3.Version != 4 {
		t.Errorf("expected version 4, got %d", rec3.Version)
	}
	if rec3.IsTombstone {
		t.Error("expected IsTombstone to be false after new put")
	}
}

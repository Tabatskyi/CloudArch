package storage

import (
	"testing"

	"github.com/vasenka88/cloudarch/pkg/api"
)

func TestSkiplistEdgeCases(t *testing.T) {
	op := NewOrderedPartition()
	if op.Count() != 0 {
		t.Fatalf("expected 0, got %d", op.Count())
	}

	// Delete non-existent
	if op.Delete("non_existent") {
		t.Fatalf("expected delete to return false")
	}

	// Exists non-existent
	if op.Exists("non_existent") {
		t.Fatalf("expected exists to return false")
	}

	// Insert and overwrite
	op.Put(api.Record{SortKey: "k1", Value: "val1"})
	op.Put(api.Record{SortKey: "k1", Value: "val1_updated"})
	if op.Count() != 1 {
		t.Fatalf("expected 1, got %d", op.Count())
	}
	r, found := op.Get("k1")
	if !found || r.Value != "val1_updated" {
		t.Fatalf("expected val1_updated, got %v", r.Value)
	}

	// Range without bounds
	op.Put(api.Record{SortKey: "k0", Value: "val0"})
	op.Put(api.Record{SortKey: "k2", Value: "val2"})
	all := op.Range("", "", -1)
	if len(all) != 3 {
		t.Fatalf("expected 3, got %d", len(all))
	}

	// Prefix
	prefixed := op.Prefix("k", 10)
	if len(prefixed) != 3 {
		t.Fatalf("expected 3, got %d", len(prefixed))
	}

	// Delete
	if !op.Delete("k1") {
		t.Fatalf("expected delete to return true")
	}
	if op.Count() != 2 {
		t.Fatalf("expected 2, got %d", op.Count())
	}
}

func TestEngineErrorConditions(t *testing.T) {
	eng := NewEngine("")

	// Create table with empty name or partitionKey
	if err := eng.CreateTable(api.TableDefinition{}); err == nil {
		t.Fatalf("expected error for empty table definition")
	}

	// Non-existent table operations
	if _, err := eng.GetTable("missing"); err != ErrTableNotFound {
		t.Fatalf("expected ErrTableNotFound, got %v", err)
	}
	if err := eng.PutRecord("missing", api.Record{PartitionKey: "p"}); err != ErrTableNotFound {
		t.Fatalf("expected ErrTableNotFound, got %v", err)
	}
	if _, err := eng.GetRecord("missing", "p", ""); err != ErrTableNotFound {
		t.Fatalf("expected ErrTableNotFound, got %v", err)
	}
	if _, err := eng.ExistsRecord("missing", "p", ""); err != ErrTableNotFound {
		t.Fatalf("expected ErrTableNotFound, got %v", err)
	}
	if err := eng.DeleteRecord("missing", "p", ""); err != ErrTableNotFound {
		t.Fatalf("expected ErrTableNotFound, got %v", err)
	}
	if _, err := eng.QueryPartition("missing", "p", "", "", 10); err != ErrTableNotFound {
		t.Fatalf("expected ErrTableNotFound, got %v", err)
	}

	// Put with missing partition key
	_ = eng.CreateTable(api.TableDefinition{TableName: "t", PartitionKey: "pk"})
	if err := eng.PutRecord("t", api.Record{}); err != ErrMissingPartitionKey {
		t.Fatalf("expected ErrMissingPartitionKey, got %v", err)
	}

	// Query non-existent partition
	results, err := eng.QueryPartition("t", "non_existent_pk", "", "", 10)
	if err != nil || len(results) != 0 {
		t.Fatalf("expected empty results for non-existent partition, got %v, err: %v", results, err)
	}

	// Exists on non-existent partition
	exists, err := eng.ExistsRecord("t", "non_existent_pk", "")
	if err != nil || exists {
		t.Fatalf("expected false, got %v, err: %v", exists, err)
	}

	// TotalRecords
	if eng.TotalRecords() != 0 {
		t.Fatalf("expected 0 total records, got %d", eng.TotalRecords())
	}
}

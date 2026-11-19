package storage

import (
	"testing"

	"github.com/vasenka88/cloudarch/pkg/api"
)

func TestPartitionHash_Consistency(t *testing.T) {
	p1 := NewOrderedPartition()
	p2 := NewOrderedPartition()

	r1 := api.Record{PartitionKey: "p1", SortKey: "a", Value: "val1", Version: 1, Timestamp: 100}
	r2 := api.Record{PartitionKey: "p1", SortKey: "b", Value: "val2", Version: 1, Timestamp: 200}

	p1.Put(r1)
	p1.Put(r2)

	// Insert in reverse order into p2; since OrderedPartition orders by sortKey, hashes must match
	p2.Put(r2)
	p2.Put(r1)

	h1 := CalculatePartitionHash(p1)
	h2 := CalculatePartitionHash(p2)

	if h1 == "" || h2 == "" {
		t.Fatal("hashes should not be empty")
	}
	if h1 != h2 {
		t.Fatalf("expected identical hashes regardless of insertion order, got %s != %s", h1, h2)
	}
}

func TestPartitionHash_Divergence(t *testing.T) {
	p1 := NewOrderedPartition()
	p2 := NewOrderedPartition()

	r1 := api.Record{PartitionKey: "p1", SortKey: "a", Value: "val1", Version: 1, Timestamp: 100}
	r2 := api.Record{PartitionKey: "p1", SortKey: "a", Value: "val2", Version: 2, Timestamp: 150}

	p1.Put(r1)
	p2.Put(r2)

	h1 := CalculatePartitionHash(p1)
	h2 := CalculatePartitionHash(p2)

	if h1 == h2 {
		t.Fatalf("expected different hashes for different values/versions, got %s == %s", h1, h2)
	}
}

func TestPartitionHash_TombstoneIgnored(t *testing.T) {
	p1 := NewOrderedPartition()
	p2 := NewOrderedPartition()

	r1 := api.Record{PartitionKey: "p1", SortKey: "a", Value: "val1", Version: 1, Timestamp: 100}
	p1.Put(r1)

	// p2 has r1 then a tombstone for r2
	p2.Put(r1)
	p2.Put(api.Record{PartitionKey: "p1", SortKey: "b", Value: nil, Version: 2, Timestamp: 200, IsTombstone: true})

	h1 := CalculatePartitionHash(p1)
	h2 := CalculatePartitionHash(p2)

	if h1 != h2 {
		t.Fatalf("tombstone should be excluded from active partition hash, got %s != %s", h1, h2)
	}
}

func TestEngineStateHash_VerifySync(t *testing.T) {
	e1 := NewEngine("")
	e2 := NewEngine("")

	def := api.TableDefinition{TableName: "users", PartitionKey: "id"}
	_ = e1.CreateTable(def)
	_ = e2.CreateTable(def)

	rec := api.Record{PartitionKey: "user-1", Value: "data", Version: 1, Timestamp: 50}
	_ = e1.PutRecord("users", rec)
	_ = e2.PutRecord("users", rec)

	h1 := CalculateEngineStateHash(e1)
	h2 := CalculateEngineStateHash(e2)

	inSync, reason := VerifyStateSync(10, 10, h1, h2)
	if !inSync {
		t.Fatalf("expected in sync, got reason: %s", reason)
	}

	// Diverge
	_ = e2.PutRecord("users", api.Record{PartitionKey: "user-2", Value: "extra", Version: 1, Timestamp: 60})
	h2Diverged := CalculateEngineStateHash(e2)

	inSync, reason = VerifyStateSync(10, 10, h1, h2Diverged)
	if inSync {
		t.Fatal("expected divergence to be detected")
	}
	if reason == "" {
		t.Fatal("expected non-empty reason for divergence")
	}
}

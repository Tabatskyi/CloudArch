package ring

import (
	"fmt"
	"math"
	"testing"
)

func TestEmptyRing(t *testing.T) {
	r := NewRing(150, nil)
	_, _, err := r.GetNode("user_123")
	if err != ErrEmptyRing {
		t.Fatalf("expected ErrEmptyRing, got %v", err)
	}
	if count := r.MemberCount(); count != 0 {
		t.Fatalf("expected 0 members, got %d", count)
	}
}

func TestAddAndGetNode(t *testing.T) {
	r := NewRing(150, nil)
	r.AddNode("shard-1", "10.0.10.10:8081")

	shardID, addr, err := r.GetNode("user_123")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if shardID != "shard-1" || addr != "10.0.10.10:8081" {
		t.Fatalf("expected shard-1 / 10.0.10.10:8081, got %s / %s", shardID, addr)
	}

	// Determinism: same key returns same node
	for i := 0; i < 100; i++ {
		sid, _, _ := r.GetNode("user_123")
		if sid != "shard-1" {
			t.Fatalf("lookup not deterministic")
		}
	}
}

func TestMultiNodeRouting(t *testing.T) {
	r := NewRing(200, nil)
	r.AddNode("shard-1", "10.0.10.10:8081")
	r.AddNode("shard-2", "10.0.10.20:8081")

	if r.MemberCount() != 2 {
		t.Fatalf("expected 2 members, got %d", r.MemberCount())
	}

	members := r.GetMembers()
	if members["shard-1"] != "10.0.10.10:8081" || members["shard-2"] != "10.0.10.20:8081" {
		t.Fatalf("unexpected members: %+v", members)
	}

	// Verify both shards receive keys
	counts := make(map[string]int)
	totalKeys := 1000
	for i := 0; i < totalKeys; i++ {
		key := fmt.Sprintf("test_key_%d", i)
		sid, _, err := r.GetNode(key)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		counts[sid]++
	}

	if counts["shard-1"] == 0 || counts["shard-2"] == 0 {
		t.Fatalf("expected both shards to receive traffic, got %+v", counts)
	}
}

func TestDistributionUniformity(t *testing.T) {
	// 2 shards with 200 virtual nodes each, testing standard deviation
	r := NewRing(200, nil)
	r.AddNode("shard-1", "10.0.10.10:8081")
	r.AddNode("shard-2", "10.0.10.20:8081")

	totalKeys := 10000
	counts := make(map[string]int)
	for i := 0; i < totalKeys; i++ {
		key := fmt.Sprintf("dist_key_%d", i)
		sid, _, err := r.GetNode(key)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		counts[sid]++
	}

	expected := float64(totalKeys) / 2.0 // 5000
	for sid, count := range counts {
		diff := math.Abs(float64(count) - expected)
		variancePct := (diff / expected) * 100.0
		t.Logf("Shard %s received %d keys (variance: %.2f%%)", sid, count, variancePct)
		if variancePct > 8.0 {
			t.Fatalf("Shard %s variance %.2f%% exceeds 8%% threshold", sid, variancePct)
		}
	}
}

func TestRemoveNode(t *testing.T) {
	r := NewRing(100, nil)
	r.AddNode("shard-1", "10.0.10.10:8081")
	r.AddNode("shard-2", "10.0.10.20:8081")

	err := r.RemoveNode("shard-nonexistent")
	if err != ErrNodeNotFound {
		t.Fatalf("expected ErrNodeNotFound, got %v", err)
	}

	err = r.RemoveNode("shard-1")
	if err != nil {
		t.Fatalf("unexpected error removing node: %v", err)
	}

	if r.MemberCount() != 1 {
		t.Fatalf("expected 1 member, got %d", r.MemberCount())
	}

	// All keys must now route to shard-2
	for i := 0; i < 50; i++ {
		sid, _, err := r.GetNode(fmt.Sprintf("key_%d", i))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if sid != "shard-2" {
			t.Fatalf("expected shard-2, got %s", sid)
		}
	}
}

func TestUpdateNodeAddress(t *testing.T) {
	r := NewRing(50, nil)
	r.AddNode("shard-1", "10.0.10.10:8081")
	r.AddNode("shard-1", "10.0.10.99:8081") // update address

	if r.MemberCount() != 1 {
		t.Fatalf("expected 1 member, got %d", r.MemberCount())
	}

	_, addr, err := r.GetNode("any_key")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if addr != "10.0.10.99:8081" {
		t.Fatalf("expected updated address 10.0.10.99:8081, got %s", addr)
	}
}

func TestMurmur3Coverage(t *testing.T) {
	// Test various byte length tails (0, 1, 2, 3)
	Murmur3_32([]byte(""), 0)
	Murmur3_32([]byte("a"), 0)
	Murmur3_32([]byte("ab"), 0)
	Murmur3_32([]byte("abc"), 0)
	Murmur3_32([]byte("abcd"), 0)
	Murmur3_32([]byte("abcde"), 0)
}

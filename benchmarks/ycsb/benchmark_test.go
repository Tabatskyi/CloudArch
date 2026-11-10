package main

import (
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/vasenka88/cloudarch/pkg/coordinator"
	"github.com/vasenka88/cloudarch/pkg/shard"
)

func TestRunWorkloadsLive(t *testing.T) {
	// 1. Start Shard 1
	shard1 := shard.NewServer(shard.Config{ShardID: "shard-1", ListenAddr: ":0", VirtualNodes: 150})
	ts1 := httptest.NewServer(shard1)
	defer ts1.Close()
	addr1 := strings.TrimPrefix(ts1.URL, "http://")

	// 2. Start Shard 2
	shard2 := shard.NewServer(shard.Config{ShardID: "shard-2", ListenAddr: ":0", VirtualNodes: 150})
	ts2 := httptest.NewServer(shard2)
	defer ts2.Close()
	addr2 := strings.TrimPrefix(ts2.URL, "http://")

	// 3. Start Coordinator
	coord := coordinator.NewServer(coordinator.Config{ListenAddr: ":0", VirtualNodes: 150})
	coordServer := httptest.NewServer(coord)
	defer coordServer.Close()

	// Register shards
	coord.Ring().AddNode("shard-1", addr1)
	coord.Ring().AddNode("shard-2", addr2)

	client := coordServer.Client()

	// Initialize table and preload records
	ensureTable(coordServer.URL, client)
	preloadRecords(coordServer.URL, client, 500)

	// Workload A (50% Read, 50% Write)
	resA := executeWorkload(coordServer.URL, client, 2000, 8, 500, 0.50)
	printResults("A", resA)

	if resA.TotalOps == 0 || resA.ErrorOps > 0 {
		t.Fatalf("Workload A encountered errors: %+v", resA)
	}

	// Workload B (95% Read, 5% Write)
	resB := executeWorkload(coordServer.URL, client, 2000, 8, 500, 0.95)
	printResults("B", resB)

	if resB.TotalOps == 0 || resB.ErrorOps > 0 {
		t.Fatalf("Workload B encountered errors: %+v", resB)
	}

	fmt.Printf("\n[BENCHMARK_SUMMARY_OK]\n")
}

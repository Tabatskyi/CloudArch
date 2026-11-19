package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/vasenka88/cloudarch/pkg/api"
	"github.com/vasenka88/cloudarch/pkg/coordinator"
	"github.com/vasenka88/cloudarch/pkg/shard"
)

func TestRunWorkloadsLive(t *testing.T) {
	// 1. Start Shard 1 with gRPC
	shard1 := shard.NewServer(shard.Config{
		ShardID:        "shard-1",
		ReplicaID:      "shard-1-r1",
		Role:           "leader",
		ListenAddr:     ":0",
		GRPCListenAddr: "127.0.0.1:0",
		VirtualNodes:   150,
	})
	if err := shard1.StartGRPC(); err != nil {
		t.Fatalf("failed to start gRPC shard 1: %v", err)
	}
	defer shard1.Stop(context.Background())
	ts1 := httptest.NewServer(shard1)
	defer ts1.Close()
	addr1 := strings.TrimPrefix(ts1.URL, "http://")
	grpc1 := shard1.GRPCAddr()

	// 2. Start Shard 2 with gRPC
	shard2 := shard.NewServer(shard.Config{
		ShardID:        "shard-2",
		ReplicaID:      "shard-2-r1",
		Role:           "leader",
		ListenAddr:     ":0",
		GRPCListenAddr: "127.0.0.1:0",
		VirtualNodes:   150,
	})
	if err := shard2.StartGRPC(); err != nil {
		t.Fatalf("failed to start gRPC shard 2: %v", err)
	}
	defer shard2.Stop(context.Background())
	ts2 := httptest.NewServer(shard2)
	defer ts2.Close()
	addr2 := strings.TrimPrefix(ts2.URL, "http://")
	grpc2 := shard2.GRPCAddr()

	// 3. Start Coordinator (W=1, R=1 for baseline test)
	coord := coordinator.NewServer(coordinator.Config{
		ListenAddr:   ":0",
		VirtualNodes: 150,
		WriteQuorum:  1,
		ReadQuorum:   1,
	})
	coordServer := httptest.NewServer(coord)
	defer coordServer.Close()

	client := coordServer.Client()

	// Register shards via coordinator HTTP API
	reg1, _ := json.Marshal(api.ShardRegistration{
		ShardID:     "shard-1",
		ReplicaID:   "shard-1-r1",
		Role:        "leader",
		GRPCAddress: grpc1,
		HTTPAddress: addr1,
	})
	_, _ = client.Post(coordServer.URL+"/internal/v1/shards", "application/json", bytes.NewReader(reg1))

	reg2, _ := json.Marshal(api.ShardRegistration{
		ShardID:     "shard-2",
		ReplicaID:   "shard-2-r1",
		Role:        "leader",
		GRPCAddress: grpc2,
		HTTPAddress: addr2,
	})
	_, _ = client.Post(coordServer.URL+"/internal/v1/shards", "application/json", bytes.NewReader(reg2))

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

func TestRunM3Benchmark(t *testing.T) {
	runM3Benchmark()
}

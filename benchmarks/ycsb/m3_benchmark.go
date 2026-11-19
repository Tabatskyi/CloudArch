package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/vasenka88/cloudarch/pkg/api"
	"github.com/vasenka88/cloudarch/pkg/coordinator"
	"github.com/vasenka88/cloudarch/pkg/shard"
	"github.com/vasenka88/cloudarch/pkg/storage"
)

type clusterNode struct {
	shardID    string
	replicaID  string
	role       string
	server     *shard.Server
	httpServer *httptest.Server
	wal        *storage.WAL
	grpcAddr   string
	httpAddr   string
}

func startClusterNode(tmpDir, shardID, replicaID, role string) (*clusterNode, error) {
	nodeDir := filepath.Join(tmpDir, replicaID)
	_ = os.MkdirAll(nodeDir, 0755)

	wal, err := storage.OpenWAL(filepath.Join(nodeDir, replicaID+".wal"))
	if err != nil {
		return nil, err
	}

	s := shard.NewServer(shard.Config{
		ShardID:        shardID,
		ReplicaID:      replicaID,
		Role:           role,
		ListenAddr:     ":0",
		GRPCListenAddr: "127.0.0.1:0",
		DataDir:        nodeDir,
		VirtualNodes:   150,
		WriteQuorum:    2,
		ReadQuorum:     2,
	})
	s.SetWAL(wal)

	replServer := shard.NewReplicationServer(replicaID, s.Engine(), wal)
	s.SetReplicationServer(replServer)

	if err := s.StartGRPC(); err != nil {
		return nil, err
	}

	ts := httptest.NewServer(s)
	httpAddr := strings.TrimPrefix(ts.URL, "http://")
	grpcAddr := s.GRPCAddr()

	return &clusterNode{
		shardID:    shardID,
		replicaID:  replicaID,
		role:       role,
		server:     s,
		httpServer: ts,
		wal:        wal,
		grpcAddr:   grpcAddr,
		httpAddr:   httpAddr,
	}, nil
}

func runM3Benchmark() {
	tmpDir, _ := os.MkdirTemp("", "m3-bench-*")
	defer os.RemoveAll(tmpDir)

	fmt.Println(">> Starting 3-replica cluster (2 Shard Groups x 3 Replicas = 6 Storage Nodes)...")

	groups := []string{"shard-1", "shard-2"}
	nodes := make(map[string]*clusterNode)
	leaders := make(map[string]*clusterNode)

	for _, g := range groups {
		// 1 Leader + 2 Followers
		leader, err := startClusterNode(tmpDir, g, g+"-leader", "leader")
		if err != nil {
			panic(err)
		}
		f1, err := startClusterNode(tmpDir, g, g+"-f1", "follower")
		if err != nil {
			panic(err)
		}
		f2, err := startClusterNode(tmpDir, g, g+"-f2", "follower")
		if err != nil {
			panic(err)
		}

		// Connect Leader QuorumCoordinator to followers
		qc := shard.NewQuorumCoordinator(leader.replicaID, leader.wal, leader.server.Engine(), nil, 2, 2*time.Second)
		_ = qc.AddPeer(f1.grpcAddr)
		_ = qc.AddPeer(f2.grpcAddr)
		leader.server.SetQuorumCoordinator(qc)

		nodes[leader.replicaID] = leader
		nodes[f1.replicaID] = f1
		nodes[f2.replicaID] = f2
		leaders[g] = leader
	}

	// Start Coordinator with W=2, R=2
	coord := coordinator.NewServer(coordinator.Config{
		ListenAddr:   ":0",
		VirtualNodes: 150,
		WriteQuorum:  2,
		ReadQuorum:   2,
	})
	coordServer := httptest.NewServer(coord)
	defer coordServer.Close()
	client := coordServer.Client()

	// Register all 6 replicas with Coordinator
	for _, n := range nodes {
		reg, _ := json.Marshal(api.ShardRegistration{
			ShardID:     n.shardID,
			ReplicaID:   n.replicaID,
			Role:        n.role,
			GRPCAddress: n.grpcAddr,
			HTTPAddress: n.httpAddr,
		})
		_, _ = client.Post(coordServer.URL+"/internal/v1/shards", "application/json", bytes.NewReader(reg))
	}

	fmt.Println(">> Initializing table usertable and preloading 500 records...")
	ensureTable(coordServer.URL, client)
	preloadRecords(coordServer.URL, client, 500)

	// Workload A (50% Read, 50% Write)
	fmt.Println(">> Running Workload A (50% Read, 50% Write, 2000 ops, 8 workers)...")
	resA := executeWorkload(coordServer.URL, client, 2000, 8, 500, 0.50)
	printResults("A", resA)

	// Workload B (95% Read, 5% Write)
	fmt.Println(">> Running Workload B (95% Read, 5% Write, 2000 ops, 8 workers)...")
	resB := executeWorkload(coordServer.URL, client, 2000, 8, 500, 0.95)
	printResults("B", resB)

	// Dump JSON for report inclusion
	out := map[string]interface{}{
		"workloadA": resA,
		"workloadB": resB,
	}
	f, err := os.Create("m3_results.json")
	if err != nil {
		f, _ = os.Create("benchmarks/ycsb/m3_results.json")
	}
	if f != nil {
		_ = json.NewEncoder(f).Encode(out)
		_ = f.Close()
	}

	fmt.Println(">> Benchmark run complete. Results saved to m3_results.json")
}

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/vasenka88/cloudarch/pkg/shard"
)

func main() {
	var (
		shardID       = flag.String("id", envOr("SHARD_ID", "shard-1"), "Unique Shard ID (replica group)")
		replicaID     = flag.String("replica-id", envOr("REPLICA_ID", ""), "Unique Replica ID within group")
		role          = flag.String("role", envOr("SHARD_ROLE", "leader"), "Role: leader or follower")
		port          = flag.Int("port", 8081, "HTTP Port to listen on")
		advertiseAddr = flag.String("advertise", envOr("SHARD_ADVERTISE", "localhost:8081"), "HTTP address advertised to coordinator")
		grpcPort      = flag.Int("grpc-port", 9091, "gRPC Port to listen on")
		grpcAdvertise = flag.String("grpc-advertise", envOr("GRPC_ADVERTISE", "localhost:9091"), "gRPC address advertised to coordinator")
		leaderAddr    = flag.String("leader-addr", envOr("LEADER_ADDR", ""), "Leader gRPC address (for followers)")
		peersFlag     = flag.String("peers", envOr("SHARD_PEERS", ""), "Comma-separated peer replica gRPC addresses")
		coordURL      = flag.String("coord", envOr("COORDINATOR_URL", "http://localhost:8080"), "Coordinator endpoint URL")
		dataDir       = flag.String("data-dir", envOr("DATA_DIR", "/tmp/kv-shard-data"), "Directory for persistent storage snapshots")
		vnodes        = flag.Int("vnodes", 200, "Number of virtual nodes in consistent hash ring")
	)
	flag.Parse()

	var peers []string
	if *peersFlag != "" {
		for _, p := range strings.Split(*peersFlag, ",") {
			p = strings.TrimSpace(p)
			if p != "" {
				peers = append(peers, p)
			}
		}
	}

	listenAddr := fmt.Sprintf("0.0.0.0:%d", *port)
	grpcListenAddr := fmt.Sprintf("0.0.0.0:%d", *grpcPort)

	rid := *replicaID
	if rid == "" {
		rid = *shardID
	}

	cfg := shard.Config{
		ShardID:           *shardID,
		ReplicaID:         rid,
		Role:              *role,
		ListenAddr:        listenAddr,
		AdvertiseAddr:     *advertiseAddr,
		GRPCListenAddr:    grpcListenAddr,
		GRPCAdvertiseAddr: *grpcAdvertise,
		LeaderAddr:        *leaderAddr,
		Peers:             peers,
		CoordinatorURL:    *coordURL,
		DataDir:           *dataDir,
		VirtualNodes:      *vnodes,
		WriteQuorum:       2,
		ReadQuorum:        2,
	}

	server := shard.NewServer(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Launch background registration with coordinator
	server.StartBootstrapLoop(ctx)

	// Graceful shutdown handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		log.Printf("[shard:%s] listening on %s (advertising %s)", *shardID, listenAddr, *advertiseAddr)
		if err := server.Start(); err != nil {
			log.Fatalf("[shard:%s] server error: %v", *shardID, err)
		}
	}()

	<-sigChan
	log.Printf("[shard:%s] shutting down gracefully...", *shardID)
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = server.Stop(shutdownCtx)
}

func envOr(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}

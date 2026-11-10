package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/vasenka88/cloudarch/pkg/shard"
)

func main() {
	var (
		shardID        = flag.String("id", envOr("SHARD_ID", "shard-1"), "Unique Shard ID")
		port           = flag.Int("port", 8081, "Port to listen on")
		advertiseAddr  = flag.String("advertise", envOr("SHARD_ADVERTISE", "localhost:8081"), "Address advertised to coordinator")
		coordURL       = flag.String("coord", envOr("COORDINATOR_URL", "http://localhost:8080"), "Coordinator endpoint URL")
		dataDir        = flag.String("data-dir", envOr("DATA_DIR", "/tmp/kv-shard-data"), "Directory for persistent storage snapshots")
		vnodes         = flag.Int("vnodes", 200, "Number of virtual nodes in consistent hash ring")
	)
	flag.Parse()

	listenAddr := fmt.Sprintf("0.0.0.0:%d", *port)
	cfg := shard.Config{
		ShardID:        *shardID,
		ListenAddr:     listenAddr,
		AdvertiseAddr:  *advertiseAddr,
		CoordinatorURL: *coordURL,
		DataDir:        *dataDir,
		VirtualNodes:   *vnodes,
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

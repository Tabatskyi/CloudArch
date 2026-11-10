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

	"github.com/vasenka88/cloudarch/pkg/coordinator"
)

func main() {
	var (
		port   = flag.Int("port", 8080, "Port for coordinator HTTP server")
		vnodes = flag.Int("vnodes", 200, "Number of virtual nodes per physical shard")
	)
	flag.Parse()

	listenAddr := fmt.Sprintf("0.0.0.0:%d", *port)
	cfg := coordinator.Config{
		ListenAddr:   listenAddr,
		VirtualNodes: *vnodes,
	}

	server := coordinator.NewServer(cfg)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		log.Printf("[coordinator] listening on %s (virtual nodes per shard: %d)", listenAddr, *vnodes)
		if err := server.Start(); err != nil {
			log.Fatalf("[coordinator] server error: %v", err)
		}
	}()

	<-sigChan
	log.Printf("[coordinator] shutting down gracefully...")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = server.Stop(shutdownCtx)
}

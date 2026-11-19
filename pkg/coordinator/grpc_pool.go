package coordinator

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	v1 "github.com/vasenka88/cloudarch/pkg/proto/v1"
)

// GRPCPool manages persistent, multiplexed gRPC client connections to storage shards.
type GRPCPool struct {
	mu           sync.RWMutex
	conns        map[string]*grpc.ClientConn
	shardClients map[string]v1.ShardServiceClient
	dialer       func(context.Context, string) (net.Conn, error)
	closed       bool
}

// NewGRPCPool constructs a new gRPC connection pool.
func NewGRPCPool() *GRPCPool {
	return &GRPCPool{
		conns:        make(map[string]*grpc.ClientConn),
		shardClients: make(map[string]v1.ShardServiceClient),
	}
}

// SetDialer allows injecting a custom dialer (such as bufconn for testing).
func (p *GRPCPool) SetDialer(dialer func(context.Context, string) (net.Conn, error)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.dialer = dialer
}

// GetClientConn returns an existing connection or dials a new one to targetAddr.
func (p *GRPCPool) GetClientConn(targetAddr string) (*grpc.ClientConn, error) {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return nil, fmt.Errorf("connection pool is closed")
	}
	conn, exists := p.conns[targetAddr]
	p.mu.RUnlock()

	if exists {
		return conn, nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	// Double check after acquiring write lock
	if conn, exists := p.conns[targetAddr]; exists {
		return conn, nil
	}

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                15 * time.Second,
			Timeout:             3 * time.Second,
			PermitWithoutStream: true,
		}),
	}

	if p.dialer != nil {
		opts = append(opts, grpc.WithContextDialer(p.dialer))
	}

	newConn, err := grpc.NewClient(targetAddr, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to dial target %s: %w", targetAddr, err)
	}

	p.conns[targetAddr] = newConn
	p.shardClients[targetAddr] = v1.NewShardServiceClient(newConn)

	return newConn, nil
}

// GetShardClient returns a v1.ShardServiceClient for the target address, reusing existing channels.
func (p *GRPCPool) GetShardClient(targetAddr string) (v1.ShardServiceClient, error) {
	p.mu.RLock()
	client, exists := p.shardClients[targetAddr]
	p.mu.RUnlock()

	if exists {
		return client, nil
	}

	if _, err := p.GetClientConn(targetAddr); err != nil {
		return nil, err
	}

	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.shardClients[targetAddr], nil
}

// ConnectionCount returns the number of active cached connections.
func (p *GRPCPool) ConnectionCount() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.conns)
}

// Close closes all pooled gRPC connections.
func (p *GRPCPool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil
	}
	p.closed = true

	var firstErr error
	for _, conn := range p.conns {
		if err := conn.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	p.conns = make(map[string]*grpc.ClientConn)
	p.shardClients = make(map[string]v1.ShardServiceClient)
	return firstErr
}

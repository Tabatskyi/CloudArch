package coordinator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/vasenka88/cloudarch/pkg/api"
	v1 "github.com/vasenka88/cloudarch/pkg/proto/v1"
	"github.com/vasenka88/cloudarch/pkg/ring"
)

// Config configures the coordinator.
type Config struct {
	ListenAddr   string
	VirtualNodes int
	WriteQuorum  int
	ReadQuorum   int
}

// Server is the HTTP server for the Coordinator node.
type Server struct {
	cfg         Config
	ring        *ring.Ring
	grpcPool    *GRPCPool
	mu          sync.RWMutex
	tables      map[string]api.TableDefinition
	shards      map[string]api.ShardInfo
	mux         *http.ServeMux
	server      *http.Server
	startTime   time.Time
	httpClient  *http.Client
	readCounter uint64
	writeQuorum int
	readQuorum  int
}

// NewServer creates a new Coordinator server.
func NewServer(cfg Config) *Server {
	if cfg.VirtualNodes <= 0 {
		cfg.VirtualNodes = 200
	}
	wq := cfg.WriteQuorum
	if wq <= 0 {
		wq = 2
	}
	rq := cfg.ReadQuorum
	if rq <= 0 {
		rq = 2
	}

	server := &Server{
		cfg:         cfg,
		ring:        ring.NewRing(cfg.VirtualNodes, ring.MD5Hash),
		grpcPool:    NewGRPCPool(),
		tables:      make(map[string]api.TableDefinition),
		shards:      make(map[string]api.ShardInfo),
		mux:         http.NewServeMux(),
		startTime:   time.Now(),
		httpClient:  &http.Client{Timeout: 5 * time.Second},
		writeQuorum: wq,
		readQuorum:  rq,
	}
	server.registerRoutes()
	return server
}

// SetGRPCPool allows injecting a custom GRPCPool (e.g. for bufconn in unit testing).
func (server *Server) SetGRPCPool(pool *GRPCPool) {
	server.grpcPool = pool
}

// SetQuorum configures write and read quorum numbers.
func (server *Server) SetQuorum(w, r int) {
	server.writeQuorum = w
	server.readQuorum = r
}

// Ring returns the consistent hash ring instance.
func (server *Server) Ring() *ring.Ring {
	return server.ring
}

func (server *Server) registerRoutes() {
	server.mux.HandleFunc("/healthz", server.handleHealthz)
	server.mux.HandleFunc("/internal/v1/shards", server.handleInternalShards)
	server.mux.HandleFunc("/api/v1/tables", server.handleTables)
	server.mux.HandleFunc("/api/v1/tables/", server.handleTableSubroutes)
}

func (server *Server) handleHealthz(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	status := api.HealthStatus{
		Status:        "ok",
		NodeID:        "coordinator",
		UptimeSeconds: int64(time.Since(server.startTime).Seconds()),
		ActiveShards:  server.ring.MemberCount(),
	}
	writeJSON(writer, http.StatusOK, status)
}

func (server *Server) handleInternalShards(writer http.ResponseWriter, request *http.Request) {
	switch request.Method {
	case http.MethodPost:
		var reg api.ShardRegistration
		if err := json.NewDecoder(request.Body).Decode(&reg); err != nil {
			writeError(writer, http.StatusBadRequest, "invalid shard registration payload", err.Error())
			return
		}

		if reg.ShardID == "" {
			writeError(writer, http.StatusBadRequest, "shardId is required", "")
			return
		}
		if reg.Address == "" && reg.GRPCAddress == "" && reg.HTTPAddress == "" {
			writeError(writer, http.StatusBadRequest, "network address is required", "")
			return
		}

		// Defaults for backwards compatibility and role-based topology
		if reg.ReplicaID == "" {
			reg.ReplicaID = reg.ShardID
		}
		if reg.Role == "" {
			reg.Role = "leader"
		}
		if reg.GRPCAddress == "" {
			reg.GRPCAddress = reg.Address
		}
		if reg.HTTPAddress == "" {
			reg.HTTPAddress = reg.Address
		}
		if reg.Address == "" {
			reg.Address = reg.HTTPAddress
		}

		server.mu.Lock()
		server.ring.AddReplica(reg.ShardID, reg.ReplicaID, reg.Role, reg.GRPCAddress, reg.HTTPAddress)

		shardKey := fmt.Sprintf("%s/%s", reg.ShardID, reg.ReplicaID)
		server.shards[shardKey] = api.ShardInfo{
			ShardID:      reg.ShardID,
			ReplicaID:    reg.ReplicaID,
			Role:         reg.Role,
			GRPCAddress:  reg.GRPCAddress,
			HTTPAddress:  reg.HTTPAddress,
			Address:      reg.Address,
			Status:       "healthy",
			RegisteredAt: time.Now(),
		}

		currentTables := make([]api.TableDefinition, 0, len(server.tables))
		for _, table := range server.tables {
			currentTables = append(currentTables, table)
		}
		server.mu.Unlock()

		// Propagate existing tables to new shard if HTTP address is reachable
		if reg.HTTPAddress != "" {
			go func(addr string, tables []api.TableDefinition) {
				for _, tbl := range tables {
					body, _ := json.Marshal(tbl)
					reqURL := fmt.Sprintf("http://%s/api/v1/tables", addr)
					_, _ = server.httpClient.Post(reqURL, "application/json", bytes.NewBuffer(body))
				}
			}(reg.HTTPAddress, currentTables)
		}

		log.Printf("[coordinator] registered replica %s (group: %s, role: %s, gRPC: %s, HTTP: %s)", reg.ReplicaID, reg.ShardID, reg.Role, reg.GRPCAddress, reg.HTTPAddress)
		writeJSON(writer, http.StatusOK, api.SuccessResponse{
			Success: true,
			Message: fmt.Sprintf("replica %s registered successfully", reg.ReplicaID),
		})

	case http.MethodGet:
		server.mu.RLock()
		defer server.mu.RUnlock()

		list := make([]api.ShardInfo, 0, len(server.shards))
		for _, info := range server.shards {
			list = append(list, info)
		}
		sort.Slice(list, func(i, j int) bool {
			return list[i].ReplicaID < list[j].ReplicaID
		})
		writeJSON(writer, http.StatusOK, list)

	default:
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (server *Server) handleTables(writer http.ResponseWriter, request *http.Request) {
	switch request.Method {
	case http.MethodPost:
		var def api.TableDefinition
		if err := json.NewDecoder(request.Body).Decode(&def); err != nil {
			writeError(writer, http.StatusBadRequest, "invalid table payload", err.Error())
			return
		}

		if def.TableName == "" || def.PartitionKey == "" {
			writeError(writer, http.StatusBadRequest, "tableName and partitionKey are required", "")
			return
		}

		server.mu.Lock()
		if _, exists := server.tables[def.TableName]; exists {
			server.mu.Unlock()
			writeError(writer, http.StatusConflict, "table already exists", "")
			return
		}
		server.tables[def.TableName] = def
		var httpAddrs []string
		for _, info := range server.shards {
			addr := info.HTTPAddress
			if addr == "" {
				addr = info.Address
			}
			if addr != "" {
				httpAddrs = append(httpAddrs, addr)
			}
		}
		server.mu.Unlock()

		// Broadcast table creation to all active shards via HTTP
		for _, addr := range httpAddrs {
			body, _ := json.Marshal(def)
			reqURL := fmt.Sprintf("http://%s/api/v1/tables", addr)
			_, _ = server.httpClient.Post(reqURL, "application/json", bytes.NewBuffer(body))
		}

		writeJSON(writer, http.StatusCreated, def)

	case http.MethodGet:
		server.mu.RLock()
		defer server.mu.RUnlock()

		list := make([]api.TableDefinition, 0, len(server.tables))
		for _, table := range server.tables {
			list = append(list, table)
		}
		writeJSON(writer, http.StatusOK, list)

	default:
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (server *Server) handleTableSubroutes(writer http.ResponseWriter, request *http.Request) {
	path := strings.TrimPrefix(request.URL.Path, "/api/v1/tables/")
	parts := strings.Split(path, "/")
	if len(parts) == 0 || parts[0] == "" {
		http.NotFound(writer, request)
		return
	}

	tableName := parts[0]

	server.mu.RLock()
	tblDef, exists := server.tables[tableName]
	server.mu.RUnlock()

	if len(parts) == 1 {
		if request.Method == http.MethodGet {
			if !exists {
				writeError(writer, http.StatusNotFound, "table not found", "")
				return
			}
			writeJSON(writer, http.StatusOK, tblDef)
			return
		}
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if !exists {
		writeError(writer, http.StatusNotFound, "table not found", "")
		return
	}

	sub := parts[1]
	switch sub {
	case "query":
		server.proxyQuery(writer, request, tableName)
	case "records":
		server.proxyRecords(writer, request, tableName, tblDef, parts[2:])
	default:
		http.NotFound(writer, request)
	}
}

func (server *Server) proxyRecords(writer http.ResponseWriter, request *http.Request, table string, tblDef api.TableDefinition, keyParts []string) {
	// Guard: empty ring
	if server.ring.MemberCount() == 0 {
		writeError(writer, http.StatusServiceUnavailable, "no available storage shards in ring", "empty ring")
		return
	}

	switch request.Method {
	case http.MethodPost, http.MethodPut:
		bodyBytes, err := io.ReadAll(request.Body)
		if err != nil {
			writeError(writer, http.StatusBadRequest, "failed to read body", err.Error())
			return
		}

		var rec api.Record
		if err := json.Unmarshal(bodyBytes, &rec); err != nil {
			writeError(writer, http.StatusBadRequest, "invalid record payload", err.Error())
			return
		}

		if rec.PartitionKey == "" {
			writeError(writer, http.StatusBadRequest, "partitionKey is required", "")
			return
		}
		if tblDef.SortKey != "" && rec.SortKey == "" {
			writeError(writer, http.StatusBadRequest, "table requires sortKey but sortKey is missing", "")
			return
		}

		// 1. Resolve target replica group on hash ring
		group, err := server.ring.GetReplicaGroup(rec.PartitionKey)
		if err != nil {
			writeError(writer, http.StatusServiceUnavailable, "no available storage shards in ring", err.Error())
			return
		}

		// 2. Availability Guard: verify active replicas >= W
		if len(group.Members) < server.writeQuorum {
			writeError(writer, http.StatusServiceUnavailable, "write quorum unavailable: insufficient active replicas",
				fmt.Sprintf("target replica group %s has %d replicas, requires W=%d", group.GroupID, len(group.Members), server.writeQuorum))
			return
		}

		if group.Leader == nil || group.Leader.GRPCAddress == "" {
			writeError(writer, http.StatusServiceUnavailable, "write quorum unavailable: shard leader not found", "")
			return
		}

		// 3. Dispatch write to leader via gRPC awaiting W=2 quorum
		client, err := server.grpcPool.GetShardClient(group.Leader.GRPCAddress)
		if err != nil {
			writeError(writer, http.StatusBadGateway, "failed to connect to shard leader", err.Error())
			return
		}

		valBytes := encodeAnyValue(rec.Value)
		resp, err := client.PutRecord(request.Context(), &v1.PutRecordRequest{
			Table:        table,
			PartitionKey: rec.PartitionKey,
			SortKey:      rec.SortKey,
			Value:        valBytes,
		})
		if err != nil {
			st, _ := status.FromError(err)
			if st.Code() == codes.DeadlineExceeded || st.Code() == codes.Unavailable {
				writeError(writer, http.StatusServiceUnavailable, "write quorum unavailable", st.Message())
				return
			}
			writeError(writer, http.StatusInternalServerError, "write replication failed", st.Message())
			return
		}

		recordResp := api.RecordResponse{
			Success:      true,
			ShardID:      group.GroupID,
			Table:        table,
			PartitionKey: rec.PartitionKey,
			SortKey:      rec.SortKey,
			Version:      resp.Version,
		}
		writeJSON(writer, http.StatusOK, recordResp)

	case http.MethodDelete:
		if len(keyParts) == 0 || keyParts[0] == "" {
			writeError(writer, http.StatusBadRequest, "partitionKey is required", "")
			return
		}

		pk := keyParts[0]
		sk := request.URL.Query().Get("sortKey")
		if len(keyParts) > 1 && keyParts[1] != "" {
			sk = keyParts[1]
		}

		group, err := server.ring.GetReplicaGroup(pk)
		if err != nil {
			writeError(writer, http.StatusServiceUnavailable, "no available storage shards in ring", err.Error())
			return
		}

		// Guard: W availability
		if len(group.Members) < server.writeQuorum {
			writeError(writer, http.StatusServiceUnavailable, "write quorum unavailable: insufficient active replicas", "")
			return
		}
		if group.Leader == nil || group.Leader.GRPCAddress == "" {
			writeError(writer, http.StatusServiceUnavailable, "write quorum unavailable: shard leader not found", "")
			return
		}

		client, err := server.grpcPool.GetShardClient(group.Leader.GRPCAddress)
		if err != nil {
			writeError(writer, http.StatusBadGateway, "failed to connect to shard leader", err.Error())
			return
		}

		_, err = client.DeleteRecord(request.Context(), &v1.DeleteRecordRequest{
			Table:        table,
			PartitionKey: pk,
			SortKey:      sk,
		})
		if err != nil {
			st, _ := status.FromError(err)
			if st.Code() == codes.DeadlineExceeded || st.Code() == codes.Unavailable {
				writeError(writer, http.StatusServiceUnavailable, "write quorum unavailable", st.Message())
				return
			}
			writeError(writer, http.StatusInternalServerError, "delete replication failed", st.Message())
			return
		}

		writer.WriteHeader(http.StatusNoContent)

	case http.MethodGet, http.MethodHead:
		if len(keyParts) == 0 || keyParts[0] == "" {
			writeError(writer, http.StatusBadRequest, "partitionKey is required", "")
			return
		}

		pk := keyParts[0]
		sk := request.URL.Query().Get("sortKey")
		if len(keyParts) > 1 && keyParts[1] != "" {
			sk = keyParts[1]
		}

		group, err := server.ring.GetReplicaGroup(pk)
		if err != nil {
			if request.Method == http.MethodHead {
				writer.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			writeError(writer, http.StatusServiceUnavailable, "no available storage shards in ring", err.Error())
			return
		}

		// 1. Availability Guard: verify active replicas >= R
		if len(group.Members) < server.readQuorum {
			if request.Method == http.MethodHead {
				writer.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			writeError(writer, http.StatusServiceUnavailable, "read quorum unavailable: insufficient active replicas",
				fmt.Sprintf("target replica group %s has %d replicas, requires R=%d", group.GroupID, len(group.Members), server.readQuorum))
			return
		}

		// 2. Balanced Read Load Distribution: Select R=2 distinct replicas in round-robin fashion
		allMembers := make([]*ring.ReplicaMember, 0, len(group.Members))
		for _, m := range group.Members {
			if m.GRPCAddress != "" {
				allMembers = append(allMembers, m)
			}
		}
		sort.Slice(allMembers, func(i, j int) bool {
			return allMembers[i].ReplicaID < allMembers[j].ReplicaID
		})

		if len(allMembers) < server.readQuorum {
			writeError(writer, http.StatusServiceUnavailable, "read quorum unavailable: insufficient gRPC endpoints", "")
			return
		}

		// Select R distinct replicas via rotating index
		rCount := server.readQuorum
		startIdx := int(atomic.AddUint64(&server.readCounter, 1) % uint64(len(allMembers)))
		selected := make([]*ring.ReplicaMember, 0, rCount)
		for i := 0; i < rCount; i++ {
			idx := (startIdx + i) % len(allMembers)
			selected = append(selected, allMembers[idx])
		}

		// 3. Parallel gRPC Quorum Reads across R replicas
		type readResult struct {
			resp *v1.RecordResponse
			err  error
		}
		resultChan := make(chan readResult, len(selected))
		var wg sync.WaitGroup

		for _, member := range selected {
			wg.Add(1)
			go func(m *ring.ReplicaMember) {
				defer wg.Done()
				c, dialErr := server.grpcPool.GetShardClient(m.GRPCAddress)
				if dialErr != nil {
					resultChan <- readResult{err: dialErr}
					return
				}
				res, rErr := c.GetRecord(request.Context(), &v1.GetRecordRequest{
					Table:        table,
					PartitionKey: pk,
					SortKey:      sk,
				})
				resultChan <- readResult{resp: res, err: rErr}
			}(member)
		}

		wg.Wait()
		close(resultChan)

		// 4. Resolve highest record version to eliminate stale reads
		var newestRecord *v1.RecordResponse
		notFoundCount := 0
		successCount := 0

		for res := range resultChan {
			if res.err != nil {
				st, ok := status.FromError(res.err)
				if ok && st.Code() == codes.NotFound {
					notFoundCount++
				}
				continue
			}
			if res.resp != nil {
				successCount++
				if newestRecord == nil || res.resp.Version > newestRecord.Version {
					newestRecord = res.resp
				}
			}
		}

		// If no successful responses and all returned NotFound
		if successCount == 0 {
			if notFoundCount > 0 {
				writeError(writer, http.StatusNotFound, "record not found", "")
				return
			}
			writeError(writer, http.StatusBadGateway, "all quorum read replicas unreachable", "")
			return
		}

		// If newest version is a tombstone, record is logically deleted
		if newestRecord.IsTombstone {
			writeError(writer, http.StatusNotFound, "record not found", "tombstone")
			return
		}

		if request.Method == http.MethodHead {
			writer.WriteHeader(http.StatusOK)
			return
		}

		recOut := api.Record{
			PartitionKey: newestRecord.PartitionKey,
			SortKey:      newestRecord.SortKey,
			Value:        decodeBytesToValue(newestRecord.Value),
			Version:      newestRecord.Version,
			Timestamp:    newestRecord.Timestamp,
			IsTombstone:  false,
		}
		writeJSON(writer, http.StatusOK, recOut)

	default:
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (server *Server) proxyQuery(writer http.ResponseWriter, request *http.Request, table string) {
	if request.Method != http.MethodGet {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	pk := request.URL.Query().Get("partitionKey")
	if pk == "" {
		writeError(writer, http.StatusBadRequest, "partitionKey query parameter is required", "")
		return
	}

	group, err := server.ring.GetReplicaGroup(pk)
	if err != nil || group == nil {
		writeError(writer, http.StatusServiceUnavailable, "no available storage shards in ring", "")
		return
	}

	if len(group.Members) == 0 {
		writeError(writer, http.StatusServiceUnavailable, "no active replicas in shard group", "")
		return
	}

	// Pick a replica for query in round-robin fashion
	allMembers := make([]*ring.ReplicaMember, 0, len(group.Members))
	for _, m := range group.Members {
		if m.GRPCAddress != "" {
			allMembers = append(allMembers, m)
		}
	}
	if len(allMembers) == 0 {
		writeError(writer, http.StatusServiceUnavailable, "no reachable replicas in group", "")
		return
	}

	idx := int(atomic.AddUint64(&server.readCounter, 1) % uint64(len(allMembers)))
	target := allMembers[idx]

	client, err := server.grpcPool.GetShardClient(target.GRPCAddress)
	if err != nil {
		writeError(writer, http.StatusBadGateway, "failed connecting to replica", err.Error())
		return
	}

	limit := 100
	if l := request.URL.Query().Get("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil && parsed > 0 {
			limit = parsed
		}
	}

	sortFrom := request.URL.Query().Get("sortKeyFrom")
	sortTo := request.URL.Query().Get("sortKeyTo")
	prefix := request.URL.Query().Get("prefix")
	if prefix == "" {
		prefix = request.URL.Query().Get("sortKeyPrefix")
	}

	qResp, err := client.QueryRecords(request.Context(), &v1.QueryRecordsRequest{
		Table:        table,
		PartitionKey: pk,
		Prefix:       prefix,
		Limit:        int32(limit),
	})
	if err != nil {
		st, _ := status.FromError(err)
		if st.Code() == codes.NotFound {
			writeError(writer, http.StatusNotFound, "table not found", "")
			return
		}
		writeError(writer, http.StatusInternalServerError, "query failed", st.Message())
		return
	}

	records := make([]api.Record, 0, len(qResp.Records))
	for _, r := range qResp.Records {
		if !r.IsTombstone {
			if sortFrom != "" && r.SortKey < sortFrom {
				continue
			}
			if sortTo != "" && r.SortKey > sortTo {
				continue
			}
			records = append(records, api.Record{
				PartitionKey: r.PartitionKey,
				SortKey:      r.SortKey,
				Value:        decodeBytesToValue(r.Value),
				Version:      r.Version,
				Timestamp:    r.Timestamp,
			})
		}
	}

	writeJSON(writer, http.StatusOK, records)
}

func encodeAnyValue(v any) []byte {
	if v == nil {
		return nil
	}
	switch val := v.(type) {
	case []byte:
		return val
	case string:
		return []byte(val)
	case json.RawMessage:
		return []byte(val)
	default:
		b, err := json.Marshal(val)
		if err != nil {
			return nil
		}
		return b
	}
}

func decodeBytesToValue(b []byte) any {
	if len(b) == 0 {
		return nil
	}
	if json.Valid(b) {
		var decoded any
		if err := json.Unmarshal(b, &decoded); err == nil {
			return decoded
		}
	}
	return string(b)
}

// Start runs the HTTP server.
func (server *Server) Start() error {
	server.server = &http.Server{
		Addr:    server.cfg.ListenAddr,
		Handler: server.mux,
	}
	return server.server.ListenAndServe()
}

// Stop gracefully shuts down the coordinator.
func (server *Server) Stop(ctx context.Context) error {
	if server.grpcPool != nil {
		_ = server.grpcPool.Close()
	}
	if server.server != nil {
		return server.server.Shutdown(ctx)
	}
	return nil
}

// ServeHTTP implements http.Handler for testing.
func (server *Server) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	server.mux.ServeHTTP(writer, request)
}

func copyHeaders(dst, src http.Header) {
	for k, vv := range src {
		for _, v := range vv {
			dst.Add(k, v)
		}
	}
}

func writeJSON(writer http.ResponseWriter, status int, data any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(data)
}

func writeError(writer http.ResponseWriter, status int, msg, details string) {
	writeJSON(writer, status, api.ErrorResponse{
		Error:   msg,
		Details: details,
	})
}

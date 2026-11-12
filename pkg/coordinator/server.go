package coordinator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/vasenka88/cloudarch/pkg/api"
	"github.com/vasenka88/cloudarch/pkg/ring"
)

// Config configures the coordinator.
type Config struct {
	ListenAddr   string
	VirtualNodes int
}

// Server is the HTTP server for the Coordinator node.
type Server struct {
	cfg        Config
	ring       *ring.Ring
	mu         sync.RWMutex
	tables     map[string]api.TableDefinition
	shards     map[string]api.ShardInfo
	mux        *http.ServeMux
	server     *http.Server
	startTime  time.Time
	httpClient *http.Client
}

// NewServer creates a new Coordinator server.
func NewServer(cfg Config) *Server {
	if cfg.VirtualNodes <= 0 {
		cfg.VirtualNodes = 200
	}

	server := &Server{
		cfg:        cfg,
		ring:       ring.NewRing(cfg.VirtualNodes, ring.MD5Hash),
		tables:     make(map[string]api.TableDefinition),
		shards:     make(map[string]api.ShardInfo),
		mux:        http.NewServeMux(),
		startTime:  time.Now(),
		httpClient: &http.Client{Timeout: 5 * time.Second},
	}
	server.registerRoutes()
	return server
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

		if reg.ShardID == "" || reg.Address == "" {
			writeError(writer, http.StatusBadRequest, "shardId and address are required", "")
			return
		}

		server.mu.Lock()
		server.ring.AddNode(reg.ShardID, reg.Address)
		server.shards[reg.ShardID] = api.ShardInfo{
			ShardID:      reg.ShardID,
			Address:      reg.Address,
			Status:       "healthy",
			RegisteredAt: time.Now(),
		}
		// Capture current tables to sync with new shard
		currentTables := make([]api.TableDefinition, 0, len(server.tables))
		for _, table := range server.tables {
			currentTables = append(currentTables, table)
		}
		server.mu.Unlock()

		// Propagate existing table schemas to the newly registered shard
		go func(addr string, tables []api.TableDefinition) {
			for _, tbl := range tables {
				body, _ := json.Marshal(tbl)
				reqURL := fmt.Sprintf("http://%s/api/v1/tables", addr)
				_, _ = server.httpClient.Post(reqURL, "application/json", bytes.NewBuffer(body))
			}
		}(reg.Address, currentTables)

		log.Printf("[coordinator] registered shard %s at %s (total active shards: %d)", reg.ShardID, reg.Address, server.ring.MemberCount())
		writeJSON(writer, http.StatusOK, api.SuccessResponse{
			Success: true,
			Message: fmt.Sprintf("shard %s registered successfully", reg.ShardID),
		})

	case http.MethodGet:
		server.mu.RLock()
		defer server.mu.RUnlock()

		list := make([]api.ShardInfo, 0, len(server.shards))
		for _, info := range server.shards {
			list = append(list, info)
		}
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
		members := server.ring.GetMembers()
		server.mu.Unlock()

		// Broadcast table creation to all active shards
		for _, addr := range members {
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
		// /api/v1/tables/{table}
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

		// Resolve destination shard via consistent hash ring
		_, shardAddr, err := server.ring.GetNode(rec.PartitionKey)
		if err != nil {
			writeError(writer, http.StatusServiceUnavailable, "no available storage shards in ring", err.Error())
			return
		}

		targetURL := fmt.Sprintf("http://%s/api/v1/tables/%s/records", shardAddr, table)
		req, err := http.NewRequestWithContext(request.Context(), request.Method, targetURL, bytes.NewBuffer(bodyBytes))
		if err != nil {
			writeError(writer, http.StatusInternalServerError, "failed to create proxy request", err.Error())
			return
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := server.httpClient.Do(req)
		if err != nil {
			writeError(writer, http.StatusBadGateway, "shard unreachable", err.Error())
			return
		}
		defer resp.Body.Close()

		copyHeaders(writer.Header(), resp.Header)
		writer.WriteHeader(resp.StatusCode)
		_, _ = io.Copy(writer, resp.Body)

	case http.MethodGet, http.MethodHead, http.MethodDelete:
		if len(keyParts) == 0 || keyParts[0] == "" {
			writeError(writer, http.StatusBadRequest, "partitionKey is required", "")
			return
		}

		pk := keyParts[0]
		sk := request.URL.Query().Get("sortKey")
		if len(keyParts) > 1 && keyParts[1] != "" {
			sk = keyParts[1]
		}

		// Resolve destination shard
		_, shardAddr, err := server.ring.GetNode(pk)
		if err != nil {
			if request.Method == http.MethodHead {
				writer.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			writeError(writer, http.StatusServiceUnavailable, "no available storage shards in ring", err.Error())
			return
		}

		var targetURL string
		if sk != "" {
			targetURL = fmt.Sprintf("http://%s/api/v1/tables/%s/records/%s/%s", shardAddr, table, pk, sk)
		} else {
			targetURL = fmt.Sprintf("http://%s/api/v1/tables/%s/records/%s", shardAddr, table, pk)
		}

		req, err := http.NewRequestWithContext(request.Context(), request.Method, targetURL, nil)
		if err != nil {
			writeError(writer, http.StatusInternalServerError, "failed to create proxy request", err.Error())
			return
		}

		resp, err := server.httpClient.Do(req)
		if err != nil {
			if request.Method == http.MethodHead {
				writer.WriteHeader(http.StatusBadGateway)
				return
			}
			writeError(writer, http.StatusBadGateway, "shard unreachable", err.Error())
			return
		}
		defer resp.Body.Close()

		copyHeaders(writer.Header(), resp.Header)
		writer.WriteHeader(resp.StatusCode)
		_, _ = io.Copy(writer, resp.Body)

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

	_, shardAddr, err := server.ring.GetNode(pk)
	if err != nil {
		writeError(writer, http.StatusServiceUnavailable, "no available storage shards in ring", err.Error())
		return
	}

	targetURL := fmt.Sprintf("http://%s/api/v1/tables/%s/query?%s", shardAddr, table, request.URL.RawQuery)
	req, err := http.NewRequestWithContext(request.Context(), http.MethodGet, targetURL, nil)
	if err != nil {
		writeError(writer, http.StatusInternalServerError, "failed to create proxy query", err.Error())
		return
	}

	resp, err := server.httpClient.Do(req)
	if err != nil {
		writeError(writer, http.StatusBadGateway, "shard unreachable", err.Error())
		return
	}
	defer resp.Body.Close()

	copyHeaders(writer.Header(), resp.Header)
	writer.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(writer, resp.Body)
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

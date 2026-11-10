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

	s := &Server{
		cfg:        cfg,
		ring:       ring.NewRing(cfg.VirtualNodes, ring.MD5Hash),
		tables:     make(map[string]api.TableDefinition),
		shards:     make(map[string]api.ShardInfo),
		mux:        http.NewServeMux(),
		startTime:  time.Now(),
		httpClient: &http.Client{Timeout: 5 * time.Second},
	}
	s.registerRoutes()
	return s
}

// Ring returns the consistent hash ring instance.
func (s *Server) Ring() *ring.Ring {
	return s.ring
}

func (s *Server) registerRoutes() {
	s.mux.HandleFunc("/healthz", s.handleHealthz)
	s.mux.HandleFunc("/internal/v1/shards", s.handleInternalShards)
	s.mux.HandleFunc("/api/v1/tables", s.handleTables)
	s.mux.HandleFunc("/api/v1/tables/", s.handleTableSubroutes)
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	status := api.HealthStatus{
		Status:        "ok",
		NodeID:        "coordinator",
		UptimeSeconds: int64(time.Since(s.startTime).Seconds()),
		ActiveShards:  s.ring.MemberCount(),
	}
	writeJSON(w, http.StatusOK, status)
}

func (s *Server) handleInternalShards(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		var reg api.ShardRegistration
		if err := json.NewDecoder(r.Body).Decode(&reg); err != nil {
			writeError(w, http.StatusBadRequest, "invalid shard registration payload", err.Error())
			return
		}

		if reg.ShardID == "" || reg.Address == "" {
			writeError(w, http.StatusBadRequest, "shardId and address are required", "")
			return
		}

		s.mu.Lock()
		s.ring.AddNode(reg.ShardID, reg.Address)
		s.shards[reg.ShardID] = api.ShardInfo{
			ShardID:      reg.ShardID,
			Address:      reg.Address,
			Status:       "healthy",
			RegisteredAt: time.Now(),
		}
		// Capture current tables to sync with new shard
		currentTables := make([]api.TableDefinition, 0, len(s.tables))
		for _, t := range s.tables {
			currentTables = append(currentTables, t)
		}
		s.mu.Unlock()

		// Propagate existing table schemas to the newly registered shard
		go func(addr string, tables []api.TableDefinition) {
			for _, tbl := range tables {
				body, _ := json.Marshal(tbl)
				reqURL := fmt.Sprintf("http://%s/api/v1/tables", addr)
				_, _ = s.httpClient.Post(reqURL, "application/json", bytes.NewBuffer(body))
			}
		}(reg.Address, currentTables)

		log.Printf("[coordinator] registered shard %s at %s (total active shards: %d)", reg.ShardID, reg.Address, s.ring.MemberCount())
		writeJSON(w, http.StatusOK, api.SuccessResponse{
			Success: true,
			Message: fmt.Sprintf("shard %s registered successfully", reg.ShardID),
		})

	case http.MethodGet:
		s.mu.RLock()
		defer s.mu.RUnlock()

		list := make([]api.ShardInfo, 0, len(s.shards))
		for _, info := range s.shards {
			list = append(list, info)
		}
		writeJSON(w, http.StatusOK, list)

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleTables(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		var def api.TableDefinition
		if err := json.NewDecoder(r.Body).Decode(&def); err != nil {
			writeError(w, http.StatusBadRequest, "invalid table payload", err.Error())
			return
		}

		if def.TableName == "" || def.PartitionKey == "" {
			writeError(w, http.StatusBadRequest, "tableName and partitionKey are required", "")
			return
		}

		s.mu.Lock()
		if _, exists := s.tables[def.TableName]; exists {
			s.mu.Unlock()
			writeError(w, http.StatusConflict, "table already exists", "")
			return
		}
		s.tables[def.TableName] = def
		members := s.ring.GetMembers()
		s.mu.Unlock()

		// Broadcast table creation to all active shards
		for _, addr := range members {
			body, _ := json.Marshal(def)
			reqURL := fmt.Sprintf("http://%s/api/v1/tables", addr)
			_, _ = s.httpClient.Post(reqURL, "application/json", bytes.NewBuffer(body))
		}

		writeJSON(w, http.StatusCreated, def)

	case http.MethodGet:
		s.mu.RLock()
		defer s.mu.RUnlock()

		list := make([]api.TableDefinition, 0, len(s.tables))
		for _, t := range s.tables {
			list = append(list, t)
		}
		writeJSON(w, http.StatusOK, list)

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleTableSubroutes(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/tables/")
	parts := strings.Split(path, "/")
	if len(parts) == 0 || parts[0] == "" {
		http.NotFound(w, r)
		return
	}

	tableName := parts[0]

	s.mu.RLock()
	tblDef, exists := s.tables[tableName]
	s.mu.RUnlock()

	if len(parts) == 1 {
		// /api/v1/tables/{table}
		if r.Method == http.MethodGet {
			if !exists {
				writeError(w, http.StatusNotFound, "table not found", "")
				return
			}
			writeJSON(w, http.StatusOK, tblDef)
			return
		}
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if !exists {
		writeError(w, http.StatusNotFound, "table not found", "")
		return
	}

	sub := parts[1]
	switch sub {
	case "query":
		s.proxyQuery(w, r, tableName)
	case "records":
		s.proxyRecords(w, r, tableName, tblDef, parts[2:])
	default:
		http.NotFound(w, r)
	}
}

func (s *Server) proxyRecords(w http.ResponseWriter, r *http.Request, table string, tblDef api.TableDefinition, keyParts []string) {
	switch r.Method {
	case http.MethodPost, http.MethodPut:
		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			writeError(w, http.StatusBadRequest, "failed to read body", err.Error())
			return
		}

		var rec api.Record
		if err := json.Unmarshal(bodyBytes, &rec); err != nil {
			writeError(w, http.StatusBadRequest, "invalid record payload", err.Error())
			return
		}

		if rec.PartitionKey == "" {
			writeError(w, http.StatusBadRequest, "partitionKey is required", "")
			return
		}
		if tblDef.SortKey != "" && rec.SortKey == "" {
			writeError(w, http.StatusBadRequest, "table requires sortKey but sortKey is missing", "")
			return
		}

		// Resolve destination shard via consistent hash ring
		_, shardAddr, err := s.ring.GetNode(rec.PartitionKey)
		if err != nil {
			writeError(w, http.StatusServiceUnavailable, "no available storage shards in ring", err.Error())
			return
		}

		targetURL := fmt.Sprintf("http://%s/api/v1/tables/%s/records", shardAddr, table)
		req, err := http.NewRequestWithContext(r.Context(), r.Method, targetURL, bytes.NewBuffer(bodyBytes))
		if err != nil {
			writeError(w, http.StatusInternalServerError, "failed to create proxy request", err.Error())
			return
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := s.httpClient.Do(req)
		if err != nil {
			writeError(w, http.StatusBadGateway, "shard unreachable", err.Error())
			return
		}
		defer resp.Body.Close()

		copyHeaders(w.Header(), resp.Header)
		w.WriteHeader(resp.StatusCode)
		_, _ = io.Copy(w, resp.Body)

	case http.MethodGet, http.MethodHead, http.MethodDelete:
		if len(keyParts) == 0 || keyParts[0] == "" {
			writeError(w, http.StatusBadRequest, "partitionKey is required", "")
			return
		}

		pk := keyParts[0]
		sk := r.URL.Query().Get("sortKey")
		if len(keyParts) > 1 && keyParts[1] != "" {
			sk = keyParts[1]
		}

		// Resolve destination shard
		_, shardAddr, err := s.ring.GetNode(pk)
		if err != nil {
			if r.Method == http.MethodHead {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			writeError(w, http.StatusServiceUnavailable, "no available storage shards in ring", err.Error())
			return
		}

		var targetURL string
		if sk != "" {
			targetURL = fmt.Sprintf("http://%s/api/v1/tables/%s/records/%s/%s", shardAddr, table, pk, sk)
		} else {
			targetURL = fmt.Sprintf("http://%s/api/v1/tables/%s/records/%s", shardAddr, table, pk)
		}

		req, err := http.NewRequestWithContext(r.Context(), r.Method, targetURL, nil)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "failed to create proxy request", err.Error())
			return
		}

		resp, err := s.httpClient.Do(req)
		if err != nil {
			if r.Method == http.MethodHead {
				w.WriteHeader(http.StatusBadGateway)
				return
			}
			writeError(w, http.StatusBadGateway, "shard unreachable", err.Error())
			return
		}
		defer resp.Body.Close()

		copyHeaders(w.Header(), resp.Header)
		w.WriteHeader(resp.StatusCode)
		_, _ = io.Copy(w, resp.Body)

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) proxyQuery(w http.ResponseWriter, r *http.Request, table string) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	pk := r.URL.Query().Get("partitionKey")
	if pk == "" {
		writeError(w, http.StatusBadRequest, "partitionKey query parameter is required", "")
		return
	}

	_, shardAddr, err := s.ring.GetNode(pk)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "no available storage shards in ring", err.Error())
		return
	}

	targetURL := fmt.Sprintf("http://%s/api/v1/tables/%s/query?%s", shardAddr, table, r.URL.RawQuery)
	req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, targetURL, nil)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to create proxy query", err.Error())
		return
	}

	resp, err := s.httpClient.Do(req)
	if err != nil {
		writeError(w, http.StatusBadGateway, "shard unreachable", err.Error())
		return
	}
	defer resp.Body.Close()

	copyHeaders(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

// Start runs the HTTP server.
func (s *Server) Start() error {
	s.server = &http.Server{
		Addr:    s.cfg.ListenAddr,
		Handler: s.mux,
	}
	return s.server.ListenAndServe()
}

// Stop gracefully shuts down the coordinator.
func (s *Server) Stop(ctx context.Context) error {
	if s.server != nil {
		return s.server.Shutdown(ctx)
	}
	return nil
}

// ServeHTTP implements http.Handler for testing.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func copyHeaders(dst, src http.Header) {
	for k, vv := range src {
		for _, v := range vv {
			dst.Add(k, v)
		}
	}
}

func writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}

func writeError(w http.ResponseWriter, status int, msg, details string) {
	writeJSON(w, status, api.ErrorResponse{
		Error:   msg,
		Details: details,
	})
}

package shard

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/vasenka88/cloudarch/pkg/api"
	"github.com/vasenka88/cloudarch/pkg/storage"
)

// Config configures the shard service.
type Config struct {
	ShardID        string
	ListenAddr     string
	AdvertiseAddr  string
	CoordinatorURL string
	DataDir        string
	VirtualNodes   int
}

// Server is the HTTP server for a storage shard.
type Server struct {
	cfg       Config
	engine    *storage.Engine
	mux       *http.ServeMux
	server    *http.Server
	startTime time.Time
}

// NewServer creates a new shard HTTP server instance.
func NewServer(cfg Config) *Server {
	if cfg.VirtualNodes <= 0 {
		cfg.VirtualNodes = 200
	}
	if cfg.AdvertiseAddr == "" {
		cfg.AdvertiseAddr = cfg.ListenAddr
	}

	engine := storage.NewEngine(cfg.DataDir)
	s := &Server{
		cfg:       cfg,
		engine:    engine,
		mux:       http.NewServeMux(),
		startTime: time.Now(),
	}
	s.registerRoutes()
	return s
}

// Engine returns the underlying storage engine.
func (s *Server) Engine() *storage.Engine {
	return s.engine
}

func (s *Server) registerRoutes() {
	s.mux.HandleFunc("/healthz", s.handleHealthz)
	s.mux.HandleFunc("/api/v1/tables", s.handleTables)
	s.mux.HandleFunc("/api/v1/tables/", s.handleTableRoutes)
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	status := api.HealthStatus{
		Status:        "ok",
		NodeID:        s.cfg.ShardID,
		UptimeSeconds: int64(time.Since(s.startTime).Seconds()),
	}
	writeJSON(w, http.StatusOK, status)
}

func (s *Server) handleTables(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		var def api.TableDefinition
		if err := json.NewDecoder(r.Body).Decode(&def); err != nil {
			writeError(w, http.StatusBadRequest, "invalid request body", err.Error())
			return
		}
		if err := s.engine.CreateTable(def); err != nil {
			if err == storage.ErrTableAlreadyExists {
				writeError(w, http.StatusConflict, "table already exists", "")
				return
			}
			writeError(w, http.StatusBadRequest, err.Error(), "")
			return
		}
		writeJSON(w, http.StatusCreated, def)

	case http.MethodGet:
		tables := s.engine.ListTables()
		writeJSON(w, http.StatusOK, tables)

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleTableRoutes(w http.ResponseWriter, r *http.Request) {
	// Path pattern: /api/v1/tables/{table}/...
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/tables/")
	parts := strings.Split(path, "/")
	if len(parts) == 0 || parts[0] == "" {
		http.NotFound(w, r)
		return
	}

	tableName := parts[0]

	if len(parts) == 1 {
		// /api/v1/tables/{table}
		if r.Method == http.MethodGet {
			def, err := s.engine.GetTable(tableName)
			if err != nil {
				writeError(w, http.StatusNotFound, "table not found", "")
				return
			}
			writeJSON(w, http.StatusOK, def)
			return
		}
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	sub := parts[1]
	switch sub {
	case "query":
		s.handleQuery(w, r, tableName)
	case "records":
		s.handleRecords(w, r, tableName, parts[2:])
	default:
		http.NotFound(w, r)
	}
}

func (s *Server) handleRecords(w http.ResponseWriter, r *http.Request, table string, keyParts []string) {
	switch r.Method {
	case http.MethodPost, http.MethodPut:
		var rec api.Record
		if err := json.NewDecoder(r.Body).Decode(&rec); err != nil {
			writeError(w, http.StatusBadRequest, "invalid record payload", err.Error())
			return
		}

		if err := s.engine.PutRecord(table, rec); err != nil {
			if err == storage.ErrTableNotFound {
				writeError(w, http.StatusNotFound, "table not found", "")
				return
			}
			writeError(w, http.StatusBadRequest, err.Error(), "")
			return
		}

		resp := api.RecordResponse{
			Success:      true,
			ShardID:      s.cfg.ShardID,
			Table:        table,
			PartitionKey: rec.PartitionKey,
			SortKey:      rec.SortKey,
		}
		writeJSON(w, http.StatusOK, resp)

	case http.MethodGet, http.MethodHead:
		if len(keyParts) == 0 || keyParts[0] == "" {
			writeError(w, http.StatusBadRequest, "partitionKey is required", "")
			return
		}

		pk := keyParts[0]
		sk := r.URL.Query().Get("sortKey")
		if len(keyParts) > 1 && keyParts[1] != "" {
			sk = keyParts[1]
		}

		if r.Method == http.MethodHead {
			exists, err := s.engine.ExistsRecord(table, pk, sk)
			if err != nil || !exists {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.WriteHeader(http.StatusOK)
			return
		}

		rec, err := s.engine.GetRecord(table, pk, sk)
		if err != nil {
			if err == storage.ErrRecordNotFound {
				writeError(w, http.StatusNotFound, "record not found", "")
				return
			}
			if err == storage.ErrTableNotFound {
				writeError(w, http.StatusNotFound, "table not found", "")
				return
			}
			writeError(w, http.StatusInternalServerError, err.Error(), "")
			return
		}
		writeJSON(w, http.StatusOK, rec)

	case http.MethodDelete:
		if len(keyParts) == 0 || keyParts[0] == "" {
			writeError(w, http.StatusBadRequest, "partitionKey is required", "")
			return
		}

		pk := keyParts[0]
		sk := r.URL.Query().Get("sortKey")
		if len(keyParts) > 1 && keyParts[1] != "" {
			sk = keyParts[1]
		}

		if err := s.engine.DeleteRecord(table, pk, sk); err != nil {
			if err == storage.ErrTableNotFound {
				writeError(w, http.StatusNotFound, "table not found", "")
				return
			}
			writeError(w, http.StatusInternalServerError, err.Error(), "")
			return
		}
		w.WriteHeader(http.StatusNoContent)

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request, table string) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	pk := r.URL.Query().Get("partitionKey")
	if pk == "" {
		writeError(w, http.StatusBadRequest, "partitionKey query parameter is required", "")
		return
	}

	sortFrom := r.URL.Query().Get("sortKeyFrom")
	sortTo := r.URL.Query().Get("sortKeyTo")
	limit := 100
	if l := r.URL.Query().Get("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil && parsed > 0 {
			limit = parsed
		}
	}

	records, err := s.engine.QueryPartition(table, pk, sortFrom, sortTo, limit)
	if err != nil {
		if err == storage.ErrTableNotFound {
			writeError(w, http.StatusNotFound, "table not found", "")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error(), "")
		return
	}

	if records == nil {
		records = []api.Record{}
	}
	writeJSON(w, http.StatusOK, records)
}

// RegisterWithCoordinator attempts to register this shard with the coordinator.
func (s *Server) RegisterWithCoordinator(ctx context.Context) error {
	if s.cfg.CoordinatorURL == "" {
		return nil
	}

	regPayload := api.ShardRegistration{
		ShardID:      s.cfg.ShardID,
		Address:      s.cfg.AdvertiseAddr,
		VirtualNodes: s.cfg.VirtualNodes,
	}

	body, err := json.Marshal(regPayload)
	if err != nil {
		return err
	}

	reqURL := fmt.Sprintf("%s/internal/v1/shards", strings.TrimRight(s.cfg.CoordinatorURL, "/"))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL, bytes.NewBuffer(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("coordinator returned status %d", resp.StatusCode)
	}

	log.Printf("[shard:%s] successfully registered with coordinator at %s", s.cfg.ShardID, reqURL)
	return nil
}

// StartBootstrapLoop runs a background loop until successfully registered with coordinator.
func (s *Server) StartBootstrapLoop(ctx context.Context) {
	go func() {
		backoff := 1 * time.Second
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			if err := s.RegisterWithCoordinator(ctx); err == nil {
				return
			} else {
				log.Printf("[shard:%s] coordinator registration failed (%v), retrying in %v...", s.cfg.ShardID, err, backoff)
			}

			time.Sleep(backoff)
			if backoff < 10*time.Second {
				backoff *= 2
			}
		}
	}()
}

// Start begins serving HTTP traffic.
func (s *Server) Start() error {
	s.server = &http.Server{
		Addr:    s.cfg.ListenAddr,
		Handler: s.mux,
	}
	return s.server.ListenAndServe()
}

// Stop gracefully shuts down the server.
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

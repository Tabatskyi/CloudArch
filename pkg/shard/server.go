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
	server := &Server{
		cfg:       cfg,
		engine:    engine,
		mux:       http.NewServeMux(),
		startTime: time.Now(),
	}
	server.registerRoutes()
	return server
}

// Engine returns the underlying storage engine.
func (server *Server) Engine() *storage.Engine {
	return server.engine
}

func (server *Server) registerRoutes() {
	server.mux.HandleFunc("/healthz", server.handleHealthz)
	server.mux.HandleFunc("/api/v1/tables", server.handleTables)
	server.mux.HandleFunc("/api/v1/tables/", server.handleTableRoutes)
}

func (server *Server) handleHealthz(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	status := api.HealthStatus{
		Status:        "ok",
		NodeID:        server.cfg.ShardID,
		UptimeSeconds: int64(time.Since(server.startTime).Seconds()),
	}
	writeJSON(writer, http.StatusOK, status)
}

func (server *Server) handleTables(writer http.ResponseWriter, request *http.Request) {
	switch request.Method {
	case http.MethodPost:
		var def api.TableDefinition
		if err := json.NewDecoder(request.Body).Decode(&def); err != nil {
			writeError(writer, http.StatusBadRequest, "invalid request body", err.Error())
			return
		}
		if err := server.engine.CreateTable(def); err != nil {
			if err == storage.ErrTableAlreadyExists {
				writeError(writer, http.StatusConflict, "table already exists", "")
				return
			}
			writeError(writer, http.StatusBadRequest, err.Error(), "")
			return
		}
		writeJSON(writer, http.StatusCreated, def)

	case http.MethodGet:
		tables := server.engine.ListTables()
		writeJSON(writer, http.StatusOK, tables)

	default:
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (server *Server) handleTableRoutes(writer http.ResponseWriter, request *http.Request) {
	// Path pattern: /api/v1/tables/{table}/...
	path := strings.TrimPrefix(request.URL.Path, "/api/v1/tables/")
	parts := strings.Split(path, "/")
	if len(parts) == 0 || parts[0] == "" {
		http.NotFound(writer, request)
		return
	}

	tableName := parts[0]

	if len(parts) == 1 {
		// /api/v1/tables/{table}
		if request.Method == http.MethodGet {
			def, err := server.engine.GetTable(tableName)
			if err != nil {
				writeError(writer, http.StatusNotFound, "table not found", "")
				return
			}
			writeJSON(writer, http.StatusOK, def)
			return
		}
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	sub := parts[1]
	switch sub {
	case "query":
		server.handleQuery(writer, request, tableName)
	case "records":
		server.handleRecords(writer, request, tableName, parts[2:])
	default:
		http.NotFound(writer, request)
	}
}

func (server *Server) handleRecords(writer http.ResponseWriter, request *http.Request, table string, keyParts []string) {
	switch request.Method {
	case http.MethodPost, http.MethodPut:
		var rec api.Record
		if err := json.NewDecoder(request.Body).Decode(&rec); err != nil {
			writeError(writer, http.StatusBadRequest, "invalid record payload", err.Error())
			return
		}

		if err := server.engine.PutRecord(table, rec); err != nil {
			if err == storage.ErrTableNotFound {
				writeError(writer, http.StatusNotFound, "table not found", "")
				return
			}
			writeError(writer, http.StatusBadRequest, err.Error(), "")
			return
		}

		resp := api.RecordResponse{
			Success:      true,
			ShardID:      server.cfg.ShardID,
			Table:        table,
			PartitionKey: rec.PartitionKey,
			SortKey:      rec.SortKey,
		}
		writeJSON(writer, http.StatusOK, resp)

	case http.MethodGet, http.MethodHead:
		if len(keyParts) == 0 || keyParts[0] == "" {
			writeError(writer, http.StatusBadRequest, "partitionKey is required", "")
			return
		}

		partKey := keyParts[0]
		sortKey := request.URL.Query().Get("sortKey")
		if len(keyParts) > 1 && keyParts[1] != "" {
			sortKey = keyParts[1]
		}

		if request.Method == http.MethodHead {
			exists, err := server.engine.ExistsRecord(table, partKey, sortKey)
			if err != nil || !exists {
				writer.WriteHeader(http.StatusNotFound)
				return
			}
			writer.WriteHeader(http.StatusOK)
			return
		}

		rec, err := server.engine.GetRecord(table, partKey, sortKey)
		if err != nil {
			if err == storage.ErrRecordNotFound {
				writeError(writer, http.StatusNotFound, "record not found", "")
				return
			}
			if err == storage.ErrTableNotFound {
				writeError(writer, http.StatusNotFound, "table not found", "")
				return
			}
			writeError(writer, http.StatusInternalServerError, err.Error(), "")
			return
		}
		writeJSON(writer, http.StatusOK, rec)

	case http.MethodDelete:
		if len(keyParts) == 0 || keyParts[0] == "" {
			writeError(writer, http.StatusBadRequest, "partitionKey is required", "")
			return
		}

		partKey := keyParts[0]
		sortKey := request.URL.Query().Get("sortKey")
		if len(keyParts) > 1 && keyParts[1] != "" {
			sortKey = keyParts[1]
		}

		if err := server.engine.DeleteRecord(table, partKey, sortKey); err != nil {
			if err == storage.ErrTableNotFound {
				writeError(writer, http.StatusNotFound, "table not found", "")
				return
			}
			writeError(writer, http.StatusInternalServerError, err.Error(), "")
			return
		}
		writer.WriteHeader(http.StatusNoContent)

	default:
		writeError(writer, http.StatusMethodNotAllowed, "method not allowed", "")
	}
}

func (server *Server) handleQuery(writer http.ResponseWriter, request *http.Request, table string) {
	if request.Method != http.MethodGet {
		writeError(writer, http.StatusMethodNotAllowed, "method not allowed", "")
		return
	}

	partKey := request.URL.Query().Get("partitionKey")
	if partKey == "" {
		writeError(writer, http.StatusBadRequest, "partitionKey query parameter is required", "")
		return
	}

	sortFrom := request.URL.Query().Get("sortKeyFrom")
	sortTo := request.URL.Query().Get("sortKeyTo")
	limit := 100
	if l := request.URL.Query().Get("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil && parsed > 0 {
			limit = parsed
		}
	}

	records, err := server.engine.QueryPartition(table, partKey, sortFrom, sortTo, limit)
	if err != nil {
		if err == storage.ErrTableNotFound {
			writeError(writer, http.StatusNotFound, "table not found", "")
			return
		}
		writeError(writer, http.StatusInternalServerError, err.Error(), "")
		return
	}

	if records == nil {
		records = []api.Record{}
	}
	writeJSON(writer, http.StatusOK, records)
}

// RegisterWithCoordinator attempts to register this shard with the coordinator.
func (server *Server) RegisterWithCoordinator(ctx context.Context) error {
	if server.cfg.CoordinatorURL == "" {
		return nil
	}

	regPayload := api.ShardRegistration{
		ShardID:      server.cfg.ShardID,
		Address:      server.cfg.AdvertiseAddr,
		VirtualNodes: server.cfg.VirtualNodes,
	}

	body, err := json.Marshal(regPayload)
	if err != nil {
		return err
	}

	reqURL := fmt.Sprintf("%s/internal/v1/shards", strings.TrimRight(server.cfg.CoordinatorURL, "/"))
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

	log.Printf("[shard:%s] successfully registered with coordinator at %s", server.cfg.ShardID, reqURL)
	return nil
}

// StartBootstrapLoop runs a background loop until successfully registered with coordinator.
func (server *Server) StartBootstrapLoop(ctx context.Context) {
	go func() {
		backoff := 1 * time.Second
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			if err := server.RegisterWithCoordinator(ctx); err == nil {
				return
			} else {
				log.Printf("[shard:%s] coordinator registration failed (%v), retrying in %v...", server.cfg.ShardID, err, backoff)
			}

			time.Sleep(backoff)
			if backoff < 10*time.Second {
				backoff *= 2
			}
		}
	}()
}

// Start begins serving HTTP traffic.
func (server *Server) Start() error {
	server.server = &http.Server{
		Addr:    server.cfg.ListenAddr,
		Handler: server.mux,
	}
	return server.server.ListenAndServe()
}

// Stop gracefully shuts down the server.
func (server *Server) Stop(ctx context.Context) error {
	if server.server != nil {
		return server.server.Shutdown(ctx)
	}
	return nil
}

// ServeHTTP implements http.Handler for testing.
func (server *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	server.mux.ServeHTTP(w, r)
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

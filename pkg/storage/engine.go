package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/vasenka88/cloudarch/pkg/api"
)

var (
	ErrTableNotFound      = errors.New("table not found")
	ErrTableAlreadyExists = errors.New("table already exists")
	ErrRecordNotFound     = errors.New("record not found")
	ErrMissingPartitionKey = errors.New("record is missing partition key")
	ErrMissingSortKey     = errors.New("table requires a sort key but record sort key is missing")
)

// TableStore maintains the schema and partition collections for a single table.
type TableStore struct {
	mu         sync.RWMutex
	Definition api.TableDefinition
	Partitions map[string]*OrderedPartition
}

func newTableStore(def api.TableDefinition) *TableStore {
	return &TableStore{
		Definition: def,
		Partitions: make(map[string]*OrderedPartition),
	}
}

func (ts *TableStore) getOrCreatePartition(pk string) *OrderedPartition {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	p, exists := ts.Partitions[pk]
	if !exists {
		p = NewOrderedPartition()
		ts.Partitions[pk] = p
	}
	return p
}

func (ts *TableStore) getPartition(pk string) (*OrderedPartition, bool) {
	ts.mu.RLock()
	defer ts.mu.RUnlock()

	p, exists := ts.Partitions[pk]
	return p, exists
}

// Engine is the central local storage engine managing multiple tables and their partitions.
type Engine struct {
	mu      sync.RWMutex
	dataDir string
	tables  map[string]*TableStore
}

// NewEngine creates an in-memory storage engine with an optional data directory for persistence.
func NewEngine(dataDir string) *Engine {
	if dataDir != "" {
		_ = os.MkdirAll(dataDir, 0755)
	}
	return &Engine{
		dataDir: dataDir,
		tables:  make(map[string]*TableStore),
	}
}

// CreateTable registers a new table schema.
func (e *Engine) CreateTable(def api.TableDefinition) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if def.TableName == "" || def.PartitionKey == "" {
		return errors.New("tableName and partitionKey are required")
	}

	if _, exists := e.tables[def.TableName]; exists {
		return ErrTableAlreadyExists
	}

	e.tables[def.TableName] = newTableStore(def)
	return nil
}

// GetTable retrieves a table's schema.
func (e *Engine) GetTable(name string) (api.TableDefinition, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	ts, exists := e.tables[name]
	if !exists {
		return api.TableDefinition{}, ErrTableNotFound
	}
	return ts.Definition, nil
}

// ListTables returns all registered table definitions.
func (e *Engine) ListTables() []api.TableDefinition {
	e.mu.RLock()
	defer e.mu.RUnlock()

	defs := make([]api.TableDefinition, 0, len(e.tables))
	for _, ts := range e.tables {
		defs = append(defs, ts.Definition)
	}
	return defs
}

// PutRecord inserts or updates a record into the table.
func (e *Engine) PutRecord(table string, rec api.Record) error {
	e.mu.RLock()
	ts, exists := e.tables[table]
	e.mu.RUnlock()

	if !exists {
		return ErrTableNotFound
	}

	if rec.PartitionKey == "" {
		return ErrMissingPartitionKey
	}
	if ts.Definition.SortKey != "" && rec.SortKey == "" {
		return ErrMissingSortKey
	}

	partition := ts.getOrCreatePartition(rec.PartitionKey)
	partition.Put(rec)
	return nil
}

// GetRecord reads a record by partitionKey and sortKey.
func (e *Engine) GetRecord(table, partitionKey, sortKey string) (api.Record, error) {
	e.mu.RLock()
	ts, exists := e.tables[table]
	e.mu.RUnlock()

	if !exists {
		return api.Record{}, ErrTableNotFound
	}

	partition, found := ts.getPartition(partitionKey)
	if !found {
		return api.Record{}, ErrRecordNotFound
	}

	rec, found := partition.Get(sortKey)
	if !found {
		return api.Record{}, ErrRecordNotFound
	}
	return rec, nil
}

// ExistsRecord checks if a record exists in the table.
func (e *Engine) ExistsRecord(table, partitionKey, sortKey string) (bool, error) {
	e.mu.RLock()
	ts, exists := e.tables[table]
	e.mu.RUnlock()

	if !exists {
		return false, ErrTableNotFound
	}

	partition, found := ts.getPartition(partitionKey)
	if !found {
		return false, nil
	}

	return partition.Exists(sortKey), nil
}

// DeleteRecord deletes a record from the table.
func (e *Engine) DeleteRecord(table, partitionKey, sortKey string) error {
	e.mu.RLock()
	ts, exists := e.tables[table]
	e.mu.RUnlock()

	if !exists {
		return ErrTableNotFound
	}

	partition, found := ts.getPartition(partitionKey)
	if !found {
		return nil // idempotent deletion
	}

	partition.Delete(sortKey)
	return nil
}

// QueryPartition executes an intra-partition range scan.
func (e *Engine) QueryPartition(table, partitionKey, sortKeyFrom, sortKeyTo string, limit int) ([]api.Record, error) {
	e.mu.RLock()
	ts, exists := e.tables[table]
	e.mu.RUnlock()

	if !exists {
		return nil, ErrTableNotFound
	}

	partition, found := ts.getPartition(partitionKey)
	if !found {
		return []api.Record{}, nil
	}

	return partition.Range(sortKeyFrom, sortKeyTo, limit), nil
}

// TotalRecords returns the count of records across all tables.
func (e *Engine) TotalRecords() int {
	e.mu.RLock()
	defer e.mu.RUnlock()

	total := 0
	for _, ts := range e.tables {
		ts.mu.RLock()
		for _, p := range ts.Partitions {
			total += p.Count()
		}
		ts.mu.RUnlock()
	}
	return total
}

type snapshotData struct {
	Tables  []api.TableDefinition `json:"tables"`
	Records map[string][]api.Record `json:"records"`
}

// Snapshot writes the in-memory state to disk in JSON format.
func (e *Engine) Snapshot(filename string) error {
	if e.dataDir == "" {
		return nil
	}
	e.mu.RLock()
	defer e.mu.RUnlock()

	data := snapshotData{
		Tables:  make([]api.TableDefinition, 0, len(e.tables)),
		Records: make(map[string][]api.Record),
	}

	for tableName, ts := range e.tables {
		data.Tables = append(data.Tables, ts.Definition)
		ts.mu.RLock()
		for _, p := range ts.Partitions {
			data.Records[tableName] = append(data.Records[tableName], p.Range("", "", 1000000)...)
		}
		ts.mu.RUnlock()
	}

	raw, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return fmt.Errorf("snapshot marshal error: %w", err)
	}

	target := filepath.Join(e.dataDir, filename)
	return os.WriteFile(target, raw, 0644)
}

// Restore loads an engine snapshot from disk.
func (e *Engine) Restore(filename string) error {
	if e.dataDir == "" {
		return nil
	}
	target := filepath.Join(e.dataDir, filename)
	raw, err := os.ReadFile(target)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	var data snapshotData
	if err := json.Unmarshal(raw, &data); err != nil {
		return fmt.Errorf("restore unmarshal error: %w", err)
	}

	for _, tbl := range data.Tables {
		_ = e.CreateTable(tbl)
	}
	for tbl, recs := range data.Records {
		for _, r := range recs {
			_ = e.PutRecord(tbl, r)
		}
	}
	return nil
}

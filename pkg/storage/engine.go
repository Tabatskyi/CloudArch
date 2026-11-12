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
	ErrTableNotFound       = errors.New("table not found")
	ErrTableAlreadyExists  = errors.New("table already exists")
	ErrRecordNotFound      = errors.New("record not found")
	ErrMissingPartitionKey = errors.New("record is missing partition key")
	ErrMissingSortKey      = errors.New("table requires a sort key but record sort key is missing")
)

// TableStore maintains the schema and partition collections for a single table.
type TableStore struct {
	mutex      sync.RWMutex
	Definition api.TableDefinition
	Partitions map[string]*OrderedPartition
}

func newTableStore(def api.TableDefinition) *TableStore {
	return &TableStore{
		Definition: def,
		Partitions: make(map[string]*OrderedPartition),
	}
}

func (tableStore *TableStore) getOrCreatePartition(partitionKey string) *OrderedPartition {
	tableStore.mutex.Lock()
	defer tableStore.mutex.Unlock()

	partition, exists := tableStore.Partitions[partitionKey]
	if !exists {
		partition = NewOrderedPartition()
		tableStore.Partitions[partitionKey] = partition
	}
	return partition
}

func (tableStore *TableStore) getPartition(partitionKey string) (*OrderedPartition, bool) {
	tableStore.mutex.RLock()
	defer tableStore.mutex.RUnlock()

	p, exists := tableStore.Partitions[partitionKey]
	return p, exists
}

// Engine is the central local storage engine managing multiple tables and their partitions.
type Engine struct {
	mutex   sync.RWMutex
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
func (engine *Engine) CreateTable(def api.TableDefinition) error {
	engine.mutex.Lock()
	defer engine.mutex.Unlock()

	if def.TableName == "" || def.PartitionKey == "" {
		return errors.New("tableName and partitionKey are required")
	}

	if _, exists := engine.tables[def.TableName]; exists {
		return ErrTableAlreadyExists
	}

	engine.tables[def.TableName] = newTableStore(def)
	return nil
}

// GetTable retrieves a table's schema.
func (engine *Engine) GetTable(name string) (api.TableDefinition, error) {
	engine.mutex.RLock()
	defer engine.mutex.RUnlock()

	tableStore, exists := engine.tables[name]
	if !exists {
		return api.TableDefinition{}, ErrTableNotFound
	}
	return tableStore.Definition, nil
}

// ListTables returns all registered table definitions.
func (engine *Engine) ListTables() []api.TableDefinition {
	engine.mutex.RLock()
	defer engine.mutex.RUnlock()

	defs := make([]api.TableDefinition, 0, len(engine.tables))
	for _, tableStore := range engine.tables {
		defs = append(defs, tableStore.Definition)
	}
	return defs
}

// PutRecord inserts or updates a record into the table.
func (engine *Engine) PutRecord(table string, rec api.Record) error {
	engine.mutex.RLock()
	tableStore, exists := engine.tables[table]
	engine.mutex.RUnlock()

	if !exists {
		return ErrTableNotFound
	}

	if rec.PartitionKey == "" {
		return ErrMissingPartitionKey
	}
	if tableStore.Definition.SortKey != "" && rec.SortKey == "" {
		return ErrMissingSortKey
	}

	partition := tableStore.getOrCreatePartition(rec.PartitionKey)
	partition.Put(rec)
	return nil
}

// GetRecord reads a record by partitionKey and sortKey.
func (engine *Engine) GetRecord(table, partitionKey, sortKey string) (api.Record, error) {
	engine.mutex.RLock()
	tableStore, exists := engine.tables[table]
	engine.mutex.RUnlock()

	if !exists {
		return api.Record{}, ErrTableNotFound
	}

	partition, found := tableStore.getPartition(partitionKey)
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
func (engine *Engine) ExistsRecord(table, partitionKey, sortKey string) (bool, error) {
	engine.mutex.RLock()
	tableStore, exists := engine.tables[table]
	engine.mutex.RUnlock()

	if !exists {
		return false, ErrTableNotFound
	}

	partition, found := tableStore.getPartition(partitionKey)
	if !found {
		return false, nil
	}

	return partition.Exists(sortKey), nil
}

// DeleteRecord deletes a record from the table.
func (engine *Engine) DeleteRecord(table, partitionKey, sortKey string) error {
	engine.mutex.RLock()
	tableStore, exists := engine.tables[table]
	engine.mutex.RUnlock()

	if !exists {
		return ErrTableNotFound
	}

	partition, found := tableStore.getPartition(partitionKey)
	if !found {
		return nil // idempotent deletion
	}

	partition.Delete(sortKey)
	return nil
}

// QueryPartition executes an intra-partition range scan.
func (engine *Engine) QueryPartition(table, partitionKey, sortKeyFrom, sortKeyTo string, limit int) ([]api.Record, error) {
	engine.mutex.RLock()
	tableStore, exists := engine.tables[table]
	engine.mutex.RUnlock()

	if !exists {
		return nil, ErrTableNotFound
	}

	partition, found := tableStore.getPartition(partitionKey)
	if !found {
		return []api.Record{}, nil
	}

	return partition.Range(sortKeyFrom, sortKeyTo, limit), nil
}

// TotalRecords returns the count of records across all tables.
func (engine *Engine) TotalRecords() int {
	engine.mutex.RLock()
	defer engine.mutex.RUnlock()

	total := 0
	for _, tableStore := range engine.tables {
		tableStore.mutex.RLock()
		for _, partition := range tableStore.Partitions {
			total += partition.Count()
		}
		tableStore.mutex.RUnlock()
	}
	return total
}

type snapshotData struct {
	Tables  []api.TableDefinition   `json:"tables"`
	Records map[string][]api.Record `json:"records"`
}

// Snapshot writes the in-memory state to disk in JSON format.
func (engine *Engine) Snapshot(filename string) error {
	if engine.dataDir == "" {
		return nil
	}
	engine.mutex.RLock()
	defer engine.mutex.RUnlock()

	data := snapshotData{
		Tables:  make([]api.TableDefinition, 0, len(engine.tables)),
		Records: make(map[string][]api.Record),
	}

	for tableName, tableStore := range engine.tables {
		data.Tables = append(data.Tables, tableStore.Definition)
		tableStore.mutex.RLock()
		for _, partition := range tableStore.Partitions {
			data.Records[tableName] = append(data.Records[tableName], partition.Range("", "", 1000000)...)
		}
		tableStore.mutex.RUnlock()
	}

	raw, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return fmt.Errorf("snapshot marshal error: %w", err)
	}

	target := filepath.Join(engine.dataDir, filename)
	return os.WriteFile(target, raw, 0644)
}

// Restore loads an engine snapshot from disk.
func (engine *Engine) Restore(filename string) error {
	if engine.dataDir == "" {
		return nil
	}
	target := filepath.Join(engine.dataDir, filename)
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

	for _, table := range data.Tables {
		_ = engine.CreateTable(table)
	}
	for tableName, recs := range data.Records {
		for _, r := range recs {
			_ = engine.PutRecord(tableName, r)
		}
	}
	return nil
}

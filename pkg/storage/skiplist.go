package storage

import (
	"math/rand"
	"strings"
	"sync"

	"github.com/vasenka88/cloudarch/pkg/api"
)

const (
	maxLevel    = 16
	probability = 0.5
)

type skipNode struct {
	sortKey string
	record  api.Record
	forward []*skipNode
}

// OrderedPartition represents an ordered collection of records for a single partition key.
type OrderedPartition struct {
	mu     sync.RWMutex
	header *skipNode
	level  int
	count  int
}

// NewOrderedPartition creates a new ordered partition.
func NewOrderedPartition() *OrderedPartition {
	header := &skipNode{
		forward: make([]*skipNode, maxLevel),
	}
	return &OrderedPartition{
		header: header,
		level:  1,
	}
}

func (op *OrderedPartition) randomLevel() int {
	lvl := 1
	for rand.Float64() < probability && lvl < maxLevel {
		lvl++
	}
	return lvl
}

// Put inserts or updates a record identified by its sortKey.
func (op *OrderedPartition) Put(rec api.Record) {
	op.mu.Lock()
	defer op.mu.Unlock()

	update := make([]*skipNode, maxLevel)
	current := op.header

	for i := op.level - 1; i >= 0; i-- {
		for current.forward[i] != nil && current.forward[i].sortKey < rec.SortKey {
			current = current.forward[i]
		}
		update[i] = current
	}

	current = current.forward[0]
	if current != nil && current.sortKey == rec.SortKey {
		// Overwrite existing record
		current.record = rec
		return
	}

	// Insert new node
	lvl := op.randomLevel()
	if lvl > op.level {
		for i := op.level; i < lvl; i++ {
			update[i] = op.header
		}
		op.level = lvl
	}

	newNode := &skipNode{
		sortKey: rec.SortKey,
		record:  rec,
		forward: make([]*skipNode, lvl),
	}

	for i := 0; i < lvl; i++ {
		newNode.forward[i] = update[i].forward[i]
		update[i].forward[i] = newNode
	}
	op.count++
}

// Get retrieves a record by its sortKey.
func (op *OrderedPartition) Get(sortKey string) (api.Record, bool) {
	op.mu.RLock()
	defer op.mu.RUnlock()

	current := op.header
	for i := op.level - 1; i >= 0; i-- {
		for current.forward[i] != nil && current.forward[i].sortKey < sortKey {
			current = current.forward[i]
		}
	}

	current = current.forward[0]
	if current != nil && current.sortKey == sortKey {
		return current.record, true
	}
	return api.Record{}, false
}

// Exists checks if a record with the sortKey exists.
func (op *OrderedPartition) Exists(sortKey string) bool {
	_, found := op.Get(sortKey)
	return found
}

// Delete removes the record with the specified sortKey.
func (op *OrderedPartition) Delete(sortKey string) bool {
	op.mu.Lock()
	defer op.mu.Unlock()

	update := make([]*skipNode, maxLevel)
	current := op.header

	for i := op.level - 1; i >= 0; i-- {
		for current.forward[i] != nil && current.forward[i].sortKey < sortKey {
			current = current.forward[i]
		}
		update[i] = current
	}

	current = current.forward[0]
	if current == nil || current.sortKey != sortKey {
		return false
	}

	for i := 0; i < op.level; i++ {
		if update[i].forward[i] != current {
			break
		}
		update[i].forward[i] = current.forward[i]
	}

	for op.level > 1 && op.header.forward[op.level-1] == nil {
		op.level--
	}

	op.count--
	return true
}

// Range returns records matching optional sort key range boundaries [from, to] up to limit.
func (op *OrderedPartition) Range(from, to string, limit int) []api.Record {
	op.mu.RLock()
	defer op.mu.RUnlock()

	if limit <= 0 {
		limit = 100
	}

	var results []api.Record
	current := op.header

	if from != "" {
		for i := op.level - 1; i >= 0; i-- {
			for current.forward[i] != nil && current.forward[i].sortKey < from {
				current = current.forward[i]
			}
		}
		current = current.forward[0]
	} else {
		current = current.forward[0]
	}

	for current != nil && len(results) < limit {
		if to != "" && current.sortKey > to {
			break
		}
		results = append(results, current.record)
		current = current.forward[0]
	}

	return results
}

// Prefix returns records whose sortKey begins with prefix.
func (op *OrderedPartition) Prefix(prefix string, limit int) []api.Record {
	op.mu.RLock()
	defer op.mu.RUnlock()

	if limit <= 0 {
		limit = 100
	}

	var results []api.Record
	current := op.header

	for i := op.level - 1; i >= 0; i-- {
		for current.forward[i] != nil && current.forward[i].sortKey < prefix {
			current = current.forward[i]
		}
	}
	current = current.forward[0]

	for current != nil && len(results) < limit {
		if !strings.HasPrefix(current.sortKey, prefix) {
			break
		}
		results = append(results, current.record)
		current = current.forward[0]
	}

	return results
}

// Count returns the number of records stored in this partition.
func (op *OrderedPartition) Count() int {
	op.mu.RLock()
	defer op.mu.RUnlock()
	return op.count
}

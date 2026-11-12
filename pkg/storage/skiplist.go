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
	mutex  sync.RWMutex
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

func (orderedPartition *OrderedPartition) randomLevel() int {
	lvl := 1
	for rand.Float64() < probability && lvl < maxLevel {
		lvl++
	}
	return lvl
}

// Put inserts or updates a record identified by its sortKey.
func (orderedPartition *OrderedPartition) Put(rec api.Record) {
	orderedPartition.mutex.Lock()
	defer orderedPartition.mutex.Unlock()

	update := make([]*skipNode, maxLevel)
	current := orderedPartition.header

	for i := orderedPartition.level - 1; i >= 0; i-- {
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
	lvl := orderedPartition.randomLevel()
	if lvl > orderedPartition.level {
		for i := orderedPartition.level; i < lvl; i++ {
			update[i] = orderedPartition.header
		}
		orderedPartition.level = lvl
	}

	newNode := &skipNode{
		sortKey: rec.SortKey,
		record:  rec,
		forward: make([]*skipNode, lvl),
	}

	for i := range lvl {
		newNode.forward[i] = update[i].forward[i]
		update[i].forward[i] = newNode
	}
	orderedPartition.count++
}

// Get retrieves a record by its sortKey.
func (orderedPartition *OrderedPartition) Get(sortKey string) (api.Record, bool) {
	orderedPartition.mutex.RLock()
	defer orderedPartition.mutex.RUnlock()

	current := orderedPartition.header
	for i := orderedPartition.level - 1; i >= 0; i-- {
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
func (orderedPartition *OrderedPartition) Exists(sortKey string) bool {
	_, found := orderedPartition.Get(sortKey)
	return found
}

// Delete removes the record with the specified sortKey.
func (orderedPartition *OrderedPartition) Delete(sortKey string) bool {
	orderedPartition.mutex.Lock()
	defer orderedPartition.mutex.Unlock()

	update := make([]*skipNode, maxLevel)
	current := orderedPartition.header

	for i := orderedPartition.level - 1; i >= 0; i-- {
		for current.forward[i] != nil && current.forward[i].sortKey < sortKey {
			current = current.forward[i]
		}
		update[i] = current
	}

	current = current.forward[0]
	if current == nil || current.sortKey != sortKey {
		return false
	}

	for i := 0; i < orderedPartition.level; i++ {
		if update[i].forward[i] != current {
			break
		}
		update[i].forward[i] = current.forward[i]
	}

	for orderedPartition.level > 1 && orderedPartition.header.forward[orderedPartition.level-1] == nil {
		orderedPartition.level--
	}

	orderedPartition.count--
	return true
}

// Range returns records matching optional sort key range boundaries [from, to] up to limit.
func (orderedPartition *OrderedPartition) Range(from, to string, limit int) []api.Record {
	orderedPartition.mutex.RLock()
	defer orderedPartition.mutex.RUnlock()

	if limit <= 0 {
		limit = 100
	}

	var results []api.Record
	current := orderedPartition.header

	if from != "" {
		for i := orderedPartition.level - 1; i >= 0; i-- {
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
func (orderedPartition *OrderedPartition) Prefix(prefix string, limit int) []api.Record {
	orderedPartition.mutex.RLock()
	defer orderedPartition.mutex.RUnlock()

	if limit <= 0 {
		limit = 100
	}

	var results []api.Record
	current := orderedPartition.header

	for i := orderedPartition.level - 1; i >= 0; i-- {
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
func (orderedPartition *OrderedPartition) Count() int {
	orderedPartition.mutex.RLock()
	defer orderedPartition.mutex.RUnlock()
	return orderedPartition.count
}

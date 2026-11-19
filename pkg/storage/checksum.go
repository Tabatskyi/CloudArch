package storage

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
)

// CalculatePartitionHash computes a deterministic SHA-256 checksum across all active records
// in an OrderedPartition, ordered by sortKey.
func CalculatePartitionHash(partition *OrderedPartition) string {
	if partition == nil {
		return ""
	}

	partition.mutex.RLock()
	defer partition.mutex.RUnlock()

	hasher := sha256.New()
	current := partition.header.forward[0]

	for current != nil {
		rec := current.record
		if !rec.IsTombstone {
			valBytes, _ := json.Marshal(rec.Value)
			entryStr := fmt.Sprintf("%s:%s:%d:%d:%s;", rec.PartitionKey, rec.SortKey, rec.Version, rec.Timestamp, string(valBytes))
			hasher.Write([]byte(entryStr))
		}
		current = current.forward[0]
	}

	return hex.EncodeToString(hasher.Sum(nil))
}

// CalculateTableHash computes a deterministic composite hash for a TableStore across all its partitions.
func CalculateTableHash(tableStore *TableStore) string {
	if tableStore == nil {
		return ""
	}

	tableStore.mutex.RLock()
	partitionKeys := make([]string, 0, len(tableStore.Partitions))
	for pk := range tableStore.Partitions {
		partitionKeys = append(partitionKeys, pk)
	}
	tableStore.mutex.RUnlock()

	sort.Strings(partitionKeys)

	hasher := sha256.New()
	for _, pk := range partitionKeys {
		p, exists := tableStore.getPartition(pk)
		if exists {
			pHash := CalculatePartitionHash(p)
			hasher.Write([]byte(fmt.Sprintf("%s=%s;", pk, pHash)))
		}
	}

	return hex.EncodeToString(hasher.Sum(nil))
}

// CalculateEngineStateHash computes a deterministic state hash across all tables in an Engine.
func CalculateEngineStateHash(engine *Engine) string {
	if engine == nil {
		return ""
	}

	engine.mutex.RLock()
	tableNames := make([]string, 0, len(engine.tables))
	for name := range engine.tables {
		tableNames = append(tableNames, name)
	}
	engine.mutex.RUnlock()

	sort.Strings(tableNames)

	hasher := sha256.New()
	for _, name := range tableNames {
		engine.mutex.RLock()
		ts := engine.tables[name]
		engine.mutex.RUnlock()

		if ts != nil {
			tHash := CalculateTableHash(ts)
			hasher.Write([]byte(fmt.Sprintf("%s=%s;", name, tHash)))
		}
	}

	return hex.EncodeToString(hasher.Sum(nil))
}

// VerifyStateSync compares leader and follower state hashes at a specific log offset.
func VerifyStateSync(leaderOffset, followerOffset uint64, leaderHash, followerHash string) (bool, string) {
	if leaderOffset != followerOffset {
		return false, fmt.Sprintf("offset mismatch: leader=%d, follower=%d", leaderOffset, followerOffset)
	}
	if leaderHash != followerHash {
		return false, fmt.Sprintf("checksum mismatch at offset %d: leader=%s, follower=%s", leaderOffset, leaderHash, followerHash)
	}
	return true, ""
}

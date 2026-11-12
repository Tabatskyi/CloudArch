package ring

import (
	"crypto/md5"
	"encoding/binary"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sort"
	"sync"
)

var (
	// ErrEmptyRing is returned when attempting to locate a node in an unpopulated ring.
	ErrEmptyRing = errors.New("consistent hash ring is empty: no shards registered")
	// ErrNodeNotFound is returned when a node being removed does not exist.
	ErrNodeNotFound = errors.New("node not found in ring")
)

// HashFunc defines the function signature for hashing byte slices to uint32.
type HashFunc func([]byte) uint32

// MD5Hash returns a 32-bit integer from the first 4 bytes of MD5 checksum.
func MD5Hash(b []byte) uint32 {
	hash := md5.Sum(b)
	return binary.BigEndian.Uint32(hash[:4])
}

// Ring manages consistent hashing of keys across physical shards using virtual nodes.
type Ring struct {
	mutex        sync.RWMutex
	virtualNodes int
	hashFunc     HashFunc
	ring         []uint32          // sorted slice of token hashes
	vnodes       map[uint32]string // token hash -> shardId
	nodeAddress  map[string]string // shardId -> address
}

// NewRing constructs a consistent hash ring with the specified virtual node count.
func NewRing(virtualNodes int, hashFunc HashFunc) *Ring {
	if virtualNodes <= 0 {
		virtualNodes = 200
	}
	if hashFunc == nil {
		hashFunc = MD5Hash
	}
	return &Ring{
		virtualNodes: virtualNodes,
		hashFunc:     hashFunc,
		ring:         make([]uint32, 0),
		vnodes:       make(map[uint32]string),
		nodeAddress:  make(map[string]string),
	}
}

// AddNode registers a physical shard node with its virtual tokens in the ring.
func (ring *Ring) AddNode(shardID, address string) {
	ring.mutex.Lock()
	defer ring.mutex.Unlock()

	// If node already registered with same or updated address, remove old tokens first
	if _, exists := ring.nodeAddress[shardID]; exists {
		ring.removeNodeLocked(shardID)
	}

	ring.nodeAddress[shardID] = address

	for i := 0; i < ring.virtualNodes; i++ {
		tokenID := fmt.Sprintf("%s#vn%d", shardID, i)
		hash := ring.hashFunc([]byte(tokenID))
		ring.ring = append(ring.ring, hash)
		ring.vnodes[hash] = shardID
	}

	slices.Sort(ring.ring)
}

// RemoveNode removes a physical shard node and its virtual tokens from the ring.
func (ring *Ring) RemoveNode(shardID string) error {
	ring.mutex.Lock()
	defer ring.mutex.Unlock()

	if _, exists := ring.nodeAddress[shardID]; !exists {
		return ErrNodeNotFound
	}
	ring.removeNodeLocked(shardID)
	return nil
}

func (ring *Ring) removeNodeLocked(shardID string) {
	delete(ring.nodeAddress, shardID)

	var newRing []uint32
	for _, h := range ring.ring {
		if ring.vnodes[h] == shardID {
			delete(ring.vnodes, h)
		} else {
			newRing = append(newRing, h)
		}
	}
	ring.ring = newRing
}

// GetNode resolves the primary shard ID and address for a partition key.
func (ring *Ring) GetNode(key string) (shardID string, address string, err error) {
	ring.mutex.RLock()
	defer ring.mutex.RUnlock()

	if len(ring.ring) == 0 {
		return "", "", ErrEmptyRing
	}

	hash := ring.hashFunc([]byte(key))
	idx := sort.Search(len(ring.ring), func(i int) bool {
		return ring.ring[i] >= hash
	})

	// Wrap around to the start of the circular ring
	if idx >= len(ring.ring) {
		idx = 0
	}

	shardID = ring.vnodes[ring.ring[idx]]
	address = ring.nodeAddress[shardID]
	return shardID, address, nil
}

// GetMembers returns a copy of all registered shard IDs and addresses.
func (ring *Ring) GetMembers() map[string]string {
	ring.mutex.RLock()
	defer ring.mutex.RUnlock()

	members := make(map[string]string, len(ring.nodeAddress))
	maps.Copy(members, ring.nodeAddress)
	return members
}

// MemberCount returns the number of active physical shards.
func (ring *Ring) MemberCount() int {
	ring.mutex.RLock()
	defer ring.mutex.RUnlock()
	return len(ring.nodeAddress)
}

package ring

import (
	"crypto/md5"
	"encoding/binary"
	"errors"
	"fmt"
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
	h := md5.Sum(b)
	return binary.BigEndian.Uint32(h[:4])
}

// Ring manages consistent hashing of keys across physical shards using virtual nodes.
type Ring struct {
	mu           sync.RWMutex
	virtualNodes int
	hashFunc     HashFunc
	ring         []uint32          // sorted slice of token hashes
	vnodes       map[uint32]string // token hash -> shardId
	nodeAddress  map[string]string // shardId -> address
}

// NewRing constructs a consistent hash ring with the specified virtual node count.
func NewRing(virtualNodes int, hf HashFunc) *Ring {
	if virtualNodes <= 0 {
		virtualNodes = 200
	}
	if hf == nil {
		hf = MD5Hash
	}
	return &Ring{
		virtualNodes: virtualNodes,
		hashFunc:     hf,
		ring:         make([]uint32, 0),
		vnodes:       make(map[uint32]string),
		nodeAddress:  make(map[string]string),
	}
}

// AddNode registers a physical shard node with its virtual tokens in the ring.
func (r *Ring) AddNode(shardID, address string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// If node already registered with same or updated address, remove old tokens first
	if _, exists := r.nodeAddress[shardID]; exists {
		r.removeNodeLocked(shardID)
	}

	r.nodeAddress[shardID] = address

	for i := 0; i < r.virtualNodes; i++ {
		tokenID := fmt.Sprintf("%s#vn%d", shardID, i)
		h := r.hashFunc([]byte(tokenID))
		r.ring = append(r.ring, h)
		r.vnodes[h] = shardID
	}

	sort.Slice(r.ring, func(i, j int) bool {
		return r.ring[i] < r.ring[j]
	})
}

// RemoveNode removes a physical shard node and its virtual tokens from the ring.
func (r *Ring) RemoveNode(shardID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.nodeAddress[shardID]; !exists {
		return ErrNodeNotFound
	}
	r.removeNodeLocked(shardID)
	return nil
}

func (r *Ring) removeNodeLocked(shardID string) {
	delete(r.nodeAddress, shardID)

	var newRing []uint32
	for _, h := range r.ring {
		if r.vnodes[h] == shardID {
			delete(r.vnodes, h)
		} else {
			newRing = append(newRing, h)
		}
	}
	r.ring = newRing
}

// GetNode resolves the primary shard ID and address for a partition key.
func (r *Ring) GetNode(key string) (shardID string, address string, err error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.ring) == 0 {
		return "", "", ErrEmptyRing
	}

	h := r.hashFunc([]byte(key))
	idx := sort.Search(len(r.ring), func(i int) bool {
		return r.ring[i] >= h
	})

	// Wrap around to the start of the circular ring
	if idx >= len(r.ring) {
		idx = 0
	}

	shardID = r.vnodes[r.ring[idx]]
	address = r.nodeAddress[shardID]
	return shardID, address, nil
}

// GetMembers returns a copy of all registered shard IDs and addresses.
func (r *Ring) GetMembers() map[string]string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	members := make(map[string]string, len(r.nodeAddress))
	for k, v := range r.nodeAddress {
		members[k] = v
	}
	return members
}

// MemberCount returns the number of active physical shards.
func (r *Ring) MemberCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.nodeAddress)
}

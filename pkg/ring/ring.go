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
	"time"
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

// ReplicaMember represents a single replica instance within a replica group.
type ReplicaMember struct {
	ShardID      string    `json:"shardId"`
	ReplicaID    string    `json:"replicaId"`
	Role         string    `json:"role"` // "leader" or "follower"
	GRPCAddress  string    `json:"grpcAddress"`
	HTTPAddress  string    `json:"httpAddress"`
	Status       string    `json:"status"`
	RegisteredAt time.Time `json:"registeredAt"`
}

// ReplicaGroup represents a replication group of N replicas for a shard partition.
type ReplicaGroup struct {
	GroupID string
	Members map[string]*ReplicaMember
	Leader  *ReplicaMember
}

// Ring manages consistent hashing of keys across replica groups using virtual nodes.
type Ring struct {
	mutex        sync.RWMutex
	virtualNodes int
	hashFunc     HashFunc
	ring         []uint32                 // sorted slice of token hashes
	vnodes       map[uint32]string        // token hash -> shardId (replica group ID)
	nodeAddress  map[string]string        // shardId -> primary address
	groups       map[string]*ReplicaGroup // shardId -> ReplicaGroup
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
		groups:       make(map[string]*ReplicaGroup),
	}
}

// AddNode registers a physical shard node with its virtual tokens in the ring (backwards-compatible).
func (ring *Ring) AddNode(shardID, address string) {
	ring.AddReplica(shardID, shardID, "leader", address, address)
}

// AddReplica registers a replica member to a replica group (N=3) and places the group on the ring if new.
func (ring *Ring) AddReplica(shardID, replicaID, role, grpcAddr, httpAddr string) {
	ring.mutex.Lock()
	defer ring.mutex.Unlock()

	if replicaID == "" {
		replicaID = shardID
	}
	if role == "" {
		role = "leader"
	}

	group, exists := ring.groups[shardID]
	if !exists {
		group = &ReplicaGroup{
			GroupID: shardID,
			Members: make(map[string]*ReplicaMember),
		}
		ring.groups[shardID] = group
		ring.addTokensLocked(shardID)
	}

	member := &ReplicaMember{
		ShardID:      shardID,
		ReplicaID:    replicaID,
		Role:         role,
		GRPCAddress:  grpcAddr,
		HTTPAddress:  httpAddr,
		Status:       "healthy",
		RegisteredAt: time.Now(),
	}

	group.Members[replicaID] = member
	if role == "leader" {
		group.Leader = member
		ring.nodeAddress[shardID] = grpcAddr
	}
}

func (ring *Ring) addTokensLocked(shardID string) {
	for i := 0; i < ring.virtualNodes; i++ {
		tokenID := fmt.Sprintf("%s#vn%d", shardID, i)
		hash := ring.hashFunc([]byte(tokenID))
		ring.ring = append(ring.ring, hash)
		ring.vnodes[hash] = shardID
	}
	slices.Sort(ring.ring)
}

// RemoveNode removes a replica group and all its virtual tokens from the ring.
func (ring *Ring) RemoveNode(shardID string) error {
	ring.mutex.Lock()
	defer ring.mutex.Unlock()

	if _, exists := ring.groups[shardID]; !exists && len(ring.nodeAddress[shardID]) == 0 {
		return ErrNodeNotFound
	}
	ring.removeNodeLocked(shardID)
	return nil
}

func (ring *Ring) removeNodeLocked(shardID string) {
	delete(ring.groups, shardID)
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

// GetReplicaGroup resolves the target ReplicaGroup ($N=3$) for a partition key.
func (ring *Ring) GetReplicaGroup(key string) (*ReplicaGroup, error) {
	ring.mutex.RLock()
	defer ring.mutex.RUnlock()

	if len(ring.ring) == 0 {
		return nil, ErrEmptyRing
	}

	hash := ring.hashFunc([]byte(key))
	idx := sort.Search(len(ring.ring), func(i int) bool {
		return ring.ring[i] >= hash
	})

	if idx >= len(ring.ring) {
		idx = 0
	}

	shardID := ring.vnodes[ring.ring[idx]]
	group, exists := ring.groups[shardID]
	if !exists {
		// Fallback for nodes registered via AddNode directly
		return &ReplicaGroup{
			GroupID: shardID,
			Members: map[string]*ReplicaMember{
				shardID: {
					ShardID:     shardID,
					ReplicaID:   shardID,
					Role:        "leader",
					GRPCAddress: ring.nodeAddress[shardID],
					HTTPAddress: ring.nodeAddress[shardID],
				},
			},
			Leader: &ReplicaMember{
				ShardID:     shardID,
				ReplicaID:   shardID,
				Role:        "leader",
				GRPCAddress: ring.nodeAddress[shardID],
				HTTPAddress: ring.nodeAddress[shardID],
			},
		}, nil
	}

	return group, nil
}

// GetNode resolves the primary shard ID and address for a partition key.
func (ring *Ring) GetNode(key string) (shardID string, address string, err error) {
	group, err := ring.GetReplicaGroup(key)
	if err != nil {
		return "", "", err
	}
	if group.Leader != nil && group.Leader.GRPCAddress != "" {
		return group.GroupID, group.Leader.GRPCAddress, nil
	}
	ring.mutex.RLock()
	addr := ring.nodeAddress[group.GroupID]
	ring.mutex.RUnlock()
	return group.GroupID, addr, nil
}

// GetReplicaGroups returns a snapshot map of all registered replica groups.
func (ring *Ring) GetReplicaGroups() map[string]*ReplicaGroup {
	ring.mutex.RLock()
	defer ring.mutex.RUnlock()

	result := make(map[string]*ReplicaGroup, len(ring.groups))
	for k, v := range ring.groups {
		membersCopy := make(map[string]*ReplicaMember, len(v.Members))
		maps.Copy(membersCopy, v.Members)
		result[k] = &ReplicaGroup{
			GroupID: v.GroupID,
			Members: membersCopy,
			Leader:  v.Leader,
		}
	}
	return result
}

// GetMembers returns a copy of all registered shard IDs and addresses.
func (ring *Ring) GetMembers() map[string]string {
	ring.mutex.RLock()
	defer ring.mutex.RUnlock()

	members := make(map[string]string, len(ring.nodeAddress))
	maps.Copy(members, ring.nodeAddress)
	return members
}

// MemberCount returns the number of active replica groups on the ring.
func (ring *Ring) MemberCount() int {
	ring.mutex.RLock()
	defer ring.mutex.RUnlock()
	if len(ring.groups) > 0 {
		return len(ring.groups)
	}
	return len(ring.nodeAddress)
}

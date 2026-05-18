package dkv

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"slices"
	"sort"
	"sync"
)

const (
	defaultVnodes = 128
)

// NodeID is a unique identifier for a node in the cluster.
type NodeID string

// HashRing implements consistent hashing for data partitioning across dkv nodes.
type HashRing struct {
	mu        sync.RWMutex
	vnodes    []vnode
	nodes     map[NodeID]bool
	hashBufPool     sync.Pool // pools *[]byte for key serialization
	ownersSlicePool sync.Pool // pools *[]NodeID for clockwise replica replication routing
}

// Splitting a physical node into 128 virtual positions avoids statistical hotspotting
// and ensures a uniform keyspace distribution across the cluster.
type vnode struct {
	hash uint64
	node NodeID
}

// NewHashRing initializes an empty consistent hashing ring.
func NewHashRing() *HashRing {
	return &HashRing{
		nodes: make(map[NodeID]bool),
		hashBufPool: sync.Pool{
			New: func() any {
				b := make([]byte, 0, 512)
				return &b
			},
		},
		ownersSlicePool: sync.Pool{
			New: func() any {
				s := make([]NodeID, 0, 8)
				return &s
			},
		},
	}
}


// AddNode inserts a node into the ring with a predefined number of virtual nodes.
func (r *HashRing) AddNode(nodeID NodeID) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.nodes[nodeID] {
		return
	}

	// Deterministically distribute 128 virtual points across the circular range
	bufPtr := r.hashBufPool.Get().(*[]byte)
	buf := (*bufPtr)[:0]
	for i := range defaultVnodes {
		buf = fmt.Appendf(buf[:0], "%s-%d", nodeID, i)
		h := sha256.Sum256(buf)
		hash := binary.BigEndian.Uint64(h[:8])
		r.vnodes = append(r.vnodes, vnode{hash: hash, node: nodeID})
	}
	*bufPtr = buf
	r.hashBufPool.Put(bufPtr)

	// Keep vnodes sorted in ascending order of their hash to enable O(log K) binary search lookup
	sort.Slice(r.vnodes, func(i, j int) bool {
		return r.vnodes[i].hash < r.vnodes[j].hash
	})

	r.nodes[nodeID] = true
}

// RemoveNode removes a node and its virtual nodes from the ring.
func (r *HashRing) RemoveNode(nodeID NodeID) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.nodes[nodeID] {
		return
	}

	newVnodes := make([]vnode, 0, len(r.vnodes)-defaultVnodes)
	for _, v := range r.vnodes {
		if v.node != nodeID {
			newVnodes = append(newVnodes, v)
		}
	}
	r.vnodes = newVnodes
	delete(r.nodes, nodeID)
}

// hashKey computes a uint64 hash of the given key.
func (r *HashRing) hashKey(key Key) uint64 {
	bufPtr := r.hashBufPool.Get().(*[]byte)
	buf := (*bufPtr)[:0]
	buf = append(buf, key...)
	h := sha256.Sum256(buf)
	hash := binary.BigEndian.Uint64(h[:8])
	*bufPtr = buf
	r.hashBufPool.Put(bufPtr)
	return hash
}

// GetNode returns the ID of the node responsible for the given key.
func (r *HashRing) GetNode(key Key) NodeID {
	// It maps the key onto the 64-bit circular ring and searches clockwise
	// to find the first virtual node whose hash is greater than or equal to the key's hash.

	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.vnodes) == 0 {
		return ""
	}

	hash := r.hashKey(key)

	// Perform an O(log K) binary search to locate the clockwise neighbor
	idx := sort.Search(len(r.vnodes), func(i int) bool {
		return r.vnodes[i].hash >= hash
	})

	// Circular Wrap-around: If the key hash is greater than all virtual node hashes on the ring,
	// wrap back geometrically to index 0 (the first virtual node on the circle).
	if idx == len(r.vnodes) {
		idx = 0
	}
	return r.vnodes[idx].node
}

// GetOwners returns the N nodes responsible for a key in clockwise order.
func (r *HashRing) GetOwners(key Key, replicationFactor int) []NodeID {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.vnodes) == 0 {
		return nil
	}

	hash := r.hashKey(key)

	// Locate the starting index clockwise from the key's hash with binary search
	idx := sort.Search(len(r.vnodes), func(i int) bool {
		return r.vnodes[i].hash >= hash
	})

	slicePtr := r.ownersSlicePool.Get().(*[]NodeID)
	owners := (*slicePtr)[:0]

	// Walk the circle clockwise modulo the ring length to gather N distinct physical nodes
	for i := 0; i < len(r.vnodes) && len(owners) < replicationFactor; i++ {
		vnodeIdx := (idx + i) % len(r.vnodes)
		node := r.vnodes[vnodeIdx].node

		// Skip this vnode if its physical host is already in the owner list.
		// This ensures replicas are placed on separate physical nodes for fault tolerance.
		duplicate := slices.Contains(owners, node)
		if !duplicate {
			owners = append(owners, node)
		}
	}

	return owners
}

// PutOwners returns a slice of NodeIDs back to the ring's slice pool for recycling.
func (r *HashRing) PutOwners(owners []NodeID) {
	if cap(owners) > 0 {
		owners = owners[:0]
		r.ownersSlicePool.Put(&owners)
	}
}


// GetNodes returns all unique node IDs currently in the ring.
func (r *HashRing) GetNodes() []NodeID {
	r.mu.RLock()
	defer r.mu.RUnlock()

	nodes := make([]NodeID, 0, len(r.nodes))
	for node := range r.nodes {
		nodes = append(nodes, node)
	}
	return nodes
}

package dkv

import (
	"slices"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
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
	mu     sync.RWMutex
	vnodes []vnode
	nodes  map[NodeID]bool
	pool   sync.Pool
}

type vnode struct {
	hash uint64
	node NodeID
}

// NewHashRing initializes an empty consistent hashing ring.
func NewHashRing() *HashRing {
	return &HashRing{
		nodes: make(map[NodeID]bool),
		pool: sync.Pool{
			New: func() any {
				b := make([]byte, 0, 512)
				return &b
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

	bufPtr := r.pool.Get().(*[]byte)
	buf := (*bufPtr)[:0]

	for i := range defaultVnodes {
		buf = fmt.Appendf(buf[:0], "%s-%d", nodeID, i)
		h := sha256.Sum256(buf)
		hash := binary.BigEndian.Uint64(h[:8])
		r.vnodes = append(r.vnodes, vnode{hash: hash, node: nodeID})
	}

	*bufPtr = buf
	r.pool.Put(bufPtr)

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

// GetNode returns the ID of the node responsible for the given key.
func (r *HashRing) GetNode(key Key) NodeID {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.vnodes) == 0 {
		return ""
	}

	bufPtr := r.pool.Get().(*[]byte)
	buf := (*bufPtr)[:0]
	buf = append(buf, key...)
	h := sha256.Sum256(buf)
	hash := binary.BigEndian.Uint64(h[:8])
	*bufPtr = buf
	r.pool.Put(bufPtr)

	idx := sort.Search(len(r.vnodes), func(i int) bool {
		return r.vnodes[i].hash >= hash
	})

	if idx == len(r.vnodes) {
		idx = 0
	}
	return r.vnodes[idx].node
}

// GetOwners returns the N nodes responsible for a key in clockwise order.
func (r *HashRing) GetOwners(key Key, n int) []NodeID {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.vnodes) == 0 {
		return nil
	}

	bufPtr := r.pool.Get().(*[]byte)
	buf := (*bufPtr)[:0]
	buf = append(buf, key...)
	h := sha256.Sum256(buf)
	hash := binary.BigEndian.Uint64(h[:8])
	*bufPtr = buf
	r.pool.Put(bufPtr)

	idx := sort.Search(len(r.vnodes), func(i int) bool {
		return r.vnodes[i].hash >= hash
	})

	owners := make([]NodeID, 0, n)

	for i := 0; i < len(r.vnodes) && len(owners) < n; i++ {
		vnodeIdx := (idx + i) % len(r.vnodes)
		node := r.vnodes[vnodeIdx].node
		
		duplicate := slices.Contains(owners, node)
		if !duplicate {
			owners = append(owners, node)
		}
	}

	return owners
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

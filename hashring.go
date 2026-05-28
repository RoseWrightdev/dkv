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
	hashBufPool sync.Pool
	nodes       map[NodeID]uint32
	nodeList    []NodeID
	vnodes      []vnode
	mu          sync.RWMutex
}

// Splitting a physical node into 128 virtual positions avoids statistical hotspotting
// and ensures a uniform keyspace distribution across the cluster.
// This struct is pointer-free (contains no pointers or strings), completely eliminating
// Go GC Write Barrier overhead during merge copy operations.
type vnode struct {
	hash    uint64
	nodeIdx uint32
}

// NewHashRing initializes an empty consistent hashing ring.
func NewHashRing() *HashRing {
	return &HashRing{
		nodes: make(map[NodeID]uint32),
		hashBufPool: sync.Pool{
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

	if _, exists := r.nodes[nodeID]; exists {
		return
	}

	nodeIdx := uint32(len(r.nodeList))
	r.nodes[nodeID] = nodeIdx
	r.nodeList = append(r.nodeList, nodeID)

	var newVnodes [defaultVnodes]vnode
	bufPtr := r.hashBufPool.Get().(*[]byte)
	buf := (*bufPtr)[:0]
	for i := range defaultVnodes {
		buf = fmt.Appendf(buf[:0], "%s-%d", nodeID, i)
		h := sha256.Sum256(buf)
		hash := binary.BigEndian.Uint64(h[:8])
		newVnodes[i] = vnode{hash: hash, nodeIdx: nodeIdx}
	}
	*bufPtr = buf
	r.hashBufPool.Put(bufPtr)

	slices.SortFunc(newVnodes[:], func(a, b vnode) int {
		if a.hash < b.hash {
			return -1
		} else if a.hash > b.hash {
			return 1
		}
		return 0
	})

	merged := make([]vnode, len(r.vnodes)+defaultVnodes)
	i, j := 0, 0
	k := 0
	for i < len(r.vnodes) && j < defaultVnodes {
		if r.vnodes[i].hash < newVnodes[j].hash {
			merged[k] = r.vnodes[i]
			i++
		} else {
			merged[k] = newVnodes[j]
			j++
		}
		k++
	}
	copy(merged[k:], r.vnodes[i:])
	k += len(r.vnodes) - i
	copy(merged[k:], newVnodes[j:])
	r.vnodes = merged
}

// AddNodes inserts multiple nodes into the ring in a single highly-optimized pass.
func (r *HashRing) AddNodes(nodeIDs []NodeID) {
	r.mu.Lock()
	defer r.mu.Unlock()

	var newNodes []NodeID
	for _, id := range nodeIDs {
		if _, exists := r.nodes[id]; !exists {
			newNodes = append(newNodes, id)
		}
	}

	if len(newNodes) == 0 {
		return
	}

	startIdx := len(r.nodeList)
	for i, id := range newNodes {
		r.nodes[id] = uint32(startIdx + i)
		r.nodeList = append(r.nodeList, id)
	}

	totalNewVnodes := len(newNodes) * defaultVnodes
	newVnodes := make([]vnode, totalNewVnodes)

	bufPtr := r.hashBufPool.Get().(*[]byte)
	buf := (*bufPtr)[:0]

	vIdx := 0
	for i, id := range newNodes {
		nodeIdx := uint32(startIdx + i)
		for j := range defaultVnodes {
			buf = fmt.Appendf(buf[:0], "%s-%d", id, j)
			h := sha256.Sum256(buf)
			hash := binary.BigEndian.Uint64(h[:8])
			newVnodes[vIdx] = vnode{hash: hash, nodeIdx: nodeIdx}
			vIdx++
		}
	}
	*bufPtr = buf
	r.hashBufPool.Put(bufPtr)

	slices.SortFunc(newVnodes, func(a, b vnode) int {
		if a.hash < b.hash {
			return -1
		} else if a.hash > b.hash {
			return 1
		}
		return 0
	})

	if len(r.vnodes) == 0 {
		r.vnodes = newVnodes
		return
	}

	merged := make([]vnode, len(r.vnodes)+len(newVnodes))
	i, j := 0, 0
	k := 0
	for i < len(r.vnodes) && j < len(newVnodes) {
		if r.vnodes[i].hash < newVnodes[j].hash {
			merged[k] = r.vnodes[i]
			i++
		} else {
			merged[k] = newVnodes[j]
			j++
		}
		k++
	}
	copy(merged[k:], r.vnodes[i:])
	k += len(r.vnodes) - i
	copy(merged[k:], newVnodes[j:])
	r.vnodes = merged
}

// RemoveNode removes a node and its virtual nodes from the ring.
func (r *HashRing) RemoveNode(nodeID NodeID) {
	r.mu.Lock()
	defer r.mu.Unlock()

	nodeIdx, exists := r.nodes[nodeID]
	if !exists {
		return
	}

	newVnodes := make([]vnode, 0, len(r.vnodes)-defaultVnodes)
	for _, v := range r.vnodes {
		if v.nodeIdx != nodeIdx {
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
	return r.nodeList[r.vnodes[idx].nodeIdx]
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

	owners := make([]NodeID, 0, replicationFactor)

	// Walk the circle clockwise modulo the ring length to gather N distinct physical nodes
	for i := 0; i < len(r.vnodes) && len(owners) < replicationFactor; i++ {
		vnodeIdx := (idx + i) % len(r.vnodes)
		node := r.nodeList[r.vnodes[vnodeIdx].nodeIdx]

		// Skip this vnode if its physical host is already in the owner list.
		// This ensures replicas are placed on separate physical nodes for fault tolerance.
		duplicate := slices.Contains(owners, node)
		if !duplicate {
			owners = append(owners, node)
		}
	}

	return owners
}

// PutOwners is a no-op because slice pooling was removed to avoid staticcheck allocations.
func (r *HashRing) PutOwners(_ []NodeID) {
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

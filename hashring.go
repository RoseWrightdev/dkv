package dkv

import (
	"fmt"
	"slices"
	"sort"
	"sync"

	"github.com/rosewrightdev/dkv/kv"
	"github.com/rosewrightdev/dkv/security"
)

const (
	defaultVnodes = 128
)

// NodeID is a unique identifier for a node in the cluster.
type NodeID string

// HashRing implements consistent hashing for data partitioning across dkv nodes.
type HashRing struct {
	nodes    map[NodeID]uint32
	nodeList []NodeID
	vnodes   []vnode
	mu       sync.RWMutex
}

// Splitting a physical node into 128 virtual positions avoids statistical hotspotting
// and ensures a uniform keyspace distribution across the cluster.
type vnode struct {
	hash    uint64
	nodeIdx uint32
}

var vnodeSlicePool = sync.Pool{
	New: func() any {
		s := make([]vnode, 0, 1024)
		return &s
	},
}

// NewHashRing initializes an empty consistent hashing ring.
func NewHashRing() *HashRing {
	return &HashRing{
		nodes: make(map[NodeID]uint32),
	}
}

// AddNode inserts a node into the ring with a predefined number of virtual nodes.
func (r *HashRing) AddNode(nodeID NodeID) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.nodes[nodeID]; exists {
		return
	}

	// #nosec G115
	nodeIdx := uint32(len(r.nodeList))
	r.nodes[nodeID] = nodeIdx
	r.nodeList = append(r.nodeList, nodeID)

	var newVnodes [defaultVnodes]vnode
	for i := range defaultVnodes {
		hash := security.HashFuncSecure(fmt.Sprintf("%s-%d", nodeID, i))
		newVnodes[i] = vnode{hash: hash, nodeIdx: nodeIdx}
	}

	slices.SortFunc(newVnodes[:], func(a, b vnode) int {
		if a.hash < b.hash {
			return -1
		} else if a.hash > b.hash {
			return 1
		}
		return 0
	})

	needed := len(r.vnodes) + defaultVnodes
	var merged []vnode
	if v := vnodeSlicePool.Get(); v != nil {
		sPtr := v.(*[]vnode)
		if cap(*sPtr) >= needed {
			merged = (*sPtr)[:needed]
		} else {
			vnodeSlicePool.Put(sPtr)
			merged = make([]vnode, needed)
		}
	} else {
		merged = make([]vnode, needed)
	}

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

	oldVnodes := r.vnodes
	r.vnodes = merged
	if cap(oldVnodes) > 0 {
		oldVnodes = oldVnodes[:0]
		vnodeSlicePool.Put(&oldVnodes)
	}
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
		// #nosec G115
		r.nodes[id] = uint32(startIdx + i)
		r.nodeList = append(r.nodeList, id)
	}

	totalNewVnodes := len(newNodes) * defaultVnodes
	newVnodes := make([]vnode, totalNewVnodes)

	vIdx := 0
	for i, id := range newNodes {
		// #nosec G115
		nodeIdx := uint32(startIdx + i)
		for j := range defaultVnodes {
			hash := security.HashFuncSecure(fmt.Sprintf("%s-%d", id, j))
			newVnodes[vIdx] = vnode{hash: hash, nodeIdx: nodeIdx}
			vIdx++
		}
	}

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

	needed := len(r.vnodes) + len(newVnodes)
	var merged []vnode
	if v := vnodeSlicePool.Get(); v != nil {
		sPtr := v.(*[]vnode)
		if cap(*sPtr) >= needed {
			merged = (*sPtr)[:needed]
		} else {
			vnodeSlicePool.Put(sPtr)
			merged = make([]vnode, needed)
		}
	} else {
		merged = make([]vnode, needed)
	}

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

	oldVnodes := r.vnodes
	r.vnodes = merged
	if cap(oldVnodes) > 0 {
		oldVnodes = oldVnodes[:0]
		vnodeSlicePool.Put(&oldVnodes)
	}
}

// RemoveNode removes a node and its virtual nodes from the ring.
func (r *HashRing) RemoveNode(nodeID NodeID) {
	r.mu.Lock()
	defer r.mu.Unlock()

	nodeIdx, exists := r.nodes[nodeID]
	if !exists {
		return
	}

	neededCap := max(len(r.vnodes)-defaultVnodes, 0)
	var newVnodes []vnode
	if v := vnodeSlicePool.Get(); v != nil {
		sPtr := v.(*[]vnode)
		if cap(*sPtr) >= neededCap {
			newVnodes = (*sPtr)[:0]
		} else {
			vnodeSlicePool.Put(sPtr)
			newVnodes = make([]vnode, 0, neededCap)
		}
	} else {
		newVnodes = make([]vnode, 0, neededCap)
	}

	for _, v := range r.vnodes {
		if v.nodeIdx != nodeIdx {
			newVnodes = append(newVnodes, v)
		}
	}

	oldVnodes := r.vnodes
	r.vnodes = newVnodes
	if cap(oldVnodes) > 0 {
		oldVnodes = oldVnodes[:0]
		vnodeSlicePool.Put(&oldVnodes)
	}
	delete(r.nodes, nodeID)
}

// GetNode returns the ID of the node responsible for the given key.
func (r *HashRing) GetNode(key kv.Key) NodeID {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.vnodes) == 0 {
		return ""
	}

	hash := security.HashFuncSecure(key)

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
func (r *HashRing) GetOwners(key kv.Key, replicationFactor int) []NodeID {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.vnodes) == 0 {
		return nil
	}

	hash := security.HashFuncSecure(key)

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
	_ = r
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

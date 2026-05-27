package dkv

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"slices"
	"testing"
)

func BenchmarkHashRingAdd(b *testing.B) {
	for _, nodes := range []int{10, 50, 100, 200, 400, 800} {
		b.Run(fmt.Sprintf("%d", nodes), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				r := NewHashRing()
				for j := range nodes {
					r.AddNode(NodeID(fmt.Sprintf("node-%d", j)))
				}
			}
		})
	}
}

func BenchmarkHashRingAddBatch(b *testing.B) {
	for _, nodes := range []int{10, 50, 100, 200, 400, 800} {
		b.Run(fmt.Sprintf("%d", nodes), func(b *testing.B) {
			nodeIDs := make([]NodeID, nodes)
			for j := range nodes {
				nodeIDs[j] = NodeID(fmt.Sprintf("node-%d", j))
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				r := NewHashRing()
				r.AddNodes(nodeIDs)
			}
		})
	}
}

func BenchmarkHashRingAddFast(b *testing.B) {
	for _, nodes := range []int{10, 50, 100, 200, 400, 800} {
		b.Run(fmt.Sprintf("%d", nodes), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				r := NewHashRing()
				for j := range nodes {
					addNodeFast(r, NodeID(fmt.Sprintf("node-%d", j)))
				}
			}
		})
	}
}

func addNodeFast(r *HashRing, nodeID NodeID) {
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

func BenchmarkHashRing_GetNode(b *testing.B) {
	ring := NewHashRing()
	for i := range 10 {
		ring.AddNode(NodeID(fmt.Sprintf("node-%d", i)))
	}

	key := "some-very-long-key-to-hash"

	for b.Loop() {
		_ = ring.GetNode(key)
	}
}

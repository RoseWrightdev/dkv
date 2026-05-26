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
	if r.nodes[nodeID] {
		return
	}

	newVnodes := make([]vnode, 0, defaultVnodes)
	buf := make([]byte, 0, 128)
	for i := range defaultVnodes {
		buf = fmt.Appendf(buf[:0], "%s-%d", nodeID, i)
		h := sha256.Sum256(buf)
		hash := binary.BigEndian.Uint64(h[:8])
		newVnodes = append(newVnodes, vnode{hash: hash, node: nodeID})
	}

	slices.SortFunc(newVnodes, func(a, b vnode) int {
		if a.hash < b.hash {
			return -1
		} else if a.hash > b.hash {
			return 1
		}
		return 0
	})

	merged := make([]vnode, 0, len(r.vnodes)+len(newVnodes))
	i, j := 0, 0
	for i < len(r.vnodes) && j < len(newVnodes) {
		if r.vnodes[i].hash < newVnodes[j].hash {
			merged = append(merged, r.vnodes[i])
			i++
		} else {
			merged = append(merged, newVnodes[j])
			j++
		}
	}
	merged = append(merged, r.vnodes[i:]...)
	merged = append(merged, newVnodes[j:]...)
	r.vnodes = merged
	r.nodes[nodeID] = true
}

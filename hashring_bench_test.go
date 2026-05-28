package dkv

import (
	"fmt"
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

package mesh

import (
	"fmt"
	"testing"

	"github.com/rosewrightdev/oryx/kv"
)

func BenchmarkHashRingAdd(b *testing.B) {
	for _, nodes := range []int{10, 50, 100, 200, 400, 800} {
		b.Run(fmt.Sprintf("%d", nodes), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				r := NewHashRing()
				for j := range nodes {
					r.AddNode(kv.NodeID(fmt.Sprintf("node-%d", j)))
				}
			}
		})
	}
}

func BenchmarkHashRingAddBatch(b *testing.B) {
	for _, nodes := range []int{10, 50, 100, 200, 400, 800} {
		b.Run(fmt.Sprintf("%d", nodes), func(b *testing.B) {
			nodeIDs := make([]kv.NodeID, nodes)
			for j := range nodes {
				nodeIDs[j] = kv.NodeID(fmt.Sprintf("node-%d", j))
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
		ring.AddNode(kv.NodeID(fmt.Sprintf("node-%d", i)))
	}

	key := "some-very-long-key-to-hash"

	for b.Loop() {
		_ = ring.GetNode(key)
	}
}

// BenchmarkHashRing_GetOwnersUndersizedCluster measures the path where the
// cluster holds fewer physical nodes than the replication factor, which is the
// normal state while a cluster is forming. Without the node-count cap on the
// clockwise walk, every call scans the full vnode ring instead of stopping once
// all available nodes are collected.
func BenchmarkHashRing_GetOwnersUndersizedCluster(b *testing.B) {
	for _, nodes := range []int{2, 3, 8} {
		b.Run(fmt.Sprintf("nodes=%d/rf=16", nodes), func(b *testing.B) {
			ring := NewHashRing()
			for i := range nodes {
				ring.AddNode(kv.NodeID(fmt.Sprintf("node-%d", i)))
			}
			b.ResetTimer()
			for b.Loop() {
				_ = ring.GetOwners("some-very-long-key-to-hash", 16)
			}
		})
	}
}

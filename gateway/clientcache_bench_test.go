package gateway

import (
	"fmt"
	"testing"

	"github.com/rosewrightdev/dkv/mesh"
	"google.golang.org/grpc/credentials/insecure"
)

func BenchmarkClientCacheGet(b *testing.B) {
	cc := NewClientCache(insecure.NewCredentials())

	// Pre-populate the cache with 5 peer nodes
	for i := range 5 {
		addr := mesh.PeerAddress(fmt.Sprintf("node-%d:50051", i))
		_, _ = cc.Get(addr)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		// Cycle through the 5 nodes to simulate concurrent reads
		i := 0
		for pb.Next() {
			addr := mesh.PeerAddress(fmt.Sprintf("node-%d:50051", i%5))
			_, _ = cc.Get(addr)
			i++
		}
	})
}

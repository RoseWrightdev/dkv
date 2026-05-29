package evict

import (
	"fmt"
	"testing"
	"time"
)

func BenchmarkLRU_Seen(b *testing.B) {
	lru := NewLRU(LRUConfig{
		Capacity:   10000,
		TTL:        time.Hour,
		ShardCount: 16,
	})
	lru.Start()
	defer lru.Stop()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		lru.seen(fmt.Sprintf("key-%d", i%10000), uint64(i))
	}
}

package dkv

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
	lru.start()
	defer lru.stop()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		lru.seen(fmt.Sprintf("key-%d", i%10000), uint64(i))
	}
}

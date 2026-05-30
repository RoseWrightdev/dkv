package evict

import (
	"fmt"
	"testing"
	"time"

	"github.com/rosewrightdev/dkv/kv"
)

func BenchmarkEviction_Publish(b *testing.B) {
	evt := NewLRU(LRUConfig{
		Capacity:   1000,
		TTL:        time.Hour,
		ShardCount: 16,
	})
	evt.Start()
	defer evt.Stop()
	evt.SetEvictCallback(func(_ kv.Key, _ Reason) error {
		return nil
	})

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		evt.Publish(fmt.Sprintf("key-%d", i), uint64(i))
	}
}

func BenchmarkEviction_PublishDelete(b *testing.B) {
	evt := NewLRU(LRUConfig{
		Capacity:   1000,
		TTL:        time.Hour,
		ShardCount: 16,
	})
	evt.Start()
	defer evt.Stop()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		evt.PublishDelete(fmt.Sprintf("key-%d", i), uint64(i))
	}
}

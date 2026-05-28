package dkv

import (
	"fmt"
	"testing"
	"time"
)

func BenchmarkEviction_Publish(b *testing.B) {
	evt := NewLRU(LRUConfig{
		Capacity:   1000,
		TTL:        time.Hour,
		ShardCount: 16,
	})
	evt.start()
	defer evt.stop()
	evt.SetEvictCallback(func(_ Key, _ EvictReason) error {
		return nil
	})

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		evt.publish(fmt.Sprintf("key-%d", i), uint64(i))
	}
}

func BenchmarkEviction_PublishDelete(b *testing.B) {
	evt := NewLRU(LRUConfig{
		Capacity:   1000,
		TTL:        time.Hour,
		ShardCount: 16,
	})
	evt.start()
	defer evt.stop()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		evt.publishDelete(fmt.Sprintf("key-%d", i), uint64(i))
	}
}

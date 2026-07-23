package hashmap

import (
	"fmt"
	"testing"

	"github.com/rosewrightdev/dkv/kv"
	"github.com/rosewrightdev/dkv/security"
)

func BenchmarkShardedMap_RootDigest(b *testing.B) {
	sm := NewShardedMap()
	for i := range 10000 {
		key := fmt.Sprintf("key-%d", i)
		sm.Store(key, security.HashFunc(key), kv.Value{
			Data:      []byte("value"),
			Timestamp: int64(i),
		})
	}

	b.ReportAllocs()
	for b.Loop() {
		_ = sm.RootDigest()
	}
}

func BenchmarkShardedMap_FillShardDigests(b *testing.B) {
	sm := NewShardedMap()
	for i := range 10000 {
		key := fmt.Sprintf("key-%d", i)
		sm.Store(key, security.HashFunc(key), kv.Value{
			Data:      []byte("value"),
			Timestamp: int64(i),
		})
	}
	shards := make(map[ShardID]Digest)

	b.ReportAllocs()
	for b.Loop() {
		sm.FillShardDigests(shards)
	}
}

func BenchmarkShardedMap_FillDigests(b *testing.B) {
	sm := NewShardedMap()
	for i := range 10000 {
		key := fmt.Sprintf("key-%d", i)
		sm.Store(key, security.HashFunc(key), kv.Value{
			Data:      []byte("value"),
			Timestamp: int64(i),
		})
	}
	buckets := make(map[ShardID]ShardDigest)
	for i := range ShardCount {
		buckets[ShardID(i)] = make([]Digest, SubBucketCount)
	}

	b.ReportAllocs()
	for b.Loop() {
		sm.FillDigests(buckets)
	}
}

func BenchmarkShardedMap_StoreUpdate(b *testing.B) {
	sm := NewShardedMap()
	key := "test-key"
	hash := security.HashFunc(key)

	// Pre-fill the key
	sm.Store(key, hash, kv.Value{
		NodeID:    "node-1",
		Data:      []byte("some-value-payload-of-reasonable-size"),
		Timestamp: 100,
	})

	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		sm.Store(key, hash, kv.Value{
			NodeID:    "node-1",
			Data:      []byte("some-value-payload-of-reasonable-size"),
			Timestamp: int64(i + 101),
		})
	}
}

func BenchmarkShardedMap_Delete(b *testing.B) {
	sm := NewShardedMap()
	key := "test-key"
	hash := security.HashFunc(key)

	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		b.StopTimer()
		sm.Store(key, hash, kv.Value{
			NodeID:    "node-1",
			Data:      []byte("some-value-payload-of-reasonable-size"),
			Timestamp: int64(i),
		})
		b.StartTimer()
		sm.Delete(key, hash)
	}
}

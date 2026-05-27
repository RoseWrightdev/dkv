package dkv

import (
	"fmt"
	"testing"
)

func BenchmarkShardedMap_RootDigest(b *testing.B) {
	sm := newShardedMap()
	for i := range 10000 {
		key := fmt.Sprintf("key-%d", i)
		sm.Store(key, hashFunc(key), Value{
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
	sm := newShardedMap()
	for i := range 10000 {
		key := fmt.Sprintf("key-%d", i)
		sm.Store(key, hashFunc(key), Value{
			Data:      []byte("value"),
			Timestamp: int64(i),
		})
	}
	shards := make(map[ShardID]Digest)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		sm.FillShardDigests(shards)
	}
}

func BenchmarkShardedMap_FillDigests(b *testing.B) {
	sm := newShardedMap()
	for i := range 10000 {
		key := fmt.Sprintf("key-%d", i)
		sm.Store(key, hashFunc(key), Value{
			Data:      []byte("value"),
			Timestamp: int64(i),
		})
	}
	buckets := make(map[ShardID]ShardDigest)
	for i := range shardCount {
		buckets[ShardID(i)] = make([]Digest, subBucketCount)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		sm.FillDigests(buckets)
	}
}

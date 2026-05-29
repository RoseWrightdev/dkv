package dkv

import (
	"fmt"
	"sync"
	"testing"

	"github.com/rosewrightdev/dkv/kv"
	"github.com/rosewrightdev/dkv/security"
	"github.com/stretchr/testify/assert"
)

func TestShardedMap_Basic(t *testing.T) {
	sm := newShardedMap()

	key, hash := "test", security.HashFunc("test")
	val := kv.Value{Data: []byte("val"), Timestamp: 123}

	sm.Store(key, hash, val)

	got, ok := sm.Load(key, hash)
	assert.True(t, ok)
	assert.Equal(t, val, got)

	sm.Delete(key, hash)
	val, ok = sm.Load(key, hash)
	assert.Nil(t, val.Data)
	assert.False(t, ok)
}

func TestShardedMap_Digests(t *testing.T) {
	sm := newShardedMap()

	// Ensure we put things in different shards by manually picking hashes
	sm.Store("a", 0, kv.Value{Timestamp: 1})
	sm.Store("b", 1, kv.Value{Timestamp: 1})

	digests := make(map[ShardID]ShardDigest)
	for i := range shardCount {
		digests[ShardID(i)] = make([]Digest, subBucketCount)
	}
	sm.FillDigests(digests)
	assert.Len(t, digests, int(shardCount))
	assert.NotEqual(t, digests[0], digests[1])

	// Check empty shard
	emptyDigest := make([]Digest, subBucketCount)
	assert.Equal(t, emptyDigest, digests[2], "Empty shard should have empty sub-hashes")

	// Verify sub-bucket indexing for shard 0
	// hash 0 maps to shard 0, subIndex 0
	assert.NotEqual(t, uint64(0), digests[0][0])
}

func TestShardedMap_Concurrency(t *testing.T) {
	sm := newShardedMap()
	const (
		keys       = 100
		goroutines = 10
	)

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := range goroutines {
		go func(id int) {
			defer wg.Done()
			for k := range keys {
				key := fmt.Sprintf("k-%d", k)
				h := security.HashFunc(key)
				sm.Store(key, h, kv.Value{Timestamp: int64(id)})
			}
		}(i)
	}

	wg.Wait()

	// Check random key
	key := "k-50"
	h := security.HashFunc(key)
	v, ok := sm.Load(key, h)
	assert.True(t, ok)
	assert.GreaterOrEqual(t, v.Timestamp, int64(0))
}

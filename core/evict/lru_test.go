package evict

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestLRU_Capacity(t *testing.T) {
	const capacity = 4
	const shards = 1
	lru := NewLRU(LRUConfig{
		Capacity:   capacity,
		TTL:        time.Hour,
		ShardCount: shards,
	})
	lru.Start()
	defer lru.Stop()

	evictedKeys := make(chan string, 10)
	lru.SetEvictCallback(func(key string, _ Reason) error {
		evictedKeys <- key
		return nil
	})

	// Fill shard
	lru.seen("k1", 1)
	lru.seen("k2", 2)
	lru.seen("k3", 3)
	lru.seen("k4", 4)

	// This should evict k1 (Least Recently Used)
	lru.seen("k5", 5)

	select {
	case k := <-evictedKeys:
		assert.Equal(t, "k1", k)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Timeout waiting for eviction")
	}
}

func TestLRU_TTL_Manual(t *testing.T) {
	lru := NewLRU(LRUConfig{
		Capacity:   100,
		TTL:        10 * time.Millisecond,
		ShardCount: 1,
	})

	// Small TTL and manual trigger of eviction
	lru.seen("expired", 1)
	time.Sleep(50 * time.Millisecond)

	lru.shards[0].evictExpired()

	// Verify cache is empty
	assert.Equal(t, 0, len(lru.shards[0].cache))
}

func TestLRU_BackgroundWorker(t *testing.T) {
	lru := NewLRU(LRUConfig{
		Capacity:   100,
		TTL:        50 * time.Millisecond,
		ShardCount: 1,
	})
	lru.Start()
	defer lru.Stop()

	evicted := make(chan string, 1)
	lru.SetEvictCallback(func(key string, _ Reason) error {
		evicted <- key
		return nil
	})

	lru.seen("timeout", 1)

	select {
	case k := <-evicted:
		assert.Equal(t, "timeout", k)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Background evictor failed to trigger")
	}
}

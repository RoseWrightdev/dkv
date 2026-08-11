package evict

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/rosewrightdev/dkv/kv"
	"github.com/rosewrightdev/dkv/security"

	"github.com/stretchr/testify/assert"
)

// todo: add granual tests covering more functionality
func TestTTLExpiration(t *testing.T) {
	lru := NewLRU(LRUConfig{Capacity: 100, TTL: 100 * time.Millisecond, ShardCount: 16})

	var evictCount int32
	lru.SetEvictCallback(func(_ kv.Key, _ Reason) error {
		atomic.AddInt32(&evictCount, 1)
		return nil
	})

	lru.Start()
	defer lru.Stop()

	lru.seen("expired-key", security.HashFunc("expired-key"))
	shard := lru.getShardByHash(security.HashFunc("expired-key"))

	// Wait for TTL to pass + some buffer for the reaper
	time.Sleep(250 * time.Millisecond)

	count := atomic.LoadInt32(&evictCount)
	assert.Equal(t, int32(1), count, "Key should have been evicted by TTL reaper")

	shard.mu.Lock()
	hKey := security.HashFunc("expired-key")
	_, exists := shard.cache[hKey]
	shard.mu.Unlock()
	assert.False(t, exists, "Key should be removed from cache")
}

func TestSlidingExpiration(t *testing.T) {
	ttl := 200 * time.Millisecond
	lru := NewLRU(LRUConfig{Capacity: 100, TTL: ttl, ShardCount: 16})

	var evictCount int32
	lru.SetEvictCallback(func(_ kv.Key, _ Reason) error {
		atomic.AddInt32(&evictCount, 1)
		return nil
	})

	lru.Start()
	defer lru.Stop()

	lru.seen("sliding-key", security.HashFunc("sliding-key"))

	// Wait half the TTL
	time.Sleep(120 * time.Millisecond)

	// Access again to reset TTL
	lru.seen("sliding-key", security.HashFunc("sliding-key"))

	// Wait another 120ms (total 240ms since start)
	time.Sleep(120 * time.Millisecond)

	count := atomic.LoadInt32(&evictCount)
	assert.Equal(t, int32(0), count, "Key should NOT have been evicted yet due to sliding expiration")

	// Now wait for the reset TTL to pass
	time.Sleep(200 * time.Millisecond)
	count = atomic.LoadInt32(&evictCount)
	assert.Equal(t, int32(1), count, "Key should have been evicted by now")
}

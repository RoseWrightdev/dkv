package dkv

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTTLExpiration(t *testing.T) {
	lru := NewLRU(100, 100*time.Millisecond)

	var evictCount int32
	lru.SetEvictCallback(func(key Key) error {
		atomic.AddInt32(&evictCount, 1)
		return nil
	})

	lru.start()
	defer lru.stop()

	lru.seen("expired-key")

	// Wait for TTL to pass + some buffer for the reaper
	time.Sleep(250 * time.Millisecond)

	count := atomic.LoadInt32(&evictCount)
	assert.Equal(t, int32(1), count, "Key should have been evicted by TTL reaper")

	lru.mu.Lock()

	hKey := lru.hashKey("expired-key")
	_, exists := lru.cache[hKey]
	lru.mu.Unlock()
	assert.False(t, exists, "Key should be removed from cache")
}

func TestSlidingExpiration(t *testing.T) {
	ttl := 200 * time.Millisecond
	lru := NewLRU(100, ttl)

	var evictCount int32
	lru.SetEvictCallback(func(key Key) error {
		atomic.AddInt32(&evictCount, 1)
		return nil
	})

	lru.start()
	defer lru.stop()

	lru.seen("sliding-key")

	// Wait half the TTL
	time.Sleep(120 * time.Millisecond)

	// Access again to reset TTL
	lru.seen("sliding-key")

	// Wait another 120ms (total 240ms since start)
	time.Sleep(120 * time.Millisecond)

	count := atomic.LoadInt32(&evictCount)
	assert.Equal(t, int32(0), count, "Key should NOT have been evicted yet due to sliding expiration")

	// Now wait for the reset TTL to pass
	time.Sleep(200 * time.Millisecond)
	count = atomic.LoadInt32(&evictCount)
	assert.Equal(t, int32(1), count, "Key should have been evicted by now")
}

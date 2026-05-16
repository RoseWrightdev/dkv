package dkv

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestEngineBuilder(t *testing.T) {
	eb := NewEngineBuilder()
	assert.Equal(t, 0, eb.walSegments)

	eb.SetWalPath(mockConfig.walPath)
	assert.Equal(t, mockConfig.walPath, eb.walPath)

	eb.SetSssPath(mockConfig.sssPath)
	assert.Equal(t, mockConfig.sssPath, eb.sssPath)

	eb.SetWalSyncInterval(mockConfig.walSyncInterval)
	assert.Equal(t, mockConfig.walSyncInterval, eb.walSyncInterval)

	eb.SetSssInterval(mockConfig.sssInterval)
	assert.Equal(t, mockConfig.sssInterval, eb.sssInterval)

	eb.SetWalBufferSize(mockConfig.walBufferSize)
	assert.Equal(t, mockConfig.walBufferSize, eb.walBufferSize)

	eb.SetWalSegments(mockConfig.walSegments)
	assert.Equal(t, mockConfig.walSegments, eb.walSegments)
	
	lru := NewLRU(LRUConfig{Capacity: 500, TTL: time.Minute, ShardCount: 16})
	eb.SetEvictionService(lru)
	eb.SetClock(NewHLC())

	eng, err := eb.GetEngine()
	assert.Nil(t, err)
	defer eng.Stop()

	e := eng.(*engine)
	assert.Equal(t, e.sss.interval, mockConfig.sssInterval)

	actualLRU, ok := e.evictionService.(*LeastRecentlyUsed)
	assert.True(t, ok)
	assert.Equal(t, time.Minute, actualLRU.shards[0].ttl)
	assert.Equal(t, uint32(500/16), actualLRU.shards[0].capacity)

	cleanupEngineMocks(t)
}

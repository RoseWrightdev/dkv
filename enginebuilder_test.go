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
	
	lru := NewLRU(500, time.Minute)
	eb.SetEvictionService(lru)

	eng, err := eb.GetEngine()
	assert.Nil(t, err)
	defer eng.Stop()

	assert.Equal(t, eng.sss.interval, mockConfig.sssInterval)

	actualLRU, ok := eng.evictionService.(*LeastRecentlyUsed)
	assert.True(t, ok)
	assert.Equal(t, uint32(500), actualLRU.capacity)
	assert.Equal(t, time.Minute, actualLRU.ttl)

	cleanupEngineMocks(t)
}

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
	
	lru := NewLRU(LRUConfig{
		Capacity:   500,
		TTL:        time.Minute,
		ShardCount: 16,
	})
	eb.SetEvictionService(lru)
	eb.SetClock(NewHLC())
	eb.SingleNode()

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

func TestEngineBuilder_Validation(t *testing.T) {
	t.Run("MissingWalPath", func(t *testing.T) {
		eb := NewEngineBuilder().Default().SetSssPath("tmp").SetClock(NewHLC())
		eb.walPath = ""
		_, err := eb.GetEngine()
		assert.ErrorContains(t, err, "required eb.walPath is unset")
	})

	t.Run("MissingSssPath", func(t *testing.T) {
		eb := NewEngineBuilder().Default().SetWalPath("tmp").SetClock(NewHLC())
		eb.sssPath = ""
		_, err := eb.GetEngine()
		assert.ErrorContains(t, err, "required eb.sssPath is unset")
	})

	t.Run("MissingWalSyncInterval", func(t *testing.T) {
		eb := NewEngineBuilder().Default().SetWalPath("tmp").SetSssPath("tmp").SetClock(NewHLC())
		eb.walSyncInterval = 0
		_, err := eb.GetEngine()
		assert.ErrorContains(t, err, "required eb.walSyncInterval is unset")
	})

	t.Run("MissingClock", func(t *testing.T) {
		eb := NewEngineBuilder().Default().SetWalPath("tmp").SetSssPath("tmp")
		eb.clock = nil
		_, err := eb.GetEngine()
		assert.ErrorContains(t, err, "required eb.clock is unset")
	})

	t.Run("MissingGossipInterval_InDistributedMode", func(t *testing.T) {
		eb := NewEngineBuilder().Default().SetWalPath("tmp").SetSssPath("tmp").SetClock(NewHLC())
		eb.clusterBuilder.config.SingleNode = false
		eb.gossipInterval = 0
		_, err := eb.GetEngine()
		assert.ErrorContains(t, err, "required eb.gossipInterval is unset")
	})
}

func TestEngineBuilder_ProxyMethods(t *testing.T) {
	eb := NewEngineBuilder()
	eb.SetNodeName("test-node").
		SetBindAddr("127.0.0.1").
		SetBindPort(1234).
		SetAdvertiseAddr("1.2.3.4").
		SetSeedNodes([]string{"seed:1"}).
		SetGrpcPort(8080)

	cfg := eb.clusterBuilder.Build()
	assert.Equal(t, "test-node", cfg.NodeName)
	assert.Equal(t, "127.0.0.1", cfg.BindAddr)
	assert.Equal(t, 1234, cfg.BindPort)
	assert.Equal(t, "1.2.3.4", cfg.AdvertiseAddr)
	assert.Equal(t, []string{"seed:1"}, cfg.SeedNodes)
	assert.Equal(t, 8080, cfg.GrpcPort)
}

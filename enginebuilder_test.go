package dkv

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// todo: add more invarient testing
func TestEngineBuilder(t *testing.T) {
	eb := NewEngineBuilder()
	assert.Equal(t, 0, eb.walSegments)

	eb.SetWalPath(mockConfig.walPath)
	assert.Equal(t, mockConfig.walPath, eb.walPath)

	eb.SetSnpPath(mockConfig.snpPath)
	assert.Equal(t, mockConfig.snpPath, eb.snpPath)

	eb.SetWalInterval(mockConfig.walInterval)
	assert.Equal(t, mockConfig.walInterval, eb.walInterval)

	eb.SetSnpInterval(mockConfig.snpInterval)
	assert.Equal(t, mockConfig.snpInterval, eb.snpInterval)

	eb.SetWalBufferSize(mockConfig.walBufferSize)
	assert.Equal(t, mockConfig.walBufferSize, eb.walBufferSize)

	eb.SetWalSegments(mockConfig.walSegments)
	assert.Equal(t, mockConfig.walSegments, eb.walSegments)

	lru := NewLRU(LRUConfig{
		Capacity:   500,
		TTL:        time.Minute,
		ShardCount: 16,
	})
	eb.SetEvictor(lru)
	eb.SetClock(NewHLC()).SetInsecure()
	eb.SingleNode()
	eb.SetInsecure()

	eng, err := eb.Build()
	assert.Nil(t, err)
	defer eng.Stop()

	e := eng.(*engine)
	assert.Equal(t, e.snp.interval, mockConfig.snpInterval)

	actualLRU, ok := e.evt.(*LeastRecentlyUsed)
	assert.True(t, ok)
	assert.Equal(t, time.Minute, actualLRU.shards[0].ttl)
	assert.Equal(t, uint32(500/16), actualLRU.shards[0].capacity)

	cleanupEngineMocks(t)
}

func TestEngineBuilder_Validation(t *testing.T) {
	t.Run("MissingWalPath", func(t *testing.T) {
		eb := NewEngineBuilder().Default().SetSnpPath("tmp").SetClock(NewHLC()).SetInsecure()
		eb.walPath = ""
		_, err := eb.Build()
		assert.ErrorContains(t, err, "required eb.walPath is unset")
	})

	t.Run("MissingSnpPath", func(t *testing.T) {
		eb := NewEngineBuilder().Default().SetWalPath("tmp").SetClock(NewHLC()).SetInsecure()
		eb.snpPath = ""
		_, err := eb.Build()
		assert.ErrorContains(t, err, "required eb.snpPath is unset")
	})

	t.Run("MissingWalInterval", func(t *testing.T) {
		eb := NewEngineBuilder().Default().SetWalPath("tmp").SetSnpPath("tmp").SetClock(NewHLC()).SetInsecure()
		eb.walInterval = 0
		_, err := eb.Build()
		assert.ErrorContains(t, err, "required eb.walInterval is unset")
	})

	t.Run("MissingCredentials", func(t *testing.T) {
		eb := NewEngineBuilder().Default().SetWalPath("tmp").SetSnpPath("tmp").SetClock(NewHLC()).SetInsecure()
		eb.creds = nil
		_, err := eb.Build()
		assert.ErrorContains(t, err, "transport credentials are required")
	})

	t.Run("MissingClock", func(t *testing.T) {
		eb := NewEngineBuilder().Default().SetWalPath("tmp").SetSnpPath("tmp").SetInsecure()
		eb.clock = nil
		_, err := eb.Build()
		assert.ErrorContains(t, err, "required eb.clock is unset")
	})

	t.Run("MissingGossipInterval_InDistributedMode", func(t *testing.T) {
		eb := NewEngineBuilder().Default().SetWalPath("tmp").SetSnpPath("tmp").SetClock(NewHLC()).SetInsecure()
		eb.meshBuilder.config.SingleNode = false
		eb.gossipInterval = 0
		_, err := eb.Build()
		assert.ErrorContains(t, err, "required eb.gossipInterval is unset")
	})
}

func TestEngineBuilder_ProxyMethods(t *testing.T) {
	eb := NewEngineBuilder()
	eb.SetNodeID(NodeID("test-node")).
		SetBindAddr("127.0.0.1").
		SetBindPort(1234).
		SetAdvertiseAddr("1.2.3.4").
		SetSeedNodes([]string{"seed:1"}).
		SetGrpcPort(8080)

	cfg := eb.meshBuilder.Build()
	assert.Equal(t, NodeID("test-node"), cfg.NodeID)
	assert.Equal(t, "127.0.0.1", cfg.BindAddr)
	assert.Equal(t, 1234, cfg.BindPort)
	assert.Equal(t, "1.2.3.4", cfg.AdvertiseAddr)
	assert.Equal(t, []string{"seed:1"}, cfg.SeedNodes)
	assert.Equal(t, 8080, cfg.GrpcPort)
}

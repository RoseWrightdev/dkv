package dkv

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var mockConfig EngineConfig = EngineConfig{
	walPath:         "test_wal_dir",
	sssPath:         "test_snapshot.bin",
	walSyncInterval: 100 * time.Millisecond,
	sssInterval:     500 * time.Millisecond,
	walBufferSize:   uint32(64 * 1024),
	walSegments:     4,
	evictionService: NewLRU(LRUConfig{Capacity: 100, TTL: time.Hour, ShardCount: 16}),
}

func cleanupEngineMocks(t *testing.T) {
	if err := os.Remove(mockConfig.sssPath); err != nil && !os.IsNotExist(err) {
		assert.Nil(t, err)
	}
	if err := os.Remove(mockConfig.sssPath + ".tmp"); err != nil && !os.IsNotExist(err) {
		assert.Nil(t, err)
	}
	if err := os.RemoveAll(mockConfig.walPath); err != nil && !os.IsNotExist(err) {
		assert.Nil(t, err)
	}
}

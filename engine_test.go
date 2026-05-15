package dkv

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var mockConfig EngineConfig = EngineConfig{
	walPath:         "mock_wal_path.txt",
	sssPath:         "test_snapshot.json",
	walSyncInterval: 100 * time.Millisecond,
	sssInterval:     500 * time.Millisecond,
	walBufferSize:   uint32(64 * 1024),
	evictionService: NewLRU(100, time.Hour),
}

func cleanupEngineMocks(t *testing.T) {
	if err := os.Remove(mockConfig.sssPath); err != nil && !os.IsNotExist(err) {
		assert.Nil(t, err)
	}
	if err := os.Remove(mockConfig.walPath); err != nil && !os.IsNotExist(err) {
		assert.Nil(t, err)
	}
}

func TestEngineOperations(t *testing.T) {
	eng, err := newEngine(mockConfig)
	assert.Nil(t, err)
	eng.Start()
	defer eng.Stop()
	bytes := make([]byte, 1)
	bytes = append(bytes, byte(10))

	err = eng.Set("key", bytes)
	assert.Nil(t, err)
	val, ok := eng.Get("key")
	assert.Equal(t, val, bytes)
	assert.True(t, ok)

	bytes = make([]byte, 1)
	bytes = append(bytes, byte(1))
	err = eng.Set("key", bytes)
	assert.Nil(t, err)
	val, ok = eng.Get("key")
	assert.True(t, ok)
	assert.Equal(t, val, bytes)

	err = eng.Delete("key")
	assert.Nil(t, err)

	cleanupEngineMocks(t)
}

func TestEnginePersistence(t *testing.T) {
	defer cleanupEngineMocks(t)

	eng, err := newEngine(mockConfig)
	eng.Start()
	assert.Nil(t, err)
	key1, val1 := "persist1", []byte("value1")
	key2, val2 := "persist2", []byte("value2")
	assert.Nil(t, eng.Set(key1, val1))
	assert.Nil(t, eng.Set(key2, val2))

	err = eng.sss.create()
	assert.Nil(t, err)

	key3, val3 := "persist3", []byte("value3")
	assert.Nil(t, eng.Set(key3, val3))

	assert.Nil(t, eng.wal.sync())

	eng.Stop()

	eng2, err := newEngine(mockConfig)
	assert.Nil(t, err)
	eng2.Start()
	defer eng2.Stop()

	v, ok := eng2.Get(key1)
	assert.True(t, ok)
	assert.Equal(t, val1, v)

	v, ok = eng2.Get(key3)
	assert.True(t, ok)
	assert.Equal(t, val3, v)
}

package dkv

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const (
	MOCK_SSS_PATH     = "test_snapshot.json"
	MOCK_WAL_PATH     = "mock_wal_path.txt"
	MOCK_SSS_INTERVAL = 100 * time.Millisecond
)

func cleanupEngineMocks(t *testing.T) {
	if err := os.Remove(MOCK_SSS_PATH); err != nil && !os.IsNotExist(err) {
		assert.Nil(t, err)
	}
	if err := os.Remove(MOCK_WAL_PATH); err != nil && !os.IsNotExist(err) {
		assert.Nil(t, err)
	}
}

func TestEngineBuilder(t *testing.T) {
	eb := NewEngineBuilder()
	assert.Equal(t, eb, &EngineBuilder{})

	eb = NewEngineBuilder()
	eb.SetWalPath(MOCK_WAL_PATH)
	assert.Equal(t, eb.walPath, MOCK_WAL_PATH)

	eb = NewEngineBuilder()
	eb.SetSssPath(MOCK_SSS_PATH)
	assert.Equal(t, eb.sssPath, MOCK_SSS_PATH)

	eb = NewEngineBuilder()
	eb.SetWalSyncInterval(MOCK_WAL_SYNC_INTERVAL)
	assert.Equal(t, eb.walSyncInterval, MOCK_WAL_SYNC_INTERVAL)

	eb = NewEngineBuilder()
	eb.SetSssInterval(MOCK_SSS_INTERVAL)
	assert.Equal(t, eb.sssInterval, MOCK_SSS_INTERVAL)

	eb = NewEngineBuilder()
	eb.SetWalPath(MOCK_WAL_PATH)
	eb.SetSssInterval(MOCK_SSS_INTERVAL)
	eb.SetSssPath(MOCK_SSS_PATH)
	eb.SetWalSyncInterval(MOCK_WAL_SYNC_INTERVAL)
	eng, err := eb.GetEngine()
	assert.Nil(t, err)
	defer eng.Stop()

	assert.Equal(t, eng.sss.interval, MOCK_SSS_INTERVAL)
	assert.Equal(t, eng.sss.file.Name(), MOCK_SSS_PATH)
	assert.Equal(t, eng.wal.file.Name(), MOCK_WAL_PATH)

	cleanupEngineMocks(t)
}

func TestEngineOperations(t *testing.T) {
	eng, err := newEngine(MOCK_WAL_PATH, MOCK_SSS_PATH, MOCK_WAL_SYNC_INTERVAL, MOCK_SSS_INTERVAL)
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

	eng, err := newEngine(MOCK_WAL_PATH, MOCK_SSS_PATH, MOCK_WAL_SYNC_INTERVAL, MOCK_SSS_INTERVAL)
	eng.Start()
	assert.Nil(t, err)
	key1, val1 := "persist1", []byte("value1")
	key2, val2 := "persist2", []byte("value2")
	assert.Nil(t, eng.Set(key1, val1))
	assert.Nil(t, eng.Set(key2, val2))

	err = eng.sss.createNewSnapShot()
	assert.Nil(t, err)

	key3, val3 := "persist3", []byte("value3")
	assert.Nil(t, eng.Set(key3, val3))

	assert.Nil(t, eng.wal.sync())

	eng.Stop()

	eng2, err := newEngine(MOCK_WAL_PATH, MOCK_SSS_PATH, MOCK_WAL_SYNC_INTERVAL, MOCK_SSS_INTERVAL)
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

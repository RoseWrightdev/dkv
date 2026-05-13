package core

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const MOCK_SSS_PATH = "mock_sss_path.txt"
const MOCK_WAL_PATH = "mock_wal_path.txt"
const MOCK_SSS_INTERVAL = time.Duration(3) * time.Minute

func cleanupEngineMocks(t *testing.T) {
	err := os.Remove(MOCK_SSS_PATH)
	assert.Nil(t, err)
	err = os.Remove(MOCK_WAL_PATH)
	assert.Nil(t, err)
}

func TestEngineBuilder(t *testing.T) {
	eb := NewEngineBuilder()
	assert.Equal(t, eb, &EngineBuilder{})

	eb = NewEngineBuilder()
	eb.SetSssInterval(MOCK_SSS_INTERVAL)
	assert.Equal(t, eb.sssInterval, MOCK_SSS_INTERVAL)

	eb = NewEngineBuilder()
	eb.SetSssPath(MOCK_SSS_PATH)
	assert.Equal(t, eb.sssPath, MOCK_SSS_PATH)

	eb = NewEngineBuilder()
	eb.SetWalPath(MOCK_WAL_PATH)
	assert.Equal(t, eb.walPath, MOCK_WAL_PATH)

	eb = NewEngineBuilder()
	eb.SetSssInterval(MOCK_SSS_INTERVAL)
	eb.SetSssPath(MOCK_SSS_PATH)
	eb.SetWalPath(MOCK_WAL_PATH)
	eng, err := eb.GetEngine()
	assert.Nil(t, err)
	assert.Equal(t, eng.sss.interval, MOCK_SSS_INTERVAL)
	assert.Equal(t, eng.sss.file.Name(), MOCK_SSS_PATH)
	assert.Equal(t, eng.Wal.file.Name(), MOCK_WAL_PATH)

	cleanupEngineMocks(t)
}

func TestEngineOpperations(t *testing.T) {
	eng, err := newEngine(MOCK_WAL_PATH, MOCK_SSS_PATH, MOCK_SSS_INTERVAL)
	assert.Nil(t, err)
	bytes := make([]byte, 1)
	bytes = append(bytes, byte(10))

	eng.hm.Store("key", bytes)
	val, ok := eng.Get("key")
	assert.Equal(t, val, bytes)
	assert.True(t, ok)

	exists := eng.Exists("key")
	assert.True(t, exists)

	bytes = make([]byte, 1)
	bytes = append(bytes, byte(1))
	eng.Set("key", bytes)
	rawVal, _ := eng.hm.Load(Key("key"))
	val = rawVal.(Value)
	assert.Equal(t, val, bytes)

	eng.Delete("key")
	exists = eng.Exists("key")
	assert.False(t, exists)

	cleanupEngineMocks(t)
}

package core

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const SSS_PATH = "mock_sss_path.txt"
const WAL_PATH = "mock_wal_path.txt"
const SSS_INTERVAL = time.Duration(3) * time.Minute

func cleanup(t *testing.T) {
	err := os.Remove(SSS_PATH)
	assert.Nil(t, err)
	err = os.Remove(WAL_PATH)
	assert.Nil(t, err)
}

func TestEngineBuilder(t *testing.T) {
	eb := NewEngineBuilder()
	assert.Equal(t, eb, &EngineBuilder{})

	eb = NewEngineBuilder()
	eb.SetSssInterval(SSS_INTERVAL)
	assert.Equal(t, eb.sssInterval, SSS_INTERVAL)

	eb = NewEngineBuilder()
	eb.SetSssPath(SSS_PATH)
	assert.Equal(t, eb.sssPath, SSS_PATH)

	eb = NewEngineBuilder()
	eb.SetWalPath(WAL_PATH)
	assert.Equal(t, eb.walPath, WAL_PATH)

	eb = NewEngineBuilder()
	eb.SetSssInterval(SSS_INTERVAL)
	eb.SetSssPath(SSS_PATH)
	eb.SetWalPath(WAL_PATH)
	eng, err := eb.GetEngine()
	assert.Nil(t, err)
	assert.Equal(t, eng.sss.interval, SSS_INTERVAL)
	assert.Equal(t, eng.sss.file.Name(), SSS_PATH)
	assert.Equal(t, eng.Wal.file.Name(), WAL_PATH)

	cleanup(t)
}

func TestEngineOpperations(t *testing.T) {
	eng, err := newEngine(WAL_PATH, SSS_PATH, SSS_INTERVAL)
	assert.Nil(t, err)
	bytes := make([]byte, 1)
	bytes = append(bytes, byte(10))

	eng.hm.Store("key", bytes)
	val, ok := eng.Get("key")
	assert.Equal(t, bytes, val)
	assert.True(t, ok)

	exists := eng.Exists("key")
	assert.True(t, exists)

	bytes = make([]byte, 1)
	bytes = append(bytes, byte(1))
	eng.Set("key", bytes)
	rawVal, _ := eng.hm.Load(Key("key"))
	val = rawVal.(Value)
	assert.Equal(t, bytes, val)

	eng.Delete("key")
	exists = eng.Exists("key")
	assert.False(t, exists)

	cleanup(t)
}

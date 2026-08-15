package core

import (
	"os"
	"testing"
	"time"

	"github.com/rosewrightdev/oryx/core/clock"
	"github.com/rosewrightdev/oryx/core/wal"
	"github.com/rosewrightdev/oryx/kv"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEngine_VolatileMode(t *testing.T) {
	config := Config{
		WalPath:         "nop",
		SnpPath:         "nop",
		WalInterval:     100 * time.Millisecond,
		SnpInterval:     100 * time.Millisecond,
		Clock:           clock.NewClock(),
		DisableWal:      true,
		DisableSnapshot: true,
	}

	eng, err := NewEngine(config)
	require.NoError(t, err)

	eng.Start()
	defer eng.Stop()

	// Verify WAL is NopWal
	_, isNopWal := eng.Wal().(*wal.NopWal)
	assert.True(t, isNopWal, "Wal() should return *wal.NopWal when DisableWal is true")

	// Verify Basic CRUD in memory
	key := kv.Key("volatile-key")
	val := []byte("volatile-val")

	assert.NoError(t, eng.Set(key, val))
	got, ok := eng.Get(key)
	assert.True(t, ok)
	assert.Equal(t, val, got)

	deleted, err := eng.Delete(key)
	assert.NoError(t, err)
	assert.True(t, deleted)

	_, ok = eng.Get(key)
	assert.False(t, ok)

	// Ensure no files were written to disk under "nop" or "nop.tmp"
	_, err = os.Stat("nop")
	assert.True(t, os.IsNotExist(err), "nop file/dir should not exist")
	_, err = os.Stat("nop.tmp")
	assert.True(t, os.IsNotExist(err), "nop.tmp file/dir should not exist")
}

package dkv

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEngineBuilder(t *testing.T) {
	eb := NewEngineBuilder()
	assert.Equal(t, eb, &EngineBuilder{})

	eb = NewEngineBuilder()
	eb.SetWalPath(mockConfig.walPath)
	assert.Equal(t, mockConfig.walPath, eb.walPath)

	eb = NewEngineBuilder()
	eb.SetSssPath(mockConfig.sssPath)
	assert.Equal(t, mockConfig.sssPath, eb.sssPath)

	eb = NewEngineBuilder()
	eb.SetWalSyncInterval(mockConfig.walSyncInterval)
	assert.Equal(t, mockConfig.walSyncInterval, eb.walSyncInterval)

	eb = NewEngineBuilder()
	eb.SetSssInterval(mockConfig.sssInterval)
	assert.Equal(t, mockConfig.sssInterval, eb.sssInterval)

	eb = NewEngineBuilder()
	eb.SetWalBufferSize(mockConfig.walBufferSize)
	assert.Equal(t, mockConfig.walBufferSize, eb.walBufferSize)

	eb = NewEngineBuilder()
	eb.SetWalPath(mockConfig.walPath)
	eb.SetSssInterval(mockConfig.sssInterval)
	eb.SetSssPath(mockConfig.sssPath)
	eb.SetWalSyncInterval(mockConfig.walSyncInterval)
	eb.SetWalBufferSize(mockConfig.walBufferSize)
	eng, err := eb.GetEngine()
	assert.Nil(t, err)
	defer eng.Stop()

	assert.Equal(t, eng.sss.interval, mockConfig.sssInterval)
	assert.Equal(t, eng.sss.file.Name(), mockConfig.sssPath)
	assert.Equal(t, eng.wal.file.Name(), mockConfig.walPath)

	cleanupEngineMocks(t)
}

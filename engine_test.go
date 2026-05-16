package dkv

import (
	"testing"

	pb "github.com/rosewrightdev/dkv/api"
	"github.com/stretchr/testify/assert"
)

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

	err = eng.Snapshot()
	assert.Nil(t, err)

	key3, val3 := "persist3", []byte("value3")
	assert.Nil(t, eng.Set(key3, val3))

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

func TestEngine_LWW(t *testing.T) {
	defer cleanupEngineMocks(t)
	e, _ := newEngine(mockConfig)
	eng := e.(*engine)
	eng.Start()
	defer eng.Stop()

	key := "lww-key"
	val1 := []byte("old-value")
	val2 := []byte("new-value")

	ts1 := int64(1000)
	eng.clock.Update(ts1)
	assert.NoError(t, eng.Set(key, val1))

	ts2 := int64(2000)
	eng.clock.Update(ts2)
	assert.NoError(t, eng.Set(key, val2))
	got, _ := eng.Get(key)
	assert.Equal(t, val2, got)

	// Set with older timestamp (should be ignored)
	ts3 := int64(1500)
	// We call applyGossipSet directly to simulate a delayed gossip arrival
	err := eng.applyGossipSet(&pb.SetRequest{
		Key:       key,
		Value:     []byte("delayed-old-value"),
		Timestamp: ts3,
	})
	assert.NoError(t, err)
	got, _ = eng.Get(key)
	assert.Equal(t, val2, got, "Older timestamp should not overwrite newer data")
}

func TestEngine_TombstoneLWW(t *testing.T) {
	defer cleanupEngineMocks(t)
	e, _ := newEngine(mockConfig)
	eng := e.(*engine)
	eng.Start()
	defer eng.Stop()

	key := "tomb-key"
	val := []byte("data")

	ts1 := int64(1000)
	eng.clock.Update(ts1)
	eng.Set(key, val)

	ts2 := int64(2000)
	eng.clock.Update(ts2)
	eng.Delete(key)

	_, ok := eng.Get(key)
	assert.False(t, ok, "Key should be deleted")

	// Late-arriving Set with older timestamp
	ts3 := int64(1500)
	err := eng.applyGossipSet(&pb.SetRequest{
		Key:       key,
		Value:     []byte("zombie"),
		Timestamp: ts3,
	})
	assert.NoError(t, err)
	_, ok = eng.Get(key)
	assert.False(t, ok, "Old set should not resurrect a newer tombstone")
}

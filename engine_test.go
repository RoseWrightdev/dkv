package dkv

import (
	"fmt"
	"sync"
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

func TestEngine_SyncLogic(t *testing.T) {
	defer cleanupEngineMocks(t)
	e1, _ := newEngine(mockConfig)
	eng1 := e1.(*engine)
	eng1.Start()
	defer eng1.Stop()

	e2, _ := newEngine(mockConfig)
	eng2 := e2.(*engine)
	eng2.Start()
	defer eng2.Stop()

	// 1. Setup eng1 with data
	key1, val1 := "sync-1", []byte("data-1")
	eng1.Set(key1, val1)

	// 2. eng2 is empty, it pulls from eng1
	digests2 := eng2.hm.Digests()
	sets, deletes, err := eng1.SyncPull(digests2)
	assert.NoError(t, err)
	assert.Len(t, sets, 1)
	assert.Len(t, deletes, 0)
	assert.Equal(t, key1, sets[0].Key)

	// 3. eng2 pushes the updates
	err = eng2.SyncPush(sets, deletes)
	assert.NoError(t, err)
	
	got, ok := eng2.Get(key1)
	assert.True(t, ok)
	assert.Equal(t, val1, got)
}

func TestEngine_Concurrency(t *testing.T) {
	defer cleanupEngineMocks(t)
	e, _ := newEngine(mockConfig)
	eng := e.(*engine)
	eng.Start()
	defer eng.Stop()

	const (
		goroutines = 10
		iterations = 100
	)

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				key := fmt.Sprintf("k-%d-%d", id, i)
				_ = eng.Set(key, []byte("v"))
				_, _ = eng.Get(key)
			}
		}(g)
	}

	wg.Wait()
}

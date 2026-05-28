package dkv

import (
	"os"
	"strconv"
	"testing"

	pb "github.com/rosewrightdev/dkv/api"
	"github.com/stretchr/testify/assert"
)

func TestNewWal(t *testing.T) {
	_, err := newWal(mockConfig.walPath, mockConfig.walInterval, mockConfig.walBufferSize, 1)
	assert.Nil(t, err)

	cleanupEngineMocks(t)
}

func TestPublish(t *testing.T) {
	defer cleanupEngineMocks(t)

	req := pb.SetRequest{Key: "key", Value: []byte{byte(32)}, Timestamp: 100}
	wal, err := newWal(mockConfig.walPath, mockConfig.walInterval, mockConfig.walBufferSize, 1)
	assert.Nil(t, err)

	err = wal.publish(req.Key, hashFunc(req.Key), &req)
	assert.Nil(t, err)

	replay, err := wal.replay()
	assert.Nil(t, err)
	assert.Equal(t, []byte{32}, replay["key"].Data)
	assert.Equal(t, int64(100), replay["key"].Timestamp)
}

func TestReplay(t *testing.T) {
	defer cleanupEngineMocks(t)

	wal, err := newWal(mockConfig.walPath, mockConfig.walInterval, mockConfig.walBufferSize, 4)
	exceptedValues := make([][]byte, 1000)
	exceptedKeys := make([]string, 1000)
	assert.Nil(t, err)

	for i := range 1000 {
		key, val := strconv.Itoa(i), []byte{byte(i)}
		exceptedValues[i] = val
		exceptedKeys[i] = key
		req := pb.SetRequest{Key: key, Value: val, Timestamp: int64(i)}
		err = wal.publish(key, hashFunc(key), &req)
		assert.Nil(t, err)
	}
	replay, err := wal.replay()
	assert.Nil(t, err, "Replay returned error")

	gotValues := make([][]byte, 0, 1000)
	gotKeys := make([]string, 0, 1000)
	for k, v := range replay {
		gotKeys = append(gotKeys, k)
		gotValues = append(gotValues, v.Data)
	}

	assert.ElementsMatch(t, exceptedValues, gotValues)
	assert.ElementsMatch(t, exceptedKeys, gotKeys)
}

func TestClear(t *testing.T) {
	defer cleanupEngineMocks(t)

	wal, err := newWal(mockConfig.walPath, mockConfig.walInterval, mockConfig.walBufferSize, 1)
	assert.Nil(t, err)

	assert.Nil(t, wal.clear(nil))
	content, err := os.ReadFile(mockConfig.walPath + "/seg_00.log")
	assert.Nil(t, err)
	assert.Equal(t, 0, len(content))
}

func TestWal_PrepareSnapshot(t *testing.T) {
	defer cleanupEngineMocks(t)

	wal, err := newWal(mockConfig.walPath, mockConfig.walInterval, mockConfig.walBufferSize, 2)
	assert.Nil(t, err)

	// Write entries so segment files are non-empty
	for i := range 20 {
		key := strconv.Itoa(i)
		req := pb.SetRequest{Key: key, Value: []byte{byte(i)}, Timestamp: int64(i)}
		assert.Nil(t, wal.publish(key, hashFunc(key), &req))
	}

	offsets, err := wal.prepareSnapshot()
	assert.Nil(t, err)
	assert.Len(t, offsets, 2, "prepareSnapshot should return one offset per segment")

	// Each offset should be positive (segments are non-empty)
	for i, off := range offsets {
		assert.Positive(t, off, "segment %d offset should be > 0", i)
	}
}

func TestWal_ClearWithOffsets(t *testing.T) {
	defer cleanupEngineMocks(t)

	wal, err := newWal(mockConfig.walPath, mockConfig.walInterval, mockConfig.walBufferSize, 1)
	assert.Nil(t, err)

	// Write some entries before snapshot point
	for i := range 5 {
		key := strconv.Itoa(i)
		req := pb.SetRequest{Key: key, Value: []byte{byte(i)}, Timestamp: int64(i)}
		assert.Nil(t, wal.publish(key, hashFunc(key), &req))
	}

	// Capture snapshot offsets
	offsets, err := wal.prepareSnapshot()
	assert.Nil(t, err)

	// Write more entries AFTER the snapshot point
	postKeys := []string{"post-a", "post-b", "post-c"}
	for _, k := range postKeys {
		req := pb.SetRequest{Key: k, Value: []byte("post-snapshot"), Timestamp: 999}
		assert.Nil(t, wal.publish(k, hashFunc(k), &req))
	}

	// clear with offsets: only data before snapshot should be removed;
	// post-snapshot entries should survive.
	assert.Nil(t, wal.clear(offsets))

	// Replay and verify only post-snapshot entries remain
	replay, err := wal.replay()
	assert.Nil(t, err)
	for _, k := range postKeys {
		_, ok := replay[Key(k)]
		assert.True(t, ok, "post-snapshot key %q should survive clear(offsets)", k)
	}

	// Pre-snapshot keys should be gone
	for i := range 5 {
		k := Key(strconv.Itoa(i))
		_, ok := replay[k]
		assert.False(t, ok, "pre-snapshot key %q should have been cleared", k)
	}
}

func TestWal_ClearNilOffsets(t *testing.T) {
	defer cleanupEngineMocks(t)

	wal, err := newWal(mockConfig.walPath, mockConfig.walInterval, mockConfig.walBufferSize, 2)
	assert.Nil(t, err)

	for i := range 10 {
		key := strconv.Itoa(i)
		req := pb.SetRequest{Key: key, Value: []byte{byte(i)}, Timestamp: int64(i)}
		assert.Nil(t, wal.publish(key, hashFunc(key), &req))
	}

	// clear(nil) should truncate all segments entirely
	assert.Nil(t, wal.clear(nil))

	replay, err := wal.replay()
	assert.Nil(t, err)
	assert.Empty(t, replay, "all entries should be cleared when offsets is nil")
}


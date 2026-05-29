package wal

import (
	"os"
	"strconv"
	"testing"
	"time"

	pb "github.com/rosewrightdev/dkv/api"
	"github.com/rosewrightdev/dkv/kv"
	"github.com/rosewrightdev/dkv/security"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	mockWalPath       = "test_wal_dir"
	mockWalInterval   = 100 * time.Millisecond
	mockWalBufferSize = uint32(64 * 1024)
)

func cleanupWal(t *testing.T) {
	if err := os.RemoveAll(mockWalPath); err != nil && !os.IsNotExist(err) {
		assert.Nil(t, err)
	}
}

func TestNewWal(t *testing.T) {
	_, err := NewWal(mockWalPath, mockWalInterval, mockWalBufferSize, 1)
	assert.Nil(t, err)

	cleanupWal(t)
}

func TestPublish(t *testing.T) {
	defer cleanupWal(t)

	req := pb.SetRequest{Key: "key", Value: []byte{byte(32)}, Timestamp: 100}
	wal, err := NewWal(mockWalPath, mockWalInterval, mockWalBufferSize, 1)
	assert.Nil(t, err)

	err = wal.Publish(req.Key, security.HashFunc(req.Key), &req)
	assert.Nil(t, err)

	replay, err := wal.Replay()
	assert.Nil(t, err)
	assert.Equal(t, []byte{32}, replay["key"].Data)
	assert.Equal(t, int64(100), replay["key"].Timestamp)
}

func TestReplay(t *testing.T) {
	defer cleanupWal(t)

	wal, err := NewWal(mockWalPath, mockWalInterval, mockWalBufferSize, 4)
	exceptedValues := make([][]byte, 1000)
	exceptedKeys := make([]string, 1000)
	assert.Nil(t, err)

	for i := range 1000 {
		key, val := strconv.Itoa(i), []byte{byte(i)}
		exceptedValues[i] = val
		exceptedKeys[i] = key
		req := pb.SetRequest{Key: key, Value: val, Timestamp: int64(i)}
		err = wal.Publish(key, security.HashFunc(key), &req)
		assert.Nil(t, err)
	}
	replay, err := wal.Replay()
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
	defer cleanupWal(t)

	wal, err := NewWal(mockWalPath, mockWalInterval, mockWalBufferSize, 1)
	assert.Nil(t, err)

	assert.Nil(t, wal.Clear(nil))
	content, err := os.ReadFile(mockWalPath + "/seg_00.log")
	assert.Nil(t, err)
	assert.Equal(t, 0, len(content))
}

func TestWal_PrepareSnapshot(t *testing.T) {
	defer cleanupWal(t)

	wal, err := NewWal(mockWalPath, mockWalInterval, mockWalBufferSize, 2)
	assert.Nil(t, err)

	// Write entries so segment files are non-empty
	for i := range 20 {
		key := strconv.Itoa(i)
		req := pb.SetRequest{Key: key, Value: []byte{byte(i)}, Timestamp: int64(i)}
		assert.Nil(t, wal.Publish(key, security.HashFunc(key), &req))
	}

	offsets, err := wal.PrepareSnapshot()
	assert.Nil(t, err)
	assert.Len(t, offsets, 2, "PrepareSnapshot should return one offset per segment")

	// Each offset should be positive (segments are non-empty)
	for i, off := range offsets {
		assert.Positive(t, off, "segment %d offset should be > 0", i)
	}
}

func TestWal_ClearWithOffsets(t *testing.T) {
	defer cleanupWal(t)

	wal, err := NewWal(mockWalPath, mockWalInterval, mockWalBufferSize, 1)
	assert.Nil(t, err)

	// Write some entries before snapshot point
	for i := range 5 {
		key := strconv.Itoa(i)
		req := pb.SetRequest{Key: key, Value: []byte{byte(i)}, Timestamp: int64(i)}
		assert.Nil(t, wal.Publish(key, security.HashFunc(key), &req))
	}

	// Capture snapshot offsets
	offsets, err := wal.PrepareSnapshot()
	assert.Nil(t, err)

	// Write more entries AFTER the snapshot point
	postKeys := []string{"post-a", "post-b", "post-c"}
	for _, k := range postKeys {
		req := pb.SetRequest{Key: k, Value: []byte("post-snapshot"), Timestamp: 999}
		assert.Nil(t, wal.Publish(k, security.HashFunc(k), &req))
	}

	// Clear with offsets: only data before snapshot should be removed;
	// post-snapshot entries should survive.
	assert.Nil(t, wal.Clear(offsets))

	// Replay and verify only post-snapshot entries remain
	replay, err := wal.Replay()
	assert.Nil(t, err)
	for _, k := range postKeys {
		_, ok := replay[kv.Key(k)]
		assert.True(t, ok, "post-snapshot key %q should survive Clear(offsets)", k)
	}

	// Pre-snapshot keys should be gone
	for i := range 5 {
		k := kv.Key(strconv.Itoa(i))
		_, ok := replay[k]
		assert.False(t, ok, "pre-snapshot key %q should have been cleared", k)
	}
}

func TestWal_ClearNilOffsets(t *testing.T) {
	defer cleanupWal(t)

	wal, err := NewWal(mockWalPath, mockWalInterval, mockWalBufferSize, 2)
	assert.Nil(t, err)

	for i := range 10 {
		key := strconv.Itoa(i)
		req := pb.SetRequest{Key: key, Value: []byte{byte(i)}, Timestamp: int64(i)}
		assert.Nil(t, wal.Publish(key, security.HashFunc(key), &req))
	}

	// Clear(nil) should truncate all segments entirely
	assert.Nil(t, wal.Clear(nil))

	replay, err := wal.Replay()
	assert.Nil(t, err)
	assert.Empty(t, replay, "all entries should be cleared when offsets is nil")
}

func TestWal_ExtraEdgeCases(t *testing.T) {
	defer cleanupWal(t)

	// 1. NewWal directory creation failure
	// We can create a regular file first, then try to create WAL with that file's path as the directory.
	// This will make os.MkdirAll fail.
	tmpFile, err := os.CreateTemp("", "wal-failure-test-*")
	require.NoError(t, err)
	defer func() {
		_ = tmpFile.Close()
		_ = os.Remove(tmpFile.Name())
	}()

	_, err = NewWal(tmpFile.Name(), mockWalInterval, mockWalBufferSize, 1)
	assert.Error(t, err)

	// 2. Publish pb.WalEntry directly
	wal, err := NewWal(mockWalPath, mockWalInterval, mockWalBufferSize, 1)
	assert.NoError(t, err)
	defer wal.Stop()

	entryMsg := &pb.WalEntry{
		Entry: &pb.WalEntry_Set{
			Set: &pb.SetRequest{Key: "direct-entry", Value: []byte("val"), Timestamp: 200},
		},
	}
	err = wal.Publish("direct-entry", security.HashFunc("direct-entry"), entryMsg)
	assert.NoError(t, err)

	// 3. Publish unsupported type
	err = wal.Publish("key", security.HashFunc("key"), &pb.GetRequest{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported message type")

	// 4. Publish pb.DeleteRequest and verify unwrap recycled pool wrappers
	delMsg := &pb.DeleteRequest{Key: "direct-del", Timestamp: 250}
	err = wal.Publish("direct-del", security.HashFunc("direct-del"), delMsg)
	assert.NoError(t, err)

	// Verify Replay on sets and deletes
	replay, err := wal.Replay()
	assert.NoError(t, err)
	assert.Equal(t, []byte("val"), replay["direct-entry"].Data)
	assert.True(t, replay["direct-del"].Tombstone)

	// 5. replaySegment unmarshal error by writing bad bytes to the log
	wal.Stop() // stop sync so we can manually edit file safely
	segPath := mockWalPath + "/seg_00.log"

	// Corrupt the file by writing a invalid header and payload
	// #nosec G304
	f, err := os.OpenFile(segPath, os.O_WRONLY|os.O_APPEND, 0600)
	require.NoError(t, err)
	// Write header: 4 bytes size
	_, _ = f.Write([]byte{0, 0, 0, 10}) // says payload is 10 bytes
	// Write bad payload: 10 bytes of garbage
	_, _ = f.Write([]byte("garbagedata"))
	_ = f.Close()

	// Replay should fail due to protobuf unmarshal error
	walReopen, err := NewWal(mockWalPath, mockWalInterval, mockWalBufferSize, 1)
	assert.NoError(t, err)
	defer walReopen.Stop()

	_, err = walReopen.Replay()
	assert.Error(t, err)
}

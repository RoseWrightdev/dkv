package dkv

import (
	"os"
	"strconv"
	"testing"

	pb "github.com/rosewrightdev/dkv/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestWal_ExtraEdgeCases(t *testing.T) {
	defer cleanupEngineMocks(t)

	// 1. newWal directory creation failure
	// We can create a regular file first, then try to create WAL with that file's path as the directory.
	// This will make os.MkdirAll fail.
	tmpFile, err := os.CreateTemp("", "wal-failure-test-*")
	require.NoError(t, err)
	defer func() {
		_ = tmpFile.Close()
		_ = os.Remove(tmpFile.Name())
	}()

	_, err = newWal(tmpFile.Name(), mockConfig.walInterval, mockConfig.walBufferSize, 1)
	assert.Error(t, err)

	// 2. publish pb.WalEntry directly
	wal, err := newWal(mockConfig.walPath, mockConfig.walInterval, mockConfig.walBufferSize, 1)
	assert.NoError(t, err)
	defer wal.stop()

	entryMsg := &pb.WalEntry{
		Entry: &pb.WalEntry_Set{
			Set: &pb.SetRequest{Key: "direct-entry", Value: []byte("val"), Timestamp: 200},
		},
	}
	err = wal.publish("direct-entry", hashFunc("direct-entry"), entryMsg)
	assert.NoError(t, err)

	// 3. publish unsupported type
	err = wal.publish("key", hashFunc("key"), &pb.GetRequest{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported message type")

	// 4. publish pb.DeleteRequest and verify unwrap recycled pool wrappers
	delMsg := &pb.DeleteRequest{Key: "direct-del", Timestamp: 250}
	err = wal.publish("direct-del", hashFunc("direct-del"), delMsg)
	assert.NoError(t, err)

	// Verify replay on sets and deletes
	replay, err := wal.replay()
	assert.NoError(t, err)
	assert.Equal(t, []byte("val"), replay["direct-entry"].Data)
	assert.True(t, replay["direct-del"].Tombstone)

	// 5. replaySegment unmarshal error by writing bad bytes to the log
	wal.stop() // stop sync so we can manually edit file safely
	segPath := mockConfig.walPath + "/seg_00.log"
	
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
	walReopen, err := newWal(mockConfig.walPath, mockConfig.walInterval, mockConfig.walBufferSize, 1)
	assert.NoError(t, err)
	defer walReopen.stop()

	_, err = walReopen.replay()
	assert.Error(t, err)
}



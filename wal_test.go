package dkv

import (
	"os"
	"strconv"
	"testing"

	pb "github.com/rosewrightdev/dkv/api"
	"github.com/stretchr/testify/assert"
)



func TestNewWal(t *testing.T) {
	_, err := newWal(mockConfig.walPath, mockConfig.walSyncInterval, mockConfig.walBufferSize, 1)
	assert.Nil(t, err)

	cleanupEngineMocks(t)
}

func TestPublish(t *testing.T) {
	defer cleanupEngineMocks(t)

	req := pb.SetRequest{Key: "key", Value: []byte{byte(32)}, Timestamp: 100}
	wal, err := newWal(mockConfig.walPath, mockConfig.walSyncInterval, mockConfig.walBufferSize, 1)
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

	wal, err := newWal(mockConfig.walPath, mockConfig.walSyncInterval, mockConfig.walBufferSize, 4)
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

	wal, err := newWal(mockConfig.walPath, mockConfig.walSyncInterval, mockConfig.walBufferSize, 1)
	assert.Nil(t, err)

	assert.Nil(t, wal.clear())
	content, err := os.ReadFile(mockConfig.walPath + "/seg_00.log")
	assert.Nil(t, err)
	assert.Equal(t, 0, len(content))
}

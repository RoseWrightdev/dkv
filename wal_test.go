package dkv

import (
	"maps"
	"os"
	"strconv"
	"testing"

	"slices"

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

	req := pb.SetRequest{Key: "key", Value: []byte{byte(32)}}
	wal, err := newWal(mockConfig.walPath, mockConfig.walSyncInterval, mockConfig.walBufferSize, 1)
	assert.Nil(t, err)

	err = wal.publish(req.Key, hashFunc(req.Key), &req)
	assert.Nil(t, err)

	for _, seg := range wal.segments {
		seg.mu.Lock()
		seg.wrt.Flush()
		seg.file.Sync()
		seg.mu.Unlock()
	}

	// Read from the first segment since we used segmentCount=1
	got, err := os.ReadFile(mockConfig.walPath + "/seg_00.log")
	assert.Nil(t, err)

	// 00000000 00000000 00000000 00001010 00001010 00001000
	//  the first 4 bytes are the header ^        ^        ^
	//             Protobuf Tag 1 for the set feild        |
	//                        Length of the set msg (8 bytes)
	// 00001010 00000011 01101011 01100101 01111001 00010010
	//   Tag 1^       3^     "k"^     "e"^     "y"^   Tag 2^
	// 00000001 00100000
	//        1^     32^

	expected := []byte{
		0, 0, 0, 10,
		0x0A, 8,
		0b00001010, 3, 'k', 'e', 'y',
		0b00010010, 1, 32,
	}

	assert.Equal(t, expected, got)
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
		req := pb.SetRequest{Key: key, Value: val}
		err = wal.publish(key, hashFunc(key), &req)
		assert.Nil(t, err)
	}
	replay, err := wal.replay()
	assert.Nil(t, err, "Replay returned error")

	gotValues := slices.Collect(maps.Values(replay))
	gotKeys := slices.Collect(maps.Keys(replay))

	assert.ElementsMatch(t, exceptedValues, gotValues)
	assert.ElementsMatch(t, exceptedKeys, gotKeys)
}

func TestClear(t *testing.T) {
	defer cleanupEngineMocks(t)

	wal, err := newWal(mockConfig.walPath, mockConfig.walSyncInterval, mockConfig.walBufferSize, 1)
	assert.Nil(t, err)

	wal.clear()
	content, err := os.ReadFile(mockConfig.walPath + "/seg_00.log")
	assert.Equal(t, 0, len(content))
}

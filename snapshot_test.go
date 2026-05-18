package dkv

import (
	"encoding/gob"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

type mockWal struct {
	clearCalled bool
}

func (mw *mockWal) publish(_ Key, _ hashKey, _ proto.Message) error { return nil }
func (mw *mockWal) replay() (map[Key]Value, error)                  { return nil, nil }
func (mw *mockWal) clear(_ []int64) error                           { mw.clearCalled = true; return nil }
func (mw *mockWal) prepareSnapshot() ([]int64, error)               { return nil, nil }
func (mw *mockWal) stop()                                           {}
func (mw *mockWal) start()                                          {}

func TestNewSnapshoter(t *testing.T) {
	defer cleanupEngineMocks(t)

	mw := &mockWal{}
	callBack := func(_ *gob.Encoder) error { return nil }

	snp, err := newSnapshoter(mockConfig.snpPath, mockConfig.snpInterval, mw, callBack)
	assert.NoError(t, err)
	assert.NotNil(t, snp)
	assert.Equal(t, mockConfig.snpPath, snp.path)
}

func TestCreateNewSnapShot(t *testing.T) {
	defer cleanupEngineMocks(t)

	mw := &mockWal{}
	mockData := map[Key]Value{
		"user:1": {Data: []byte("alice"), Timestamp: 100},
		"user:2": {Data: []byte("bob"), Timestamp: 100},
	}
	callBack := func(enc *gob.Encoder) error {
		for k, v := range mockData {
			if err := enc.Encode(snapshotEntry{Key: k, Data: v.Data, Timestamp: v.Timestamp, Tombstone: v.Tombstone}); err != nil {
				return err
			}
		}
		return nil
	}
	snp, _ := newSnapshoter(mockConfig.snpPath, mockConfig.snpInterval, mw, callBack)

	err := snp.create()
	assert.NoError(t, err)

	file, err := os.Open(mockConfig.snpPath)
	assert.NoError(t, err)
	defer func() {
		_ = file.Close()
	}()

	dec := gob.NewDecoder(file)
	decoded := make(map[Key]Value)
	for {
		var entry snapshotEntry
		if err := dec.Decode(&entry); err != nil {
			break
		}
		decoded[entry.Key] = Value{Data: entry.Data, Timestamp: entry.Timestamp, Tombstone: entry.Tombstone}
	}
	assert.Equal(t, mockData, decoded)

	assert.True(t, mw.clearCalled)
}

func TestPeriodicSnapshots(t *testing.T) {
	defer cleanupEngineMocks(t)

	mw := &mockWal{}
	callBack := func(_ *gob.Encoder) error { return nil }

	interval := 50 * time.Millisecond
	snp, err := newSnapshoter(mockConfig.snpPath, interval, mw, callBack)
	assert.NoError(t, err)

	snp.start()
	defer snp.stop()

	time.Sleep(150 * time.Millisecond)

	_, err = os.Stat(mockConfig.snpPath)
	assert.NoError(t, err, "Snapshot file should have been created by background task")
}

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
func (mw *mockWal) replay() (map[Key]Value, error)  { return nil, nil }
func (mw *mockWal) clear() error                             { mw.clearCalled = true; return nil }
func (mw *mockWal) stop()                                    {}
func (mw *mockWal) start()                                   {}

func TestNewSnapShotService(t *testing.T) {
	defer cleanupEngineMocks(t)

	mw := &mockWal{}
	callBack := func(_ *gob.Encoder) error { return nil }

	sss, err := newSnapshotService(mockConfig.sssPath, mockConfig.sssInterval, mw, callBack)
	assert.NoError(t, err)
	assert.NotNil(t, sss)
	assert.Equal(t, mockConfig.sssPath, sss.path)
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
	sss, _ := newSnapshotService(mockConfig.sssPath, mockConfig.sssInterval, mw, callBack)

	err := sss.create()
	assert.NoError(t, err)

	file, err := os.Open(mockConfig.sssPath)
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
	sss, err := newSnapshotService(mockConfig.sssPath, interval, mw, callBack)
	assert.NoError(t, err)

	sss.start()
	defer sss.stop()

	time.Sleep(150 * time.Millisecond)

	_, err = os.Stat(mockConfig.sssPath)
	assert.NoError(t, err, "Snapshot file should have been created by background task")
}

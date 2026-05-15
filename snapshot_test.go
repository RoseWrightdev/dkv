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

func (mw *mockWal) publish(msg proto.Message) error { return nil }
func (mw *mockWal) replay() (map[Key]Value, error)  { return nil, nil }
func (mw *mockWal) clear() error                    { mw.clearCalled = true; return nil }
func (mw *mockWal) stop()                           {}
func (mw *mockWal) start()                          {}

func cleanupSnapshotMock(t *testing.T) {
	err := os.Remove(mockConfig.sssPath)
	if err != nil && !os.IsNotExist(err) {
		assert.NoError(t, err)
	}
	err = os.Remove(mockConfig.sssPath + ".tmp")
	if err != nil && !os.IsNotExist(err) {
		assert.NoError(t, err)
	}
}

func TestNewSnapShotService(t *testing.T) {
	defer cleanupSnapshotMock(t)

	mw := &mockWal{}
	callBack := func(enc *gob.Encoder) error { return nil }

	sss, err := newSnapshotService(mockConfig.sssPath, mockConfig.sssInterval, mw, callBack)
	assert.NoError(t, err)
	assert.NotNil(t, sss)
	assert.Equal(t, mockConfig.sssPath, sss.path)
}

func TestCreateNewSnapShot(t *testing.T) {
	defer cleanupSnapshotMock(t)

	mw := &mockWal{}
	mockData := map[Key]Value{
		"user:1": []byte("alice"),
		"user:2": []byte("bob"),
	}
	callBack := func(enc *gob.Encoder) error {
		for k, v := range mockData {
			if err := enc.Encode(snapshotEntry{Key: k, Value: v}); err != nil {
				return err
			}
		}
		return nil
	}
	sss, _ := newSnapshotService(mockConfig.sssPath, mockConfig.sssInterval, mw, callBack)

	err := sss.createNewSnapShot()
	assert.NoError(t, err)

	file, err := os.Open(mockConfig.sssPath)
	assert.NoError(t, err)
	defer file.Close()

	dec := gob.NewDecoder(file)
	decoded := make(map[Key]Value)
	for {
		var entry snapshotEntry
		if err := dec.Decode(&entry); err != nil {
			break
		}
		decoded[entry.Key] = entry.Value
	}
	assert.Equal(t, mockData, decoded)

	assert.True(t, mw.clearCalled)
}

func TestPeriodicSnapshots(t *testing.T) {
	defer cleanupSnapshotMock(t)

	mw := &mockWal{}
	callBack := func(enc *gob.Encoder) error { return nil }

	interval := 50 * time.Millisecond
	sss, err := newSnapshotService(mockConfig.sssPath, interval, mw, callBack)
	assert.NoError(t, err)

	sss.start()
	defer sss.stop()

	time.Sleep(150 * time.Millisecond)

	_, err = os.Stat(mockConfig.sssPath)
	assert.NoError(t, err, "Snapshot file should have been created by background task")
}

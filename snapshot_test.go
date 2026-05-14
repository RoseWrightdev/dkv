package dkv

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

type MockWal struct {
	clearCalled bool
}

func (mw *MockWal) publish(msg proto.Message) error { return nil }
func (mw *MockWal) replay() (map[Key]Value, error)  { return nil, nil }
func (mw *MockWal) clear() error                    { mw.clearCalled = true; return nil }
func (mw *MockWal) stop()                           {}
func (mw *MockWal) start()                          {}

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

	mw := &MockWal{}
	callBack := func() map[Key]Value { return map[Key]Value{} }

	sss, err := newSnapshotService(mockConfig.sssPath, mockConfig.sssInterval, mw, callBack)
	assert.NoError(t, err)
	assert.NotNil(t, sss)
	assert.Equal(t, mockConfig.sssPath, sss.path)
}

func TestCreateNewSnapShot(t *testing.T) {
	defer cleanupSnapshotMock(t)

	mw := &MockWal{}
	mockData := map[Key]Value{
		"user:1": []byte("alice"),
		"user:2": []byte("bob"),
	}
	callBack := func() map[Key]Value { return mockData }
	sss, _ := newSnapshotService(mockConfig.sssPath, mockConfig.sssInterval, mw, callBack)

	err := sss.createNewSnapShot()
	assert.NoError(t, err)

	data, err := os.ReadFile(mockConfig.sssPath)
	assert.NoError(t, err)

	var decoded map[Key]Value
	err = json.Unmarshal(data, &decoded)
	assert.NoError(t, err)
	assert.Equal(t, mockData, decoded)

	assert.True(t, mw.clearCalled)
}

func TestPeriodicSnapshots(t *testing.T) {
	defer cleanupSnapshotMock(t)

	mw := &MockWal{}
	mockData := map[Key]Value{"count": []byte("1")}
	callBack := func() map[Key]Value { return mockData }

	interval := 50 * time.Millisecond
	sss, err := newSnapshotService(mockConfig.sssPath, interval, mw, callBack)
	assert.NoError(t, err)

	sss.start()
	defer sss.stop()

	time.Sleep(150 * time.Millisecond)

	_, err = os.Stat(mockConfig.sssPath)
	assert.NoError(t, err, "Snapshot file should have been created by background task")
}

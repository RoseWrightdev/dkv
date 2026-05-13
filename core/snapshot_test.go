package core

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

func (mw *MockWal) Publish(msg proto.Message) error { return nil }
func (mw *MockWal) Replay() (*map[Key]Value, error) { return nil, nil }
func (mw *MockWal) clear() error                    { mw.clearCalled = true; return nil }

func cleanupSnapshotMock(t *testing.T) {
	err := os.Remove(MOCK_SSS_PATH)
	if err != nil && !os.IsNotExist(err) {
		assert.NoError(t, err)
	}
	err = os.Remove(MOCK_SSS_PATH + ".tmp")
	if err != nil && !os.IsNotExist(err) {
		assert.NoError(t, err)
	}
}

func TestNewSnapShotService(t *testing.T) {
	defer cleanupSnapshotMock(t)

	mw := &MockWal{}
	callBack := func() *map[Key]Value { return &map[Key]Value{} }

	sss, err := newSnapshotService(MOCK_SSS_PATH, MOCK_SSS_INTERVAL, mw, callBack)
	assert.NoError(t, err)
	assert.NotNil(t, sss)
	assert.Equal(t, MOCK_SSS_PATH, sss.path)
}

func TestCreateNewSnapShot(t *testing.T) {
	defer cleanupSnapshotMock(t)

	mw := &MockWal{}
	mockData := map[Key]Value{
		"user:1": []byte("alice"),
		"user:2": []byte("bob"),
	}
	callBack := func() *map[Key]Value { return &mockData }
	sss, _ := newSnapshotService(MOCK_SSS_PATH, MOCK_SSS_INTERVAL, mw, callBack)

	err := sss.createNewSnapShot()
	assert.NoError(t, err)

	data, err := os.ReadFile(MOCK_SSS_PATH)
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
	callBack := func() *map[Key]Value { return &mockData }

	interval := 50 * time.Millisecond
	sss, err := newSnapshotService(MOCK_SSS_PATH, interval, mw, callBack)
	assert.NoError(t, err)

	sss.Start()
	defer sss.Stop()

	time.Sleep(150 * time.Millisecond)

	_, err = os.Stat(MOCK_SSS_PATH)
	assert.NoError(t, err, "Snapshot file should have been created by background task")
}

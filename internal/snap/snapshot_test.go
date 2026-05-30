package snap

import (
	"encoding/gob"
	"os"
	"testing"
	"time"

	"github.com/rosewrightdev/dkv/kv"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

var (
	mockSnpPath     = "test_snapshot.bin"
	mockSnpInterval = 500 * time.Millisecond
)

func cleanupSnp(_ *testing.T) {
	_ = os.Remove(mockSnpPath)
	_ = os.Remove(mockSnpPath + ".tmp")
}

type mockWal struct {
	clearCalled bool
}

func (mw *mockWal) Publish(_ kv.Key, _ kv.HashKey, _ proto.Message) error { return nil }
func (mw *mockWal) Replay() (map[kv.Key]kv.Value, error)                  { return nil, nil }
func (mw *mockWal) Clear(_ []int64) error                                 { mw.clearCalled = true; return nil }
func (mw *mockWal) PrepareSnapshot() ([]int64, error)                     { return nil, nil }
func (mw *mockWal) Stop()                                                 {}
func (mw *mockWal) Start()                                                {}

func TestNewSnapshotter(t *testing.T) {
	defer cleanupSnp(t)

	mw := &mockWal{}
	callBack := func(_ *gob.Encoder) error { return nil }

	snp, err := NewSnapshotter(mockSnpPath, mockSnpInterval, mw, callBack)
	assert.NoError(t, err)
	assert.NotNil(t, snp)
	assert.Equal(t, mockSnpPath, snp.path)
}

func TestCreateNewSnapShot(t *testing.T) {
	defer cleanupSnp(t)

	mw := &mockWal{}
	mockData := map[kv.Key]kv.Value{
		"user:1": {Data: []byte("alice"), Timestamp: 100},
		"user:2": {Data: []byte("bob"), Timestamp: 100},
	}
	callBack := func(enc *gob.Encoder) error {
		for k, v := range mockData {
			if err := enc.Encode(SnapshotEntry{Key: k, Data: v.Data, Timestamp: v.Timestamp, Tombstone: v.Tombstone}); err != nil {
				return err
			}
		}
		return nil
	}
	snp, _ := NewSnapshotter(mockSnpPath, mockSnpInterval, mw, callBack)

	err := snp.Create()
	assert.NoError(t, err)

	file, err := os.Open(mockSnpPath)
	assert.NoError(t, err)
	defer func() {
		_ = file.Close()
	}()

	dec := gob.NewDecoder(file)
	decoded := make(map[kv.Key]kv.Value)
	for {
		var entry SnapshotEntry
		if err := dec.Decode(&entry); err != nil {
			break
		}
		decoded[entry.Key] = kv.Value{Data: entry.Data, Timestamp: entry.Timestamp, Tombstone: entry.Tombstone}
	}
	assert.Equal(t, mockData, decoded)

	assert.True(t, mw.clearCalled)
}

func TestPeriodicSnapshots(t *testing.T) {
	defer cleanupSnp(t)

	mw := &mockWal{}
	callBack := func(_ *gob.Encoder) error { return nil }

	interval := 50 * time.Millisecond
	snp, err := NewSnapshotter(mockSnpPath, interval, mw, callBack)
	assert.NoError(t, err)

	snp.Start()
	defer snp.Stop()

	time.Sleep(150 * time.Millisecond)

	_, err = os.Stat(mockSnpPath)
	assert.NoError(t, err, "Snapshot file should have been created by background task")
}

type errorWal struct {
	mockWal
	prepErr error
}

func (ew *errorWal) PrepareSnapshot() ([]int64, error) {
	if ew.prepErr != nil {
		return nil, ew.prepErr
	}
	return nil, nil
}

func TestSnapshot_ExtraEdgeCases(t *testing.T) {
	// 1. queueSnapShot skip / default path
	mw := &mockWal{}
	snp, err := NewSnapshotter("testpath", time.Hour, mw, func(_ *gob.Encoder) error { return nil })
	assert.NoError(t, err)

	// fill the queue
	snp.ch <- struct{}{}
	// queue again - should hit default: no-op branch
	snp.queueSnapShot()
	// clean up channel
	<-snp.ch

	// 2. prepareSnapshot error
	ew := &errorWal{prepErr: assert.AnError}
	snpErr, err := NewSnapshotter("testpath", time.Hour, ew, func(_ *gob.Encoder) error { return nil })
	assert.NoError(t, err)
	err = snpErr.Create()
	assert.Error(t, err)
	assert.Equal(t, assert.AnError, err)

	// 3. os.Create error (using invalid path)
	snpCreateErr, err := NewSnapshotter("/nonexistent-path-1234/file.snp", time.Hour, mw, func(_ *gob.Encoder) error { return nil })
	assert.NoError(t, err)
	err = snpCreateErr.Create()
	assert.Error(t, err)

	// 4. encCallBack error
	snpEncErr, err := NewSnapshotter(mockSnpPath, time.Hour, mw, func(_ *gob.Encoder) error {
		return assert.AnError
	})
	assert.NoError(t, err)
	err = snpEncErr.Create()
	assert.Error(t, err)
	assert.Equal(t, assert.AnError, err)
}

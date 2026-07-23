package writer

import (
	"errors"
	"testing"

	pb "github.com/rosewrightdev/dkv/api"
	"github.com/rosewrightdev/dkv/core/hashmap"
	"github.com/rosewrightdev/dkv/kv"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

type mockWal struct {
	clearCalled bool
}

func (mw *mockWal) Publish(_ kv.Key, _ kv.HashKey, _ proto.Message) error { return nil }
func (mw *mockWal) Replay() (map[kv.Key]kv.Value, error)                  { return nil, nil }
func (mw *mockWal) Clear(_ []int64) error                                 { mw.clearCalled = true; return nil }
func (mw *mockWal) PrepareSnapshot() ([]int64, error)                     { return nil, nil }
func (mw *mockWal) Stop()                                                 {}
func (mw *mockWal) Start()                                                {}

type mockStorageWal struct {
	pubErr error
	mockWal
}

func (m *mockStorageWal) Publish(_ kv.Key, _ kv.HashKey, _ proto.Message) error {
	return m.pubErr
}

type mockStorageClock struct {
	updateTs int64
}

func (m *mockStorageClock) Now() int64 {
	return 0
}

func (m *mockStorageClock) Update(ts int64) {
	m.updateTs = ts
}

func TestStorageWriter_All(t *testing.T) {
	hm := hashmap.NewShardedMap()
	wal := &mockStorageWal{}
	clock := &mockStorageClock{}

	sw := NewStorageWriter(hm, wal, clock)

	// ApplySet success and WAL error
	reqSet := &pb.SetRequest{
		Key:       "user:1",
		Value:     []byte("val1"),
		Timestamp: 100,
		NodeId:    "node-1",
	}
	assert.NoError(t, sw.ApplySet(reqSet))
	assert.Equal(t, int64(100), clock.updateTs)

	// WAL error
	wal.pubErr = errors.New("wal publish error")
	reqSet2 := &pb.SetRequest{
		Key:       "user:1",
		Value:     []byte("val2"),
		Timestamp: 101,
		NodeId:    "node-1",
	}
	err := sw.ApplySet(reqSet2)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to persist set to WAL")

	// LWW stale update (should be ignored, returning nil without calling WAL)
	wal.pubErr = errors.New("should not be called")
	reqStale := &pb.SetRequest{
		Key:       "user:1",
		Value:     []byte("stale"),
		Timestamp: 90,
		NodeId:    "node-1",
	}
	assert.NoError(t, sw.ApplySet(reqStale))
	wal.pubErr = nil

	// ApplyDelete success and WAL error
	reqDel := &pb.DeleteRequest{
		Key:       "user:1",
		Timestamp: 105,
		NodeId:    "node-1",
	}
	assert.NoError(t, sw.ApplyDelete(reqDel))

	// WAL error on delete
	wal.pubErr = errors.New("wal publish error")
	reqDel2 := &pb.DeleteRequest{
		Key:       "user:1",
		Timestamp: 106,
		NodeId:    "node-1",
	}
	err = sw.ApplyDelete(reqDel2)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to persist delete to WAL")

	// LWW stale delete
	wal.pubErr = errors.New("should not be called")
	reqStaleDel := &pb.DeleteRequest{
		Key:       "user:1",
		Timestamp: 95,
		NodeId:    "node-1",
	}
	assert.NoError(t, sw.ApplyDelete(reqStaleDel))
}

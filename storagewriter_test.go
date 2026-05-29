package dkv

import (
	"errors"
	"testing"

	pb "github.com/rosewrightdev/dkv/api"
	"github.com/rosewrightdev/dkv/kv"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

type mockStorageWal struct {
	mockWal
	pubErr error
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
	// Setup dependencies
	hm := newShardedMap()
	wal := &mockStorageWal{}
	clock := &mockStorageClock{}
	mesh := &MockMesher{
		Owners: []NodeID{"node-1"},
	}
	mc := &MeshConfig{
		NodeID:            "node-1",
		ReplicationFactor: 1,
	}

	sw := newStorageWriter(hm, wal, clock, mesh, mc)

	// 1. isLocal tests
	// ReplicationFactor <= 0 branch
	mc.ReplicationFactor = 0
	assert.True(t, sw.isLocal("anykey"))

	mc.ReplicationFactor = -2
	assert.True(t, sw.isLocal("anykey"))

	// SingleNode = true branch
	mc.SingleNode = true
	assert.True(t, sw.isLocal("anykey"))
	mc.SingleNode = false // restore
	mc.ReplicationFactor = 1

	// Not local branch
	mesh.Owners = []NodeID{"other-node"}
	assert.False(t, sw.isLocal("anykey"))
	mesh.Owners = []NodeID{"node-1"} // restore

	// 2. ApplySet success and WAL error
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
	// Make sure LWW accepts the store (higher timestamp)
	reqSet2 := &pb.SetRequest{
		Key:       "user:1",
		Value:     []byte("val2"),
		Timestamp: 101,
		NodeId:    "node-1",
	}
	err := sw.ApplySet(reqSet2)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to persist gossip set to WAL")

	// LWW stale update (should be ignored, returning nil without calling WAL)
	wal.pubErr = errors.New("should not be called")
	reqStale := &pb.SetRequest{
		Key:       "user:1",
		Value:     []byte("stale"),
		Timestamp: 90,
		NodeId:    "node-1",
	}
	assert.NoError(t, sw.ApplySet(reqStale))

	// Not responsible key (should return nil without calling WAL)
	mesh.Owners = []NodeID{"other-node"}
	assert.NoError(t, sw.ApplySet(reqSet2))
	mesh.Owners = []NodeID{"node-1"} // restore
	wal.pubErr = nil                 // restore

	// 3. ApplyDelete success and WAL error
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
	assert.Contains(t, err.Error(), "failed to persist gossip delete to WAL")

	// LWW stale delete
	wal.pubErr = errors.New("should not be called")
	reqStaleDel := &pb.DeleteRequest{
		Key:       "user:1",
		Timestamp: 95,
		NodeId:    "node-1",
	}
	assert.NoError(t, sw.ApplyDelete(reqStaleDel))

	// Not responsible delete
	mesh.Owners = []NodeID{"other-node"}
	assert.NoError(t, sw.ApplyDelete(reqDel2))
}

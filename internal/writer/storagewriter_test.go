package writer

import (
	"errors"
	"testing"

	pb "github.com/rosewrightdev/dkv/api"
	"github.com/rosewrightdev/dkv/internal/hashmap"
	"github.com/rosewrightdev/dkv/internal/mesh"
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

type mockMesher struct {
	AddrMap map[kv.NodeID]mesh.PeerAddress
	Owners  []kv.NodeID
}

func (m *mockMesher) Broadcast(_ []byte)          {}
func (m *mockMesher) Members() []mesh.PeerAddress { return nil }
func (m *mockMesher) Owner(_ kv.Key) kv.NodeID {
	if len(m.Owners) > 0 {
		return m.Owners[0]
	}
	return ""
}
func (m *mockMesher) GetOwners(_ kv.Key, _ int) []kv.NodeID {
	return m.Owners
}
func (m *mockMesher) PutOwners(_ []kv.NodeID) {}
func (m *mockMesher) AddressForNode(nodeID kv.NodeID) mesh.PeerAddress {
	return m.AddrMap[nodeID]
}
func (m *mockMesher) Start() error            { return nil }
func (m *mockMesher) Stop() error             { return nil }
func (m *mockMesher) UpdateLocalWeight(_ int) {}

func TestStorageWriter_All(t *testing.T) {
	// Setup dependencies
	hm := hashmap.NewShardedMap()
	wal := &mockStorageWal{}
	clock := &mockStorageClock{}
	meshObj := &mockMesher{
		Owners: []kv.NodeID{"node-1"},
	}
	mc := &mesh.Config{
		NodeID:            "node-1",
		ReplicationFactor: 1,
	}

	sw := NewStorageWriter(hm, wal, clock, meshObj, mc)

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
	meshObj.Owners = []kv.NodeID{"other-node"}
	assert.False(t, sw.isLocal("anykey"))
	meshObj.Owners = []kv.NodeID{"node-1"} // restore

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
	meshObj.Owners = []kv.NodeID{"other-node"}
	assert.NoError(t, sw.ApplySet(reqSet2))
	meshObj.Owners = []kv.NodeID{"node-1"} // restore
	wal.pubErr = nil                       // restore

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
	meshObj.Owners = []kv.NodeID{"other-node"}
	assert.NoError(t, sw.ApplyDelete(reqDel2))
}

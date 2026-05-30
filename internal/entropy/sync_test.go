package entropy

import (
	"errors"
	"fmt"
	"testing"
	"time"

	pb "github.com/rosewrightdev/dkv/api"
	"github.com/rosewrightdev/dkv/gateway"
	"github.com/rosewrightdev/dkv/internal/hashmap"
	"github.com/rosewrightdev/dkv/internal/mesh"
	"github.com/rosewrightdev/dkv/kv"
	"github.com/rosewrightdev/dkv/security"
	"github.com/stretchr/testify/assert"
)

func TestSync_PreparePullRequestDataRace(t *testing.T) {
	hm := hashmap.NewShardedMap()
	cc := gateway.NewClientCache(nil)

	syn := NewSyncer(&SyncerConfig{
		NodeID:     "node-1",
		Writer:     &mockStateTransferWriter{hm: hm},
		Mesh:       &mesh.NopMesh{},
		MeshConfig: &mesh.MeshConfig{NodeID: "node-1", SingleNode: true},
		Hm:         hm,
		Interval:   10 * time.Second,
		Creds:      nil,
		Cc:         cc,
	})

	stop := make(chan struct{})

	// Start the writer goroutine that keeps modifying/filling the map.
	go func() {
		i := 0
		for {
			select {
			case <-stop:
				return
			default:
				key := fmt.Sprintf("racekey-%d", i)
				hm.StoreLWW(key, security.HashFunc(key), kv.Value{Data: []byte("val"), Timestamp: time.Now().UnixNano()})
				m := syn.pools.bucketMaps.Get().(map[hashmap.ShardID]hashmap.ShardDigest)
				hm.FillDigests(m)
				syn.pools.bucketMaps.Put(m)
				i++
			}
		}
	}()

	// Main goroutine constantly prepares requests and reads the digests concurrently.
	for range 100 {
		req := syn.pools.pullRequests.Get().(*pb.PullRequest)
		syn.preparePullRequest(req)

		// Concurrently read the request digests that reference the pool's slices.
		for _, sd := range req.SubDigests {
			for _, h := range sd.SubHashes {
				_ = h
			}
		}

		syn.cleanupPullRequest(req)
	}

	close(stop)
}

type mockStateTransferWriter struct {
	hm        *hashmap.ShardedMap
	setErr    error
	deleteErr error
}

func (m *mockStateTransferWriter) ApplySet(req *pb.SetRequest) error {
	if m.setErr != nil {
		return m.setErr
	}
	if m.hm != nil {
		m.hm.StoreLWW(req.Key, security.HashFunc(req.Key), kv.Value{Data: req.Value, Timestamp: req.Timestamp})
	}
	return nil
}

func (m *mockStateTransferWriter) ApplyDelete(req *pb.DeleteRequest) error {
	if m.deleteErr != nil {
		return m.deleteErr
	}
	if m.hm != nil {
		m.hm.StoreLWW(req.Key, security.HashFunc(req.Key), kv.Value{Timestamp: req.Timestamp, Tombstone: true})
	}
	return nil
}

func TestSyncer_ExtraEdgeCases(t *testing.T) {
	cc := gateway.NewClientCache(nil)

	// 1. NewSyncer panics
	assert.Panics(t, func() {
		NewSyncer(&SyncerConfig{Cc: cc})
	})
	assert.Panics(t, func() {
		NewSyncer(&SyncerConfig{Mesh: &mesh.NopMesh{}})
	})

	// 2. start panic when interval <= 0
	synPanic := NewSyncer(&SyncerConfig{
		Mesh:     &mesh.NopMesh{},
		Cc:       cc,
		Interval: 0,
	})
	assert.Panics(t, func() {
		synPanic.Start()
	})

	// 3. stop idempotent check
	synStop := NewSyncer(&SyncerConfig{
		Mesh: &mesh.NopMesh{},
		Cc:   cc,
	})
	synStop.Stop()
	synStop.Stop() // should not panic/block

	// 4. push failures
	swErr := &mockStateTransferWriter{
		setErr:    errors.New("set push err"),
		deleteErr: errors.New("del push err"),
	}
	synPush := NewSyncer(&SyncerConfig{
		Mesh:   &mesh.NopMesh{},
		Cc:     cc,
		Writer: swErr,
	})
	err := synPush.Push([]*pb.SetRequest{{}}, nil)
	assert.Error(t, err)

	err = synPush.Push(nil, []*pb.DeleteRequest{{}})
	assert.Error(t, err)

	// 5. pull when root hash matches
	hm := hashmap.NewShardedMap()
	synPull := NewSyncer(&SyncerConfig{
		Mesh: &mesh.NopMesh{},
		Cc:   cc,
		Hm:   hm,
	})
	sets, dels, err := synPull.Pull(&PullConfig{Root: hm.RootDigest()})
	assert.NoError(t, err)
	assert.Nil(t, sets)
	assert.Nil(t, dels)

	// 6. performSync when mesh.Members() is empty
	synPerf := NewSyncer(&SyncerConfig{
		Mesh:       &MockMesher{},
		Cc:         cc,
		MeshConfig: &mesh.MeshConfig{},
	})
	synPerf.performSync() // should return early without panicking

	// 7. buildDeleteRequest coverage!
	// Populate map with a delete entry
	hm.StoreLWW("user:deleted", security.HashFunc("user:deleted"), kv.Value{Timestamp: 500, Tombstone: true})
	synDel := NewSyncer(&SyncerConfig{
		Mesh:       &MockMesher{Owners: []kv.NodeID{"node-1"}},
		Cc:         cc,
		Hm:         hm,
		MeshConfig: &mesh.MeshConfig{NodeID: "node-1", ReplicationFactor: 1},
	})

	// Prepare pull config
	pc := &PullConfig{
		Shards:      make(map[hashmap.ShardID]hashmap.Digest),
		Buckets:     make(map[hashmap.ShardID]hashmap.ShardDigest),
		RequesterID: "node-1",
		Root:        0, // force mismatch
	}
	// We want to trigger mismatch. Let's call Pull
	_, dels, err = synDel.Pull(pc)
	assert.NoError(t, err)
	assert.Len(t, dels, 1)
	assert.Equal(t, "user:deleted", dels[0].Key)
	assert.Equal(t, int64(500), dels[0].Timestamp)
}

type MockMesher struct {
	Owners  []kv.NodeID
	AddrMap map[kv.NodeID]mesh.PeerAddress
}

func (m *MockMesher) Broadcast(_ []byte) {}

func (m *MockMesher) Members() []mesh.PeerAddress {
	return nil
}

func (m *MockMesher) Owner(_ kv.Key) kv.NodeID {
	if len(m.Owners) > 0 {
		return m.Owners[0]
	}
	return ""
}

func (m *MockMesher) GetOwners(_ kv.Key, _ int) []kv.NodeID {
	return m.Owners
}

func (m *MockMesher) PutOwners(_ []kv.NodeID) {}

func (m *MockMesher) AddressForNode(nodeID kv.NodeID) mesh.PeerAddress {
	return m.AddrMap[nodeID]
}

func (m *MockMesher) Start() error {
	return nil
}

func (m *MockMesher) Stop() error {
	return nil
}

func (m *MockMesher) UpdateLocalWeight(_ int) {}

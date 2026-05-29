package mesh

import (
	"sync"
	"testing"
	"time"

	"github.com/rosewrightdev/dkv/kv"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockGossip struct{}

func newMockGossip() *mockGossip                { return &mockGossip{} }
func (mg *mockGossip) OnGossip(_ []byte)      {}
func (mg *mockGossip) ExportState() []byte      { return []byte("") }
func (mg *mockGossip) ImportState(_ []byte) {}

func TestClusterMembership(t *testing.T) {
	// Start first node
	c1 := MeshConfig{
		NodeID:   "node1",
		BindPort: 7001,
		GrpcPort: 8001,
	}
	mg1 := newMockGossip()
	s1, err := NewMesh(mg1, mg1, c1)
	require.NoError(t, err)
	defer func() {
		_ = s1.Stop()
	}()

	// Start second node and join first
	c2 := MeshConfig{
		NodeID:    "node2",
		BindPort:  7002,
		SeedNodes: []string{"127.0.0.1:7001"},
		GrpcPort:  8002,
	}
	mg2 := newMockGossip()
	s2, err := NewMesh(mg2, mg2, c2)
	require.NoError(t, err)
	defer func() {
		_ = s2.Stop()
	}()

	err = s2.Start()
	require.NoError(t, err)

	// Wait for convergence
	time.Sleep(200 * time.Millisecond)

	members := s1.Members()
	assert.GreaterOrEqual(t, len(members), 2)

	hasPort := func(list []PeerAddress, port string) bool {
		for _, m := range list {
			s := string(m)
			if len(s) >= len(port) && s[len(s)-len(port):] == port {
				return true
			}
		}
		return false
	}

	assert.True(t, hasPort(members, ":8001"), "Members should contain node on gRPC port 8001")
	assert.True(t, hasPort(members, ":8002"), "Members should contain node on gRPC port 8002")
}

func TestMesher_ConcurrentStop(t *testing.T) {
	c1 := MeshConfig{
		NodeID:   "node1",
		BindPort: 7003,
		GrpcPort: 8003,
	}

	mg := newMockGossip()
	s1, err := NewMesh(mg, mg, c1)
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for range 50 {
			_ = s1.Members()
			_ = s1.AddressForNode("node1")
			time.Sleep(1 * time.Millisecond)
		}
	}()

	go func() {
		defer wg.Done()
		time.Sleep(5 * time.Millisecond)
		_ = s1.Stop()
	}()

	wg.Wait()
}

type trackingExchanger struct {
	exported bool
	imported []byte
}

func (te *trackingExchanger) ExportState() []byte {
	te.exported = true
	return []byte("exported-state")
}

func (te *trackingExchanger) ImportState(b []byte) {
	te.imported = b
}

type trackingGossiper struct {
	received []byte
}

func (tg *trackingGossiper) OnGossip(b []byte) {
	tg.received = b
}

func TestMesh_DelegateCallbacks(t *testing.T) {
	cfg := MeshConfig{
		NodeID:   "test-delegate-node",
		BindPort: 7005,
		GrpcPort: 8005,
	}
	ex := &trackingExchanger{}
	gs := &trackingGossiper{}

	m, err := NewMesh(gs, ex, cfg)
	require.NoError(t, err)
	defer func() {
		_ = m.Stop()
	}()

	// 1. NodeMeta
	meta := m.NodeMeta(0)
	assert.Equal(t, []byte("8005"), meta)

	// 2. NotifyMsg
	m.NotifyMsg([]byte("gossip-payload"))
	assert.Equal(t, []byte("gossip-payload"), gs.received)

	// 3. LocalState
	state := m.LocalState(false)
	assert.Equal(t, []byte("exported-state"), state)
	assert.True(t, ex.exported)

	// 4. MergeRemoteState
	m.MergeRemoteState([]byte("incoming-state"), false)
	assert.Equal(t, []byte("incoming-state"), ex.imported)

	// 5. NotifyUpdate
	m.NotifyUpdate(nil) // should be no-op

	// 6. Broadcast logic
	m.Broadcast([]byte("broadcast-msg"))
	bcList := m.GetBroadcasts(0, 100)
	assert.NotEmpty(t, bcList)

	// Cover broadcast struct methods
	bcStruct := &broadcast{msg: []byte("bc")}
	assert.False(t, bcStruct.Invalidates(nil))
	assert.Equal(t, []byte("bc"), bcStruct.Message())
	bcStruct.Finished()
}

func TestNopMesh(t *testing.T) {
	n := &NopMesh{}
	n.Broadcast([]byte("noop"))
	assert.Nil(t, n.Members())
	assert.Equal(t, kv.NodeID(""), n.Owner(kv.Key("key")))
	assert.Nil(t, n.GetOwners(kv.Key("key"), 3))
	n.PutOwners(nil)
	assert.Equal(t, PeerAddress(""), n.AddressForNode(kv.NodeID("node")))
	assert.NoError(t, n.Start())
	assert.NoError(t, n.Stop())
}

func TestMesh_ExtraEdgeCases(t *testing.T) {
	// 1. NewMesh with invalid config to force failure
	cfg := MeshConfig{
		BindAddr: "invalid-ip-address!!!",
		BindPort: -100,
	}
	mg := newMockGossip()
	m, err := NewMesh(mg, mg, cfg)
	assert.Error(t, err)
	assert.Nil(t, m)

	// 2. stop with nil memberList
	mNil := &Mesh{}
	assert.NoError(t, mNil.Stop())

	// 3. start join failure
	cfgJoin := MeshConfig{
		NodeID:    "test-join-fail",
		BindPort:  7099,
		SeedNodes: []string{"0.0.0.0:0"}, // invalid / unreachable seed
	}
	mJoin, err := NewMesh(mg, mg, cfgJoin)
	require.NoError(t, err)
	defer func() {
		_ = mJoin.Stop()
	}()
	err = mJoin.Start()
	assert.Error(t, err)

	// 4. AddressForNode when stopped or nil memberList
	assert.Empty(t, mNil.AddressForNode("some-node"))
	assert.Empty(t, mNil.Members())
	assert.Nil(t, mNil.LocalState(false))
}

package dkv

import (
	"sync"
	"testing"
	"time"

	pb "github.com/rosewrightdev/dkv/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockGossip struct{}

func newMockGossip() *mockGossip                               { return &mockGossip{} }
func (mg *mockGossip) onMessage(data []byte)                   {}
func (mg *mockGossip) applySet(req *pb.SetRequest) error       { return nil }
func (mg *mockGossip) applyDelete(req *pb.DeleteRequest) error { return nil }
func (mg *mockGossip) getLocalState() []byte                   { return []byte("") }
func (mg *mockGossip) mergeRemoteState(buf []byte)             {}

func TestClusterMembership(t *testing.T) {
	// Start first node
	c1 := MeshConfig{
		NodeID:   "node1",
		BindPort: 7001,
		GrpcPort: 8001,
	}
	s1, err := newMesh(newMockGossip(), c1)
	require.NoError(t, err)
	defer func() {
		_ = s1.stop()
	}()

	// Start second node and join first
	c2 := MeshConfig{
		NodeID:    "node2",
		BindPort:  7002,
		SeedNodes: []string{"127.0.0.1:7001"},
		GrpcPort:  8002,
	}
	s2, err := newMesh(newMockGossip(), c2)
	require.NoError(t, err)
	defer func() {
		_ = s2.stop()
	}()

	err = s2.start()
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

	s1, err := newMesh(newMockGossip(), c1)
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
		_ = s1.stop()
	}()

	wg.Wait()
}

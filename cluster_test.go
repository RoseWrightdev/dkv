package dkv

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClusterMembership(t *testing.T) {
	// Start first node
	c1 := ClusterConfig{
		NodeID: "node1",
		BindPort: 7001,
		GrpcPort: 8001,
	}
	noOp := func() []byte { return nil }
	noOpMerge := func([]byte) {}
	s1, err := newClusterService(c1, func([]byte) {}, noOp, noOpMerge)
	require.NoError(t, err)
	defer func() {
		_ = s1.stop()
	}()

	// Start second node and join first
	c2 := ClusterConfig{
		NodeID:  "node2",
		BindPort:  7002,
		SeedNodes: []string{"127.0.0.1:7001"},
		GrpcPort:  8002,
	}
	s2, err := newClusterService(c2, func([]byte) {}, noOp, noOpMerge)
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

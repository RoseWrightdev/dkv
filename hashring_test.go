package dkv

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHashRing_Basic(t *testing.T) {
	ring := NewHashRing()

	// 1. Verify empty ring returns defaults
	assert.Empty(t, ring.GetNode("key1"))
	assert.Nil(t, ring.GetOwners("key1", 3))

	// 2. Add nodes
	ring.AddNode("node1")
	ring.AddNode("node2")
	ring.AddNode("node3")

	// Ensure unique nodes retrieval
	nodes := ring.GetNodes()
	assert.Len(t, nodes, 3)
	assert.Contains(t, nodes, NodeID("node1"))
	assert.Contains(t, nodes, NodeID("node2"))
	assert.Contains(t, nodes, NodeID("node3"))

	// 3. Verify single owner lookup
	owner := ring.GetNode("some-user-key-12345")
	assert.NotEmpty(t, owner)
	assert.Contains(t, []NodeID{"node1", "node2", "node3"}, owner)

	// 4. Verify multiple replica lookup
	owners := ring.GetOwners("some-user-key-12345", 2)
	assert.Len(t, owners, 2)
	assert.NotEqual(t, owners[0], owners[1], "Replicas must be distinct physical nodes")

	// 5. Verify lookup wrap-around and duplicate node suppression
	// Requesting more replicas than available nodes should return at most the total number of unique nodes.
	allOwners := ring.GetOwners("some-user-key-12345", 5)
	assert.Len(t, allOwners, 3, "Should return only the unique available nodes even if replication factor requested is higher")
}

func TestHashRing_Removal(t *testing.T) {
	ring := NewHashRing()
	ring.AddNode("node1")
	ring.AddNode("node2")

	// Check owner is consistently resolved
	key := "persistent-routing-key"
	owner1 := ring.GetNode(key)
	assert.Contains(t, []NodeID{"node1", "node2"}, owner1)

	// Add another node
	ring.AddNode("node3")
	
	// Remove node1
	ring.RemoveNode("node1")
	nodes := ring.GetNodes()
	assert.Len(t, nodes, 2)
	assert.NotContains(t, nodes, NodeID("node1"))

	// Ensure we still resolve keys
	owner2 := ring.GetNode(key)
	assert.NotEmpty(t, owner2)
	assert.NotEqual(t, NodeID("node1"), owner2)
}

func TestHashRing_ClockwiseOrderAndWrap(t *testing.T) {
	ring := NewHashRing()
	for i := range 5 {
		ring.AddNode(NodeID(fmt.Sprintf("node-%d", i)))
	}

	// Fetch all 5 unique nodes as replicas
	owners := ring.GetOwners("test-wrap-key", 5)
	assert.Len(t, owners, 5)

	// Verify they are distinct
	seen := make(map[NodeID]bool)
	for _, node := range owners {
		assert.False(t, seen[node], "Replica set should not have duplicate physical nodes")
		seen[node] = true
	}
}

func TestHashRing_PoolCorrectness(t *testing.T) {
	ring := NewHashRing()
	ring.AddNode("node1")

	// Get a slice from the pool via the public API GetOwners.
	owners := ring.GetOwners("key", 1)
	assert.Len(t, owners, 1)
	assert.Equal(t, NodeID("node1"), owners[0])

	// Recycle it.
	ring.PutOwners(owners)

	// Get an item directly from the pool and verify it is a direct slice, not a pointer.
	pooledVal := ring.ownersSlicePool.Get()
	slice, ok := pooledVal.([]NodeID)
	assert.True(t, ok, "The pooled value must be a direct []NodeID slice")
	assert.Equal(t, 0, len(slice), "The pooled slice should have length 0")
	assert.GreaterOrEqual(t, cap(slice), 8, "The pooled slice should have capacity of at least 8")
}



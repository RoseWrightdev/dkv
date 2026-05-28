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

func TestHashRing_AddNodes(t *testing.T) {
	t.Run("Bulk insert matches sequential insert", func(t *testing.T) {
		// Build a ring using individual AddNode calls
		ringA := NewHashRing()
		ringA.AddNode("alpha")
		ringA.AddNode("beta")
		ringA.AddNode("gamma")

		// Build a ring using bulk AddNodes
		ringB := NewHashRing()
		ringB.AddNodes([]NodeID{"alpha", "beta", "gamma"})

		// Both rings should own the same nodes
		nodesA := ringA.GetNodes()
		nodesB := ringB.GetNodes()
		assert.ElementsMatch(t, nodesA, nodesB)

		// Routing should be consistent for both rings
		keys := []Key{"test-key-1", "test-key-2", "routing-key-abc", "another-routing-key"}
		for _, k := range keys {
			assert.Equal(t, ringA.GetNode(k), ringB.GetNode(k),
				"AddNodes should produce the same routing as sequential AddNode for key %s", k)
		}
	})

	t.Run("AddNodes skips duplicates", func(t *testing.T) {
		ring := NewHashRing()
		ring.AddNode("existing-node")
		ring.AddNodes([]NodeID{"existing-node", "new-node"})

		nodes := ring.GetNodes()
		assert.Len(t, nodes, 2, "Duplicate node should not be inserted twice")
		assert.Contains(t, nodes, NodeID("existing-node"))
		assert.Contains(t, nodes, NodeID("new-node"))
	})

	t.Run("AddNodes on empty ring", func(t *testing.T) {
		ring := NewHashRing()
		ring.AddNodes([]NodeID{"node-x", "node-y"})

		nodes := ring.GetNodes()
		assert.Len(t, nodes, 2)

		owner := ring.GetNode("any-routing-key")
		assert.Contains(t, nodes, owner)
	})

	t.Run("AddNodes with empty slice is a no-op", func(t *testing.T) {
		ring := NewHashRing()
		ring.AddNode("solo")
		ring.AddNodes([]NodeID{})

		nodes := ring.GetNodes()
		assert.Len(t, nodes, 1)
	})
}

func TestHashRing_PutOwners(t *testing.T) {
	ring := NewHashRing()
	ring.AddNode("node1")
	ring.AddNode("node2")

	owners := ring.GetOwners("test-key", 2)
	assert.Len(t, owners, 2)

	// PutOwners is a no-op; calling it should not panic or corrupt the ring
	ring.PutOwners(owners)
	ring.PutOwners(nil)

	// Ring still works after PutOwners
	owner := ring.GetNode("test-key")
	assert.NotEmpty(t, owner)
}

func TestHashRing_ExtraEdgeCases(t *testing.T) {
	ring := NewHashRing()

	// 1. AddNode duplicate
	ring.AddNode("node-dup")
	ring.AddNode("node-dup")
	assert.Len(t, ring.GetNodes(), 1)

	// 2. RemoveNode non-existent
	ring.RemoveNode("non-existent")
	assert.Len(t, ring.GetNodes(), 1)

	// 3. GetNode wrap-around: find a key whose hash is greater than all vnodes
	// We can add a node, find the maximum hash in its vnodes, and then find a key that hashes higher than that.
	// Since sha256 hashes are pseudo-random, we can just try a bunch of keys until we find one with a hash larger than the maximum vnode hash.
	maxHash := uint64(0)
	for _, vn := range ring.vnodes {
		if vn.hash > maxHash {
			maxHash = vn.hash
		}
	}

	// Find a key that wraps around
	var wrapKey string
	for i := 0; i < 10000; i++ {
		k := fmt.Sprintf("wrap-candidate-%d", i)
		h := ring.hashKey(Key(k))
		if h > maxHash {
			wrapKey = k
			break
		}
	}
	assert.NotEmpty(t, wrapKey)
	node := ring.GetNode(Key(wrapKey))
	assert.Equal(t, NodeID("node-dup"), node)
}



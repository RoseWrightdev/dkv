package server

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rosewrightdev/dkv/kv"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGossipReplication(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping gossip test in short mode")
	}

	tmpDir, _ := os.MkdirTemp("", "dkv-replication-gossip-*")
	defer func() { _ = os.RemoveAll(tmpDir) }()

	ml0, ml1 := freePort(t), freePort(t)
	g0, g1 := freePort(t), freePort(t)
	seed := fmt.Sprintf("127.0.0.1:%d", ml0)

	e1, _ := newTestNode(t, tmpDir, "node1", ml0, g0, nil, 2)
	e2, _ := newTestNode(t, tmpDir, "node2", ml1, g1, []string{seed}, 2)

	// Wait for nodes to discover each other
	require.Eventually(t, func() bool {
		return e1.Owner(kv.Key("any")) != "" && e2.Owner(kv.Key("any")) != ""
	}, 10*time.Second, 100*time.Millisecond)

	// Set on Node 1 (using a key it owns)
	key := FindKeyForNode(e1, "node1")
	val := []byte("replicated-data")
	err := e1.Set(kv.Key(key), val)
	require.NoError(t, err)

	// Wait for write propagation to complete
	require.Eventually(t, func() bool {
		got, ok := e2.Get(kv.Key(key))
		return ok && string(got) == string(val)
	}, 5*time.Second, 50*time.Millisecond, "Data should have replicated to node 2")

	// Delete on Node 1 (the owner)
	err = e1.Delete(kv.Key(key))
	require.NoError(t, err)

	// Verify deletion replicates to node 2
	require.Eventually(t, func() bool {
		_, ok := e2.Get(kv.Key(key))
		return !ok
	}, 5*time.Second, 50*time.Millisecond, "Deletion should have replicated back to node 2")
}

func TestTombstoneResurrectionPrevention(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping resurrection prevention test in short mode")
	}

	tmpDir, _ := os.MkdirTemp("", "dkv-replication-tombstone-*")
	defer func() { _ = os.RemoveAll(tmpDir) }()

	ml0, ml1 := freePort(t), freePort(t)
	g0, g1 := freePort(t), freePort(t)
	seed := fmt.Sprintf("127.0.0.1:%d", ml0)

	eng0, _ := newTestNode(t, tmpDir, "node-0", ml0, g0, nil, 2)
	eng1, _ := newTestNode(t, tmpDir, "node-1", ml1, g1, []string{seed}, 2)

	// Wait for both nodes to form a cluster
	require.Eventually(t, func() bool {
		return eng0.Owner(kv.Key("any-key")) != "" && eng1.Owner(kv.Key("any-key")) != ""
	}, 10*time.Second, 100*time.Millisecond)

	// Find a key owned by node-0 (will replicate to node-1 due to RF=2)
	key := FindKeyForNode(eng0, "node-0")

	// Write — replicates to both with RF=2
	require.NoError(t, eng0.Set(kv.Key(key), []byte("will-be-deleted")))

	// Confirm replication on node-1
	require.Eventually(t, func() bool {
		_, ok := eng1.Get(kv.Key(key))
		return ok
	}, 5*time.Second, 50*time.Millisecond, "key should replicate to node-1")

	// Delete from node-0 (owner) — tombstone propagates
	require.NoError(t, eng0.Delete(kv.Key(key)))

	// Confirm deletion propagated to node-1
	require.Eventually(t, func() bool {
		_, ok := eng1.Get(kv.Key(key))
		return !ok
	}, 5*time.Second, 100*time.Millisecond, "deletion should propagate to node-1")

	// Start a THIRD stale node (has no data, joins late — simulates a stale replica)
	ml2, g2 := freePort(t), freePort(t)
	eng2, _ := newTestNode(t, tmpDir, "node-2", ml2, g2, []string{seed}, 2)

	// Wait for anti-entropy sync to complete
	time.Sleep(2 * time.Second)

	// The deleted key must NOT be resurrected on any node
	_, ok0 := eng0.Get(kv.Key(key))
	_, ok1 := eng1.Get(kv.Key(key))
	_, ok2 := eng2.Get(kv.Key(key))

	assert.False(t, ok0, "node-0: deleted key must not be resurrected")
	assert.False(t, ok1, "node-1: deleted key must not be resurrected")
	assert.False(t, ok2, "node-2: late-joining node must not resurrect deleted key")
}

func TestLWWCrossNodeConflict(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping LWW cross node conflict test in short mode")
	}

	tmpDir, _ := os.MkdirTemp("", "dkv-replication-lww-*")
	defer func() { _ = os.RemoveAll(tmpDir) }()

	ml0, ml1, ml2 := freePort(t), freePort(t), freePort(t)
	g0, g1, g2 := freePort(t), freePort(t), freePort(t)
	seed := fmt.Sprintf("127.0.0.1:%d", ml0)

	eng0, _ := newTestNode(t, tmpDir, "node-0", ml0, g0, nil, 3)
	eng1, _ := newTestNode(t, tmpDir, "node-1", ml1, g1, []string{seed}, 3)
	eng2, _ := newTestNode(t, tmpDir, "node-2", ml2, g2, []string{seed}, 3)

	// Wait for ring convergence
	require.Eventually(t, func() bool {
		o0 := eng0.Owner(kv.Key("any"))
		o1 := eng1.Owner(kv.Key("any"))
		o2 := eng2.Owner(kv.Key("any"))
		return o0 == o1 && o1 == o2 && o0 != ""
	}, 10*time.Second, 100*time.Millisecond, "all nodes should agree on key ownership")

	// Fire concurrent writes from two different nodes to the same key
	key := "lww-conflict-key"
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		_ = eng0.Set(kv.Key(key), []byte("value-from-node-0"))
	}()
	go func() {
		defer wg.Done()
		time.Sleep(5 * time.Millisecond) // ensure a later HLC timestamp
		_ = eng1.Set(kv.Key(key), []byte("value-from-node-1"))
	}()
	wg.Wait()

	// Allow gossip + syncer to converge
	time.Sleep(2 * time.Second)

	// Read from all three nodes — they must all agree on one value
	v0, ok0 := eng0.Get(kv.Key(key))
	v1, ok1 := eng1.Get(kv.Key(key))
	v2, ok2 := eng2.Get(kv.Key(key))

	require.True(t, ok0, "node-0 should have a value")
	require.True(t, ok1, "node-1 should have a value")
	require.True(t, ok2, "node-2 should have a value")

	assert.Equal(t, string(v0), string(v1), "node-0 and node-1 must agree on LWW winner")
	assert.Equal(t, string(v1), string(v2), "node-1 and node-2 must agree on LWW winner")
}

func TestConcurrentDeleteSetRace(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping concurrent delete set race test in short mode")
	}

	tmpDir, _ := os.MkdirTemp("", "dkv-replication-race-*")
	defer func() { _ = os.RemoveAll(tmpDir) }()

	ml0, ml1 := freePort(t), freePort(t)
	g0, g1 := freePort(t), freePort(t)
	seed := fmt.Sprintf("127.0.0.1:%d", ml0)

	eng0, _ := newTestNode(t, tmpDir, "node-0", ml0, g0, nil, 2)
	eng1, _ := newTestNode(t, tmpDir, "node-1", ml1, g1, []string{seed}, 2)

	require.Eventually(t, func() bool {
		return eng0.Owner(kv.Key("race-key")) != ""
	}, 10*time.Second, 100*time.Millisecond)

	key := kv.Key("race-key")

	// Seed an initial value
	require.NoError(t, eng0.Set(key, []byte("initial")))

	var wg sync.WaitGroup
	const workers = 20

	var setWins atomic.Int32
	var delWins atomic.Int32

	for i := range workers {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			if id%3 == 0 {
				if err := eng0.Delete(key); err == nil {
					delWins.Add(1)
				}
			} else {
				v := fmt.Appendf(nil, "racer-%d", id)
				if err := eng1.Set(key, v); err == nil {
					setWins.Add(1)
				}
			}
		}(i)
	}
	wg.Wait()

	// Allow gossip / syncer to converge
	time.Sleep(2 * time.Second)

	v0, ok0 := eng0.Get(key)
	v1, ok1 := eng1.Get(key)

	// Both nodes must agree on the presence/absence of the key
	assert.Equal(t, ok0, ok1, "both nodes must agree on whether the key exists")
	if ok0 && ok1 {
		assert.Equal(t, string(v0), string(v1), "both nodes must agree on the LWW-winning value")
	}
	t.Logf("Set wins: %d, Delete wins: %d, key present: %v", setWins.Load(), delWins.Load(), ok0)
}

package dkv

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/rosewrightdev/dkv/kv"
	"github.com/stretchr/testify/require"
)

func TestReadProxying(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping proxy test in short mode")
	}

	tmpDir, _ := os.MkdirTemp("", "dkv-proxy-read-*")
	defer func() { _ = os.RemoveAll(tmpDir) }()

	ml0, ml1, ml2 := freePort(t), freePort(t), freePort(t)
	g0, g1, g2 := freePort(t), freePort(t), freePort(t)

	seed := fmt.Sprintf("127.0.0.1:%d", ml0)

	// Setup 3 nodes with RF=1 using our shared test helpers
	eng0, _ := newTestNode(t, tmpDir, "node-0", ml0, g0, nil, 1)
	eng1, _ := newTestNode(t, tmpDir, "node-1", ml1, g1, []string{seed}, 1)
	_ = eng1 // Silence unused variable lint
	eng2, _ := newTestNode(t, tmpDir, "node-2", ml2, g2, []string{seed}, 1)

	// Wait for discovery and find a key owned by Node 2 (agreed upon by Node 0 and Node 2)
	var key string
	require.Eventually(t, func() bool {
		for i := range 1000 {
			k := fmt.Sprintf("key-%d", i)
			if eng0.Owner(kv.Key(k)) == kv.NodeID("node-2") && eng2.Owner(kv.Key(k)) == kv.NodeID("node-2") {
				key = k
				return true
			}
		}
		return false
	}, 10*time.Second, 100*time.Millisecond, "Nodes should discover each other and agree on ownership")
	val := []byte("proxy-value")

	// Write directly to Node 2
	err := eng2.Set(kv.Key(key), val)
	require.NoError(t, err)

	// Wait for gossip propagation to finish
	time.Sleep(500 * time.Millisecond)

	// Get key from Node 0, which triggers gateway read proxying to Node 2
	v, ok := eng0.Get(kv.Key(key))
	require.True(t, ok)
	require.Equal(t, val, v)

	// Stop Node 2 to verify Node 0 was proxying and does not hold the key locally
	eng2.Stop()
	time.Sleep(200 * time.Millisecond)

	_, ok = eng0.Get(kv.Key(key))
	require.False(t, ok, "Node 0 should not have the data locally after owner is stopped")
}

func TestWriteProxying(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping proxy test in short mode")
	}

	tmpDir, _ := os.MkdirTemp("", "dkv-proxy-write-*")
	defer func() { _ = os.RemoveAll(tmpDir) }()

	ml0, ml1, ml2 := freePort(t), freePort(t), freePort(t)
	g0, g1, g2 := freePort(t), freePort(t), freePort(t)

	seed := fmt.Sprintf("127.0.0.1:%d", ml0)

	eng0, _ := newTestNode(t, tmpDir, "node-0", ml0, g0, nil, 1)
	eng1, _ := newTestNode(t, tmpDir, "node-1", ml1, g1, []string{seed}, 1)
	_, client2 := newTestNode(t, tmpDir, "node-2", ml2, g2, []string{seed}, 1)

	// Wait for ring convergence: all nodes should agree on ownership
	var key string
	require.Eventually(t, func() bool {
		for i := range 5000 {
			k := fmt.Sprintf("proxy-write-key-%d", i)
			// Find a key NOT owned by node-2 (so the client2 write must be proxied)
			owner0 := eng0.Owner(kv.Key(k))
			owner1 := eng1.Owner(kv.Key(k))
			if owner0 == owner1 && owner0 != "node-2" && owner0 != "" {
				key = k
				return true
			}
		}
		return false
	}, 10*time.Second, 100*time.Millisecond, "nodes should converge on ring ownership")

	val := []byte("write-proxied-value")

	// Write via node-2's gRPC interface — it does NOT own this key.
	err := client2.Set(key, val)
	require.NoError(t, err, "write through non-owner node must succeed via gateway proxying")

	// Verify the owning node actually has the key.
	ownerID := eng0.Owner(kv.Key(key))
	var ownerEng Engine
	switch ownerID {
	case "node-0":
		ownerEng = eng0
	case "node-1":
		ownerEng = eng1
	}
	require.NotNil(t, ownerEng, "expected a known owner engine")

	require.Eventually(t, func() bool {
		got, ok := ownerEng.Get(kv.Key(key))
		return ok && string(got) == string(val)
	}, 5*time.Second, 50*time.Millisecond, "owner node should have the proxied write")
}

func TestDeleteProxying(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping proxy test in short mode")
	}

	tmpDir, _ := os.MkdirTemp("", "dkv-proxy-delete-*")
	defer func() { _ = os.RemoveAll(tmpDir) }()

	ml0, ml1, ml2 := freePort(t), freePort(t), freePort(t)
	g0, g1, g2 := freePort(t), freePort(t), freePort(t)

	seed := fmt.Sprintf("127.0.0.1:%d", ml0)

	eng0, _ := newTestNode(t, tmpDir, "node-0", ml0, g0, nil, 1)
	eng1, _ := newTestNode(t, tmpDir, "node-1", ml1, g1, []string{seed}, 1)
	_, client2 := newTestNode(t, tmpDir, "node-2", ml2, g2, []string{seed}, 1)

	// Find a key owned by node-0 or node-1 (not node-2, so delete is proxied)
	var key string
	var ownerEng Engine
	require.Eventually(t, func() bool {
		for i := range 5000 {
			k := fmt.Sprintf("proxy-del-key-%d", i)
			owner0 := eng0.Owner(kv.Key(k))
			owner1 := eng1.Owner(kv.Key(k))
			if owner0 == owner1 && owner0 != "node-2" && owner0 != "" {
				key = k
				if owner0 == "node-0" {
					ownerEng = eng0
				} else {
					ownerEng = eng1
				}
				return true
			}
		}
		return false
	}, 10*time.Second, 100*time.Millisecond, "nodes should converge on ring ownership")

	// Write directly to owner
	require.NoError(t, ownerEng.Set(kv.Key(key), []byte("to-be-deleted")))

	// Confirm it's readable
	_, ok := ownerEng.Get(kv.Key(key))
	require.True(t, ok)

	// Delete through node-2 (non-owner) — must be proxied
	require.NoError(t, client2.Delete(key), "delete through non-owner must succeed via gateway proxying")

	// Owner should no longer have the key
	require.Eventually(t, func() bool {
		_, ok := ownerEng.Get(kv.Key(key))
		return !ok
	}, 5*time.Second, 50*time.Millisecond, "owner should reflect the proxied delete")
}

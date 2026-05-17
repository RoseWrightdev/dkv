package dkv_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rosewrightdev/dkv"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGossipReplication(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "dkv-gossip-*")
	defer os.RemoveAll(tmpDir)

	// Setup Node 1
	n1Dir := filepath.Join(tmpDir, "node1")
	os.MkdirAll(n1Dir, 0755)

	e1, err := dkv.NewEngineBuilder().
		Default().
		SetWalPath(filepath.Join(n1Dir, "wal")).
		SetSssPath(filepath.Join(n1Dir, "sss.gob")).
		SetNodeID(dkv.NodeID("node1")).
		SetBindPort(8001).
		SetGrpcPort(9001).
		SetInsecure().
		SetReplicationFactor(2).
		GetEngine()
	require.NoError(t, err)
	e1.Start()
	defer e1.Stop()

	// Setup Node 2
	n2Dir := filepath.Join(tmpDir, "node2")
	os.MkdirAll(n2Dir, 0755)

	e2, err := dkv.NewEngineBuilder().
		Default().
		SetWalPath(filepath.Join(n2Dir, "wal")).
		SetSssPath(filepath.Join(n2Dir, "sss.gob")).
		SetNodeID(dkv.NodeID("node2")).
		SetBindPort(8002).
		SetSeedNodes([]string{"127.0.0.1:8001"}).
		SetGrpcPort(9002).
		SetInsecure().
		SetReplicationFactor(2).
		GetEngine()
	require.NoError(t, err)
	e2.Start()
	defer e2.Stop()

	// Wait for nodes to discover each other
	time.Sleep(500 * time.Millisecond)

	// Set on Node 1 (using a key it owns)
	key := dkv.FindKeyForNode(e1, "node1")
	val := []byte("replicated-data")
	err = e1.Set(key, val)
	require.NoError(t, err)

	// Wait for gossip to propagate
	time.Sleep(500 * time.Millisecond)

	// Get from Node 2
	got, ok := e2.Get(dkv.Key(key))
	assert.True(t, ok, "Data should have replicated to node 2")
	assert.Equal(t, val, got)

	// Delete on Node 1 (the owner)
	err = e1.Delete(key)
	require.NoError(t, err)

	// Wait for gossip to propagate
	time.Sleep(500 * time.Millisecond)

	// Verify deletion on Node 1
	_, ok = e1.Get(dkv.Key(key))
	assert.False(t, ok, "Deletion should have replicated back to node 1")
}

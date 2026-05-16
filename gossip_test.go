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
		SetClusterConfig(dkv.ClusterConfig{
			NodeName: "node1",
			BindPort: 8001,
		}).
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
		SetClusterConfig(dkv.ClusterConfig{
			NodeName:  "node2",
			BindPort:  8002,
			SeedNodes: []string{"127.0.0.1:8001"},
		}).
		GetEngine()
	require.NoError(t, err)
	e2.Start()
	defer e2.Stop()

	// Wait for nodes to discover each other
	time.Sleep(500 * time.Millisecond)

	// Set on Node 1
	key, val := "gossip-key", []byte("replicated-data")
	err = e1.Set(key, val)
	require.NoError(t, err)

	// Wait for gossip to propagate
	time.Sleep(500 * time.Millisecond)

	// Get from Node 2
	got, ok := e2.Get(key)
	assert.True(t, ok, "Data should have replicated to node 2")
	assert.Equal(t, val, got)

	// Delete on Node 2
	err = e2.Delete(key)
	require.NoError(t, err)

	// Wait for gossip to propagate
	time.Sleep(500 * time.Millisecond)

	// Verify deletion on Node 1
	_, ok = e1.Get(key)
	assert.False(t, ok, "Deletion should have replicated back to node 1")
}

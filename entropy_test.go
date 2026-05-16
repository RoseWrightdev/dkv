package dkv

import (
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAntiEntropySync(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "dkv-entropy-*")
	defer os.RemoveAll(tmpDir)

	// Setup Node 1
	n1Dir := filepath.Join(tmpDir, "node1")
	os.MkdirAll(n1Dir, 0755)

	e1, err := NewEngineBuilder().
		Default().
		SetWalPath(filepath.Join(n1Dir, "wal")).
		SetSssPath(filepath.Join(n1Dir, "sss.gob")).
		SetGossipInterval(100 * time.Millisecond).
		SetNodeName("node1").
		SetBindPort(9001).
		SetGrpcPort(9002).
		GetEngine()
	require.NoError(t, err)

	s1 := NewServer(e1)
	l1, _ := net.Listen("tcp", "127.0.0.1:9002")
	go s1.Run(l1)
	defer s1.Stop()

	// Write data to Node 1
	key, val := "entropy-key", []byte("eventual-data")
	err = e1.Set(key, val)
	require.NoError(t, err)

	// Setup Node 2 and join Node 1
	n2Dir := filepath.Join(tmpDir, "node2")
	os.MkdirAll(n2Dir, 0755)
	e2, err := NewEngineBuilder().
		Default().
		SetWalPath(filepath.Join(n2Dir, "wal")).
		SetSssPath(filepath.Join(n2Dir, "sss.gob")).
		SetGossipInterval(100 * time.Millisecond).
		SetNodeName("node2").
		SetBindPort(9003).
		SetSeedNodes([]string{"127.0.0.1:9001"}).
		SetGrpcPort(9004).
		GetEngine()
	require.NoError(t, err)

	s2 := NewServer(e2)
	l2, _ := net.Listen("tcp", "127.0.0.1:9004")
	go s2.Run(l2)
	defer s2.Stop()

	e1.Start()
	e2.Start()
	defer e1.Stop()
	defer e2.Stop()

	// Wait for Anti-Entropy to perform sync (if it hasn't already via memberlist join)
	time.Sleep(1500 * time.Millisecond)

	// Verify sync
	got, ok := e2.Get(key)
	assert.True(t, ok, "Node 2 should have reconciled the key")
	assert.Equal(t, val, got)

	// Test Deletion reconciliation
	err = e1.Delete(key)
	require.NoError(t, err)

	// Wait for Anti-Entropy again.
	time.Sleep(1500 * time.Millisecond)

	_, ok = e2.Get(key)
	assert.False(t, ok, "Node 2 should have reconciled the deletion via Anti-Entropy")
}

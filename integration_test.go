package dkv_test

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rosewrightdev/dkv"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEngineOperations(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "dkv-test-*")
	defer os.RemoveAll(tmpDir)

	walPath := filepath.Join(tmpDir, "wal")
	sssPath := filepath.Join(tmpDir, "snapshot.bin")

	eng, err := dkv.NewEngineBuilder().
		Default().
		SetWalPath(walPath).
		SetSssPath(sssPath).
		SingleNode().
		GetEngine()

	require.NoError(t, err)
	eng.Start()
	defer eng.Stop()

	key, val := "foo", []byte("bar")

	// Set
	err = eng.Set(key, val)
	assert.NoError(t, err)

	// Get
	got, ok := eng.Get(key)
	assert.True(t, ok)
	assert.Equal(t, val, got)

	// Delete
	err = eng.Delete(key)
	assert.NoError(t, err)
	_, ok = eng.Get(key)
	assert.False(t, ok)
}

func TestClusterScale(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping scale test in short mode")
	}

	tmpDir, _ := os.MkdirTemp("", "dkv-scale-*")
	defer os.RemoveAll(tmpDir)

	count := 3
	var engines []dkv.Engine
	var clients []*dkv.Client
	var seedAddr string

	for i := range count {
		name := fmt.Sprintf("node-%d", i)
		nodeDir := filepath.Join(tmpDir, name)
		os.MkdirAll(nodeDir, 0755)

		mlLis, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		mlPort := mlLis.Addr().(*net.TCPAddr).Port
		mlLis.Close()

		eb := dkv.NewEngineBuilder().
			Default().
			FastTest().
			SetWalPath(filepath.Join(nodeDir, "wal")).
			SetSssPath(filepath.Join(nodeDir, "sss.gob")).
			SetNodeID(name).
			SetBindPort(mlPort).
			SetGrpcPort(0) // Dynamic

		if i == 0 {
			seedAddr = fmt.Sprintf("127.0.0.1:%d", mlPort)
		} else {
			eb.SetSeedNodes([]string{seedAddr})
		}

		eng, err := eb.GetEngine()
		require.NoError(t, err, "Failed to create engine for %s", name)
		engines = append(engines, eng)

		lis, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		actualPort := lis.Addr().(*net.TCPAddr).Port
		
		server := dkv.NewServer(eng)
		go func() { _ = server.Run(lis) }()

		client, err := dkv.NewInsecureClient(fmt.Sprintf("127.0.0.1:%d", actualPort), time.Second)
		require.NoError(t, err)
		clients = append(clients, client)
	}

	for _, e := range engines {
		e.Start()
	}
	defer func() {
		for _, e := range engines {
			e.Stop()
		}
	}()

	// Parallel writes to different nodes
	for i := range 50 {
		go func(id int) {
			k := fmt.Sprintf("key-%d", id)
			v := []byte(fmt.Sprintf("val-%d", id))
			client := clients[id%count]
			_ = client.Set(k, v)
		}(i)
	}

	// Verify replication with polling
	for i := range 50 {
		k := fmt.Sprintf("key-%d", i)
		v := []byte(fmt.Sprintf("val-%d", i))
		client := clients[(i+1)%count]
		
		require.Eventually(t, func() bool {
			got, exists, err := client.Get(k)
			return err == nil && exists && string(got) == string(v)
		}, 5*time.Second, 100*time.Millisecond, "Node %d failed to replicate %s", (i+1)%count, k)
	}
}

func TestAntiEntropyRecovery(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "dkv-recovery-*")
	defer os.RemoveAll(tmpDir)

	// Setup Node 1
	mlLis1, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	mlPort1 := mlLis1.Addr().(*net.TCPAddr).Port
	mlLis1.Close()

	eng1, err := dkv.NewEngineBuilder().
		Default().
		FastTest().
		SetWalPath(filepath.Join(tmpDir, "n1-wal")).
		SetSssPath(filepath.Join(tmpDir, "n1-sss.gob")).
		SetNodeID("node1").
		SetBindPort(mlPort1).
		SetGrpcPort(0).
		GetEngine()
	require.NoError(t, err)
	eng1.Start()
	defer eng1.Stop()

	// Write data while Node 2 is DOWN
	for i := range 10 {
		err = eng1.Set(fmt.Sprintf("rec-%d", i), []byte("data"))
		require.NoError(t, err)
	}

	// Setup Node 2 (joins Node 1)
	eng2, err := dkv.NewEngineBuilder().
		Default().
		FastTest().
		SetWalPath(filepath.Join(tmpDir, "n2-wal")).
		SetSssPath(filepath.Join(tmpDir, "n2-sss.gob")).
		SetNodeID("node2").
		SetBindPort(0).
		SetGrpcPort(0).
		SetSeedNodes([]string{fmt.Sprintf("127.0.0.1:%d", mlPort1)}).
		GetEngine()
	require.NoError(t, err)
	eng2.Start()
	defer eng2.Stop()

	// Polling verification
	for i := range 10 {
		key := fmt.Sprintf("rec-%d", i)
		require.Eventually(t, func() bool {
			_, ok := eng2.Get(key)
			return ok
		}, 5*time.Second, 100*time.Millisecond, "Node 2 should have recovered %s", key)
	}
}

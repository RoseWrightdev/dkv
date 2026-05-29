package dkv_test

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rosewrightdev/dkv"
	"github.com/rosewrightdev/dkv/kv"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEngineOperations(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "dkv-test-*")
	defer func() {
		_ = os.RemoveAll(tmpDir)
	}()

	walPath := filepath.Join(tmpDir, "wal")
	snpPath := filepath.Join(tmpDir, "snapshot.bin")

	eng, err := dkv.NewEngineBuilder().
		Default().
		SetWalPath(walPath).
		SetSnpPath(snpPath).
		SingleNode().
		SetInsecure().
		Build()

	require.NoError(t, err)
	eng.Start()
	defer eng.Stop()

	key, val := "foo", []byte("bar")

	// Set
	err = eng.Set(key, val)
	assert.NoError(t, err)

	// Get
	got, ok := eng.Get(kv.Key(key))
	assert.True(t, ok)
	assert.Equal(t, val, got)

	// Delete
	err = eng.Delete(key)
	assert.NoError(t, err)
	_, ok = eng.Get(kv.Key(key))
	assert.False(t, ok)
}

func TestClusterScale(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping scale test in short mode")
	}

	tmpDir, _ := os.MkdirTemp("", "dkv-scale-*")
	defer func() {
		_ = os.RemoveAll(tmpDir)
	}()

	count := 3
	var engines []dkv.Engine
	var clients []*dkv.Client
	var seedAddr string

	for i := range count {
		name := fmt.Sprintf("node-%d", i)
		nodeDir := filepath.Join(tmpDir, name)
		require.NoError(t, os.MkdirAll(nodeDir, 0750))

		mlLis, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		mlPort := mlLis.Addr().(*net.TCPAddr).Port
		_ = mlLis.Close()

		gLis, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		grpcPort := gLis.Addr().(*net.TCPAddr).Port
		_ = gLis.Close()

		eb := dkv.NewEngineBuilder().
			Default().
			FastTest().
			SetWalPath(filepath.Join(nodeDir, "wal")).
			SetSnpPath(filepath.Join(nodeDir, "snp.gob")).
			SetNodeID(dkv.NodeID(name)).
			SetBindPort(mlPort).
			SetGrpcPort(grpcPort).
			SetInsecure().
			SetReplicationFactor(3)

		if i == 0 {
			seedAddr = fmt.Sprintf("127.0.0.1:%d", mlPort)
		} else {
			eb.SetSeedNodes([]string{seedAddr})
		}

		eng, err := eb.Build()
		require.NoError(t, err, "Failed to create engine for %s", name)
		engines = append(engines, eng)

		server := dkv.NewServer(eng)
		go func() { _ = server.Run() }()

		client, err := dkv.NewInsecureClient(fmt.Sprintf("127.0.0.1:%d", grpcPort), time.Second)
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
			v := fmt.Appendf(nil, "val-%d", id)
			// Retry Set on different nodes until we find the owner
			for j := range count {
				client := clients[(id+j)%count]
				err := client.Set(k, v)
				if err == nil {
					return
				}
			}
		}(i)
	}

	// Verify replication with polling
	for i := range 50 {
		k := fmt.Sprintf("key-%d", i)
		v := fmt.Appendf(nil, "val-%d", i)
		client := clients[(i+1)%count]

		require.Eventually(t, func() bool {
			got, exists, err := client.Get(k)
			return err == nil && exists && string(got) == string(v)
		}, 5*time.Second, 100*time.Millisecond, "Node %d failed to replicate %s", (i+1)%count, k)
	}
}

func TestAntiEntropyRecovery(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "dkv-recovery-*")
	defer func() {
		_ = os.RemoveAll(tmpDir)
	}()

	// Setup Node 1
	mlLis1, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	mlPort1 := mlLis1.Addr().(*net.TCPAddr).Port
	_ = mlLis1.Close()

	eng1, err := dkv.NewEngineBuilder().
		Default().
		FastTest().
		SetWalPath(filepath.Join(tmpDir, "n1-wal")).
		SetSnpPath(filepath.Join(tmpDir, "n1-snp.gob")).
		SetNodeID(dkv.NodeID("node1")).
		SetBindPort(mlPort1).
		SetGrpcPort(0).
		SetInsecure().
		SetReplicationFactor(2).
		Build()
	require.NoError(t, err)
	eng1.Start()
	defer eng1.Stop()

	// Write data while Node 2 is DOWN (Write to Any)
	for i := range 20 {
		key := fmt.Sprintf("rec-%d", i)
		err = eng1.Set(key, []byte("data"))
		require.NoError(t, err)
	}

	// Setup Node 2 (joins Node 1)
	eng2, err := dkv.NewEngineBuilder().
		Default().
		FastTest().
		SetWalPath(filepath.Join(tmpDir, "n2-wal")).
		SetSnpPath(filepath.Join(tmpDir, "n2-snp.gob")).
		SetNodeID(dkv.NodeID("node2")).
		SetBindPort(0).
		SetGrpcPort(0).
		SetSeedNodes([]string{fmt.Sprintf("127.0.0.1:%d", mlPort1)}).
		SetInsecure().
		SetReplicationFactor(2).
		Build()
	require.NoError(t, err)
	eng2.Start()
	defer eng2.Stop()

	// Polling verification: Node 2 should recover all keys (since RF=2 and Nodes=2)
	for i := range 20 {
		key := fmt.Sprintf("rec-%d", i)
		require.Eventually(t, func() bool {
			_, ok := eng2.Get(kv.Key(key))
			return ok
		}, 10*time.Second, 100*time.Millisecond, "Node 2 should have recovered %s", key)
	}
}

func TestCluster_ConcurrentShutdown(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "dkv-shutdown-*")
	defer func() {
		_ = os.RemoveAll(tmpDir)
	}()

	// Setup Node 1
	mlLis1, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	mlPort1 := mlLis1.Addr().(*net.TCPAddr).Port
	_ = mlLis1.Close()

	gLis1, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	grpcPort1 := gLis1.Addr().(*net.TCPAddr).Port
	_ = gLis1.Close()

	eng1, err := dkv.NewEngineBuilder().
		Default().
		FastTest().
		SetWalPath(filepath.Join(tmpDir, "n1-wal")).
		SetSnpPath(filepath.Join(tmpDir, "n1-snp.gob")).
		SetNodeID(dkv.NodeID("node1")).
		SetBindPort(mlPort1).
		SetGrpcPort(grpcPort1).
		SetInsecure().
		SetReplicationFactor(2).
		Build()
	require.NoError(t, err)
	eng1.Start()

	server1 := dkv.NewServer(eng1)
	go func() { _ = server1.Run() }()

	// Setup Node 2
	gLis2, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	grpcPort2 := gLis2.Addr().(*net.TCPAddr).Port
	_ = gLis2.Close()

	eng2, err := dkv.NewEngineBuilder().
		Default().
		FastTest().
		SetWalPath(filepath.Join(tmpDir, "n2-wal")).
		SetSnpPath(filepath.Join(tmpDir, "n2-snp.gob")).
		SetNodeID(dkv.NodeID("node2")).
		SetBindPort(0).
		SetGrpcPort(grpcPort2).
		SetSeedNodes([]string{fmt.Sprintf("127.0.0.1:%d", mlPort1)}).
		SetInsecure().
		SetReplicationFactor(2).
		Build()
	require.NoError(t, err)
	eng2.Start()

	server2 := dkv.NewServer(eng2)
	go func() { _ = server2.Run() }()

	// Wait for nodes to discover each other
	time.Sleep(500 * time.Millisecond)

	// Write some keys that Node 2 will proxy to Node 1
	for i := range 10 {
		key := fmt.Sprintf("shutdown-key-%d", i)
		err = eng1.Set(key, []byte("val"))
		require.NoError(t, err)
	}

	// Concurrent read proxying loop
	stopCh := make(chan struct{})
	go func() {
		for {
			select {
			case <-stopCh:
				return
			default:
				for i := range 10 {
					key := fmt.Sprintf("shutdown-key-%d", i)
					_, _ = eng2.Get(kv.Key(key))
				}
			}
		}
	}()

	// Let proxy reads run
	time.Sleep(50 * time.Millisecond)

	// Shutdown concurrently
	done := make(chan struct{})
	go func() {
		server2.Stop()
		close(done)
	}()
	time.Sleep(10 * time.Millisecond)
	server1.Stop()

	<-done
	close(stopCh)
}

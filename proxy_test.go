package dkv_test

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rosewrightdev/dkv"
	"github.com/stretchr/testify/require"
)

func TestReadProxying(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping proxy test in short mode")
	}

	tmpDir, _ := os.MkdirTemp("", "dkv-proxy-*")
	defer func() {
		_ = os.RemoveAll(tmpDir)
	}()

	// Setup 3 nodes with RF=1
	count := 3
	var engines []dkv.Engine
	var servers []*dkv.Grpc
	var seedAddr string


	for i := range count {
		name := fmt.Sprintf("node-%d", i)
		nodeDir := filepath.Join(tmpDir, name)
		require.NoError(t, os.MkdirAll(nodeDir, 0750))

		mlLis, _ := net.Listen("tcp", "127.0.0.1:0")
		mlPort := mlLis.Addr().(*net.TCPAddr).Port
		_ = mlLis.Close()

		lis, _ := net.Listen("tcp", "127.0.0.1:0")
		grpcPort := lis.Addr().(*net.TCPAddr).Port

		eb := dkv.NewEngineBuilder().
			Default().
			FastTest().
			SetWalPath(filepath.Join(nodeDir, "wal")).
			SetSssPath(filepath.Join(nodeDir, "sss.gob")).
			SetNodeID(dkv.NodeID(name)).
			SetBindPort(mlPort).
			SetGrpcPort(grpcPort).
			SetInsecure().
			SetReplicationFactor(1)

		if i == 0 {
			seedAddr = fmt.Sprintf("127.0.0.1:%d", mlPort)
		} else {
			eb.SetSeedNodes([]string{seedAddr})
		}

		eng, err := eb.GetEngine()
		require.NoError(t, err)
		engines = append(engines, eng)

		srv := dkv.NewServer(eng)
		servers = append(servers, srv)
		go func() {
			_ = srv.Run(lis)
		}()

		eng.Start()
	}

	defer func() {
		for _, s := range servers {
			s.Stop()
		}
		for _, e := range engines {
			e.Stop()
		}
	}()

	// Wait for discovery and find a key owned by Node 2 (agreed upon by Node 0)
	var key string
	require.Eventually(t, func() bool {
		for i := range 1000 {
			k := fmt.Sprintf("key-%d", i)
			if engines[0].Owner(dkv.Key(k)) == dkv.NodeID("node-2") && engines[2].Owner(dkv.Key(k)) == dkv.NodeID("node-2") {
				key = k
				return true
			}
		}
		return false
	}, 10*time.Second, 100*time.Millisecond, "Nodes should discover each other and agree on ownership")
	val := []byte("proxy-value")

	// Write directly to Node 2
	err := engines[2].Set(key, val)
	require.NoError(t, err)

	// Wait for gossip propagation to finish
	time.Sleep(500 * time.Millisecond)

	// Get key from Node 0, which triggers gateway read proxying to Node 2
	v, ok := engines[0].Get(dkv.Key(key))
	require.True(t, ok)
	require.Equal(t, val, v)

	// Stop Node 2 to verify Node 0 was proxying and does not hold the key locally
	engines[2].Stop()
	servers[2].Stop()
	time.Sleep(200 * time.Millisecond)

	_, ok = engines[0].Get(dkv.Key(key))
	require.False(t, ok, "Node 0 should not have the data locally after owner is stopped")
}

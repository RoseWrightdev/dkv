package server

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rosewrightdev/dkv"
	"github.com/rosewrightdev/dkv/gateway"
	"github.com/rosewrightdev/dkv/kv"
	"github.com/stretchr/testify/require"
)

func newTestNode(t *testing.T, tmpDir, name string, mlPort, grpcPort int, seeds []string, rf int) (dkv.Engine, *gateway.Client) {
	t.Helper()
	nodeDir := filepath.Join(tmpDir, name)
	require.NoError(t, os.MkdirAll(nodeDir, 0750))

	eb := dkv.NewEngineBuilder().
		Default().
		FastTest().
		SetWalPath(filepath.Join(nodeDir, "wal")).
		SetSnpPath(filepath.Join(nodeDir, "snp.gob")).
		SetNodeID(kv.NodeID(name)).
		SetBindPort(mlPort).
		SetGrpcPort(grpcPort).
		SetInsecure().
		SetReplicationFactor(rf)

	if len(seeds) > 0 {
		eb.SetSeedNodes(seeds)
	}

	eng, err := eb.Build()
	require.NoError(t, err)

	srv := NewServer(eng)
	go func() { _ = srv.Run() }()
	eng.Start()

	t.Cleanup(func() {
		srv.Stop()
		eng.Stop()
	})

	client, err := gateway.NewInsecureClient(fmt.Sprintf("127.0.0.1:%d", grpcPort), 2*time.Second)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	return eng, client
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	return port
}

func FindKeyForNode(e dkv.Engine, nodeID string) string {
	for i := range 1000 {
		k := fmt.Sprintf("test-key-%d", i)
		if e.Owner(kv.Key(k)) == kv.NodeID(nodeID) {
			return k
		}
	}
	panic("could not find key for node")
}

package dkv

import (
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rosewrightdev/dkv/clock"
	"github.com/rosewrightdev/dkv/evict"
	"github.com/rosewrightdev/dkv/kv"
	"github.com/rosewrightdev/dkv/mesh"
	"github.com/rosewrightdev/dkv/gateway"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

var mockConfig = EngineConfig{
	walPath:        "test_wal_dir",
	snpPath:        "test_snapshot.gob",
	walInterval:    100 * time.Millisecond,
	snpInterval:    500 * time.Millisecond,
	walBufferSize:  uint32(64 * 1024),
	walSegments:    4,
	evt:            evict.NewLRU(evict.LRUConfig{Capacity: 100, TTL: time.Hour, ShardCount: 16}),
	gossipInterval: 10 * time.Second,
	clock:          clock.NewClock(),
	meshConfig:     mesh.MeshConfig{SingleNode: true},
	creds:          insecure.NewCredentials(),
}

func init() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelError,
	})))
}

func cleanupEngineMocks(t *testing.T) {
	if err := os.Remove(mockConfig.snpPath); err != nil && !os.IsNotExist(err) {
		assert.Nil(t, err)
	}
	if err := os.Remove(mockConfig.snpPath + ".tmp"); err != nil && !os.IsNotExist(err) {
		assert.Nil(t, err)
	}
	if err := os.RemoveAll(mockConfig.walPath); err != nil && !os.IsNotExist(err) {
		assert.Nil(t, err)
	}
}

// FindKeyForNode returns a key that is owned by the given nodeID in the provided engine.
func FindKeyForNode(e Engine, nodeID string) string {
	for i := range 1000 {
		k := fmt.Sprintf("test-key-%d", i)
		if e.Owner(kv.Key(k)) == kv.NodeID(nodeID) {
			return k
		}
	}
	panic("could not find key for node")
}

// newTestNode builds, starts, and registers t.Cleanup for a single dkv engine+server.
// It returns the engine and a gRPC client pointing at it.
func newTestNode(t *testing.T, tmpDir, name string, mlPort, grpcPort int, seeds []string, rf int) (Engine, *gateway.Client) {
	t.Helper()
	nodeDir := filepath.Join(tmpDir, name)
	require.NoError(t, os.MkdirAll(nodeDir, 0750))

	eb := NewEngineBuilder().
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

// freePort returns an available TCP port on localhost.
func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	return port
}

type mockWal struct {
	clearCalled bool
}

func (mw *mockWal) Publish(_ kv.Key, _ kv.HashKey, _ proto.Message) error { return nil }
func (mw *mockWal) Replay() (map[kv.Key]kv.Value, error)                  { return nil, nil }
func (mw *mockWal) Clear(_ []int64) error                                 { mw.clearCalled = true; return nil }
func (mw *mockWal) PrepareSnapshot() ([]int64, error)                     { return nil, nil }
func (mw *mockWal) Stop()                                                 {}
func (mw *mockWal) Start()                                                {}


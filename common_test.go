package dkv

import (
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/rosewrightdev/dkv/internal/clock"
	"github.com/rosewrightdev/dkv/evict"
	"github.com/rosewrightdev/dkv/kv"
	"github.com/rosewrightdev/dkv/internal/mesh"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

var mockConfig = EngineConfig{
	walPath:        "test_wal_dir",
	snpPath:        "test_snapshot.gob",
	walInterval:    10 * time.Millisecond,
	snpInterval:    50 * time.Millisecond,
	walBufferSize:  4096,
	walSegments:    3,
	evt:            evict.NewLRU(evict.LRUConfig{Capacity: 100, TTL: time.Hour, ShardCount: 16}),
	gossipInterval: 50 * time.Millisecond,
	clock:          clock.NewClock(),
	meshConfig:     mesh.MeshConfig{SingleNode: true},
	creds:          insecure.NewCredentials(),
}

func init() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})))
}

func cleanup() {
	if err := os.Remove(mockConfig.snpPath); err != nil && !os.IsNotExist(err) {
		panic(err)
	}
	if err := os.Remove(mockConfig.snpPath + ".tmp"); err != nil && !os.IsNotExist(err) {
		panic(err)
	}
	if err := os.RemoveAll(mockConfig.walPath); err != nil && !os.IsNotExist(err) {
		panic(err)
	}
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

type mockWal struct {
	clearCalled bool
}

func (mw *mockWal) Publish(_ kv.Key, _ kv.HashKey, _ proto.Message) error { return nil }
func (mw *mockWal) Replay() (map[kv.Key]kv.Value, error)                  { return nil, nil }
func (mw *mockWal) Clear(_ []int64) error                                 { mw.clearCalled = true; return nil }
func (mw *mockWal) PrepareSnapshot() ([]int64, error)                     { return nil, nil }
func (mw *mockWal) Stop()                                                 {}
func (mw *mockWal) Start()                                                {}


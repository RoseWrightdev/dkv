package dkv

import (
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/credentials/insecure"
)

var mockConfig EngineConfig = EngineConfig{
	walPath:         "test_wal_dir",
	sssPath:         "test_snapshot.bin",
	walSyncInterval: 100 * time.Millisecond,
	sssInterval:     500 * time.Millisecond,
	walBufferSize:   uint32(64 * 1024),
	walSegments:     4,
	evictionService: NewLRU(LRUConfig{Capacity: 100, TTL: time.Hour, ShardCount: 16}),
	gossipInterval:  10 * time.Second,
	clock:           NewHLC(),
	clusterConfig:        ClusterConfig{SingleNode: true},
	transportCredentials: insecure.NewCredentials(),
}

func init() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelError,
	})))
}

func cleanupEngineMocks(t *testing.T) {
	if err := os.Remove(mockConfig.sssPath); err != nil && !os.IsNotExist(err) {
		assert.Nil(t, err)
	}
	if err := os.Remove(mockConfig.sssPath + ".tmp"); err != nil && !os.IsNotExist(err) {
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
		if e.Owner(Key(k)) == NodeID(nodeID) {
			return k
		}
	}
	panic("could not find key for node")
}

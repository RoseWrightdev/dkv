package dkv

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	pb "github.com/rosewrightdev/dkv/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSync(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "dkv-syncer-*")
	defer func() {
		_ = os.RemoveAll(tmpDir)
	}()

	// Setup Node 1
	n1Dir := filepath.Join(tmpDir, "node1")
	require.NoError(t, os.MkdirAll(n1Dir, 0750))

	e1, err := NewEngineBuilder().
		Default().
		SetWalPath(filepath.Join(n1Dir, "wal")).
		SetSnpPath(filepath.Join(n1Dir, "snp.gob")).
		SetGossipInterval(100 * time.Millisecond).
		SetNodeID(NodeID("node1")).
		SetBindPort(9001).
		SetGrpcPort(9002).
		SetInsecure().
		SetReplicationFactor(2).
		Build()
	require.NoError(t, err)

	s1 := NewServer(e1)
	go func() {
		_ = s1.Run()
	}()
	defer s1.Stop()

	// Setup Node 2 and join Node 1
	n2Dir := filepath.Join(tmpDir, "node2")
	require.NoError(t, os.MkdirAll(n2Dir, 0750))
	e2, err := NewEngineBuilder().
		Default().
		SetWalPath(filepath.Join(n2Dir, "wal")).
		SetSnpPath(filepath.Join(n2Dir, "snp.gob")).
		SetGossipInterval(100 * time.Millisecond).
		SetNodeID(NodeID("node2")).
		SetBindPort(9003).
		SetSeedNodes([]string{"127.0.0.1:9001"}).
		SetGrpcPort(9004).
		SetInsecure().
		SetReplicationFactor(2).
		Build()
	require.NoError(t, err)

	s2 := NewServer(e2)
	go func() {
		_ = s2.Run()
	}()
	defer s2.Stop()

	e1.Start()
	e2.Start()
	defer e1.Stop()
	defer e2.Stop()

	time.Sleep(500 * time.Millisecond)

	// Write data to Node 1 (using a key it owns in the final topology)
	key := FindKeyForNode(e1, "node1")
	val := []byte("eventual-data")
	err = e1.Set(key, val)
	require.NoError(t, err)

	// Wait for Syncer to perform sync (if it hasn't already via memberlist join)
	time.Sleep(1500 * time.Millisecond)

	// Verify sync
	got, ok := e2.Get(Key(key))
	assert.True(t, ok, "Node 2 should have reconciled the key")
	assert.Equal(t, val, got)

	// Test Deletion reconciliation
	err = e1.Delete(key)
	require.NoError(t, err)

	time.Sleep(1500 * time.Millisecond)

	_, ok = e2.Get(Key(key))
	assert.False(t, ok, "Node 2 should have reconciled the deletion via Syncer")
}

func TestSync_PreparePullRequestDataRace(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "dkv-syncer-race-*")
	defer func() {
		_ = os.RemoveAll(tmpDir)
	}()

	e, err := NewEngineBuilder().
		Default().
		SetWalPath(filepath.Join(tmpDir, "wal")).
		SetSnpPath(filepath.Join(tmpDir, "snp.gob")).
		SetInsecure().
		Build()
	require.NoError(t, err)
	defer e.Stop()

	eng := e.(*engine)

	syn := newSyncer(&SyncerConfig{
		nodeID:     eng.meshConfig.NodeID,
		gossip:     eng.sip,
		mesh:       eng.mesh,
		meshConfig: &eng.meshConfig,
		hm:         eng.hm,
		pools:      eng.pools,
		interval:   10 * time.Second,
		creds:      eng.creds,
	})

	stop := make(chan struct{})

	// Start the writer goroutine that keeps getting from pool, modifying/filling, and putting back.
	go func() {
		i := 0
		for {
			select {
			case <-stop:
				return
			default:
				key := fmt.Sprintf("racekey-%d", i)
				_ = e.Set(key, []byte("val"))
				m := eng.pools.bucketMaps.Get().(map[ShardID]ShardDigest)
				eng.hm.FillDigests(m)
				eng.pools.bucketMaps.Put(m)
				i++
			}
		}
	}()

	// Main goroutine constantly prepares requests and reads the digests concurrently.
	for range 100 {
		req := eng.pools.pullRequests.Get().(*pb.PullRequest)
		syn.preparePullRequest(req)

		// Concurrently read the request digests that reference the pool's slices.
		for _, sd := range req.SubDigests {
			for _, h := range sd.SubHashes {
				_ = h
			}
		}

		syn.cleanupPullRequest(req)
	}

	close(stop)
}

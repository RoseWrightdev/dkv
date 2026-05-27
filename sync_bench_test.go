package dkv

import (
	"fmt"
	"testing"
	"time"
)

func BenchmarkSyncPull_Identical(b *testing.B) {
	e, err := NewEngineBuilder().Default().
		SetWalPath(b.TempDir()).
		SetSnpPath(b.TempDir() + "/snapshot.db").
		SingleNode().
		SetInsecure().
		Build()
	if err != nil {
		b.Fatalf("Failed to create engine: %v", err)
	}
	eng := e.(*engine)
	eng.Start()
	defer eng.Stop()

	for i := range 10000 {
		_ = eng.Set(fmt.Sprintf("key-%d", i), []byte("value"))
	}

	root := eng.hm.RootDigest()
	shards := make(map[ShardID]Digest)
	buckets := make(map[ShardID]ShardDigest)
	for i := range shardCount {
		buckets[ShardID(i)] = make([]Digest, subBucketCount)
	}
	eng.hm.FillShardDigests(shards)
	eng.hm.FillDigests(buckets)

	syncer := newSyncer(&SyncerConfig{
		nodeID:     eng.meshConfig.NodeID,
		writer:     eng.sw,
		mesh:       eng.mesh,
		meshConfig: &eng.meshConfig,
		hm:         eng.hm,
		pools:      eng.pools,
		interval:   10 * time.Second,
		creds:      eng.creds,
		cc:         eng.gw.cc,
	})

	mockPullConfig := &PullConfig{
		requesterID: "node-1",
		root:        root,
		shards:      shards,
		buckets:     buckets,
	}

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		_, _, _ = eng.pullWithSyncer(mockPullConfig, *syncer)
	}
}

func BenchmarkSyncPull_SingleMismatch(b *testing.B) {
	e, err := NewEngineBuilder().Default().
		SetWalPath(b.TempDir()).
		SetSnpPath(b.TempDir() + "/snapshot.db").
		SingleNode().
		SetInsecure().
		Build()
	if err != nil {
		b.Fatalf("Failed to create engine: %v", err)
	}
	eng := e.(*engine)
	eng.Start()
	defer eng.Stop()

	for i := range 10000 {
		_ = eng.Set(fmt.Sprintf("key-%d", i), []byte("value"))
	}

	root := eng.hm.RootDigest()
	shards := make(map[ShardID]Digest)
	buckets := make(map[ShardID]ShardDigest)
	for i := range shardCount {
		buckets[ShardID(i)] = make([]Digest, subBucketCount)
	}
	eng.hm.FillShardDigests(shards)
	eng.hm.FillDigests(buckets)

	syncer := newSyncer(&SyncerConfig{
		nodeID:     eng.meshConfig.NodeID,
		writer:     eng.sw,
		mesh:       eng.mesh,
		meshConfig: &eng.meshConfig,
		hm:         eng.hm,
		pools:      eng.pools,
		interval:   10 * time.Second,
		creds:      eng.creds,
		cc:         eng.gw.cc,
	})

	// Create a mismatch in one bucket
	_ = eng.Set("mismatch-key", []byte("mismatch-value"))

	mockPullConfig := &PullConfig{
		requesterID: "node-1",
		root:        root,
		shards:      shards,
		buckets:     buckets,
	}

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		_, _, _ = eng.pullWithSyncer(mockPullConfig, *syncer)
	}
}

func BenchmarkSyncPull_FullDivergence(b *testing.B) {
	e, err := NewEngineBuilder().Default().
		SetWalPath(b.TempDir()).
		SetSnpPath(b.TempDir() + "/snapshot.db").
		SingleNode().
		SetInsecure().
		Build()
	if err != nil {
		b.Fatalf("Failed to create engine: %v", err)
	}
	eng := e.(*engine)
	eng.Start()
	defer eng.Stop()

	for i := range 10000 {
		_ = eng.Set(fmt.Sprintf("key-%d", i), []byte("value"))
	}

	root := eng.hm.RootDigest()
	shards := make(map[ShardID]Digest)
	buckets := make(map[ShardID]ShardDigest)
	for i := range shardCount {
		buckets[ShardID(i)] = make([]Digest, subBucketCount)
	}
	eng.hm.FillShardDigests(shards)
	eng.hm.FillDigests(buckets)

	syncer := newSyncer(&SyncerConfig{
		nodeID:     eng.meshConfig.NodeID,
		writer:     eng.sw,
		mesh:       eng.mesh,
		meshConfig: &eng.meshConfig,
		hm:         eng.hm,
		pools:      eng.pools,
		interval:   10 * time.Second,
		creds:      eng.creds,
		cc:         eng.gw.cc,
	})

	// Create mismatch in ALL shards
	for i := range shardCount {
		_ = eng.Set(fmt.Sprintf("mismatch-%d", i), []byte("val"))
	}

	mockPullConfig := &PullConfig{
		requesterID: "node-1",
		root:        root,
		shards:      shards,
		buckets:     buckets,
	}

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		_, _, _ = eng.pullWithSyncer(mockPullConfig, *syncer)
	}
}

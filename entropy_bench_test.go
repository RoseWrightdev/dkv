package dkv

import (
	"fmt"
	"testing"

	"github.com/rosewrightdev/dkv/entropy"
	"github.com/rosewrightdev/dkv/hashmap"
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
	shards := make(map[hashmap.ShardID]hashmap.Digest)
	buckets := make(map[hashmap.ShardID]hashmap.ShardDigest)
	for i := range hashmap.ShardCount {
		buckets[hashmap.ShardID(i)] = make([]hashmap.Digest, hashmap.SubBucketCount)
	}
	eng.hm.FillShardDigests(shards)
	eng.hm.FillDigests(buckets)

	mockPullConfig := &entropy.PullConfig{
		RequesterID: "node-1",
		Root:        root,
		Shards:      shards,
		Buckets:     buckets,
	}

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		_, _, _ = eng.SyncPull(mockPullConfig)
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
	shards := make(map[hashmap.ShardID]hashmap.Digest)
	buckets := make(map[hashmap.ShardID]hashmap.ShardDigest)
	for i := range hashmap.ShardCount {
		buckets[hashmap.ShardID(i)] = make([]hashmap.Digest, hashmap.SubBucketCount)
	}
	eng.hm.FillShardDigests(shards)
	eng.hm.FillDigests(buckets)

	// Create a mismatch in one bucket
	_ = eng.Set("mismatch-key", []byte("mismatch-value"))

	mockPullConfig := &entropy.PullConfig{
		RequesterID: "node-1",
		Root:        root,
		Shards:      shards,
		Buckets:     buckets,
	}

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		_, _, _ = eng.SyncPull(mockPullConfig)
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
	shards := make(map[hashmap.ShardID]hashmap.Digest)
	buckets := make(map[hashmap.ShardID]hashmap.ShardDigest)
	for i := range hashmap.ShardCount {
		buckets[hashmap.ShardID(i)] = make([]hashmap.Digest, hashmap.SubBucketCount)
	}
	eng.hm.FillShardDigests(shards)
	eng.hm.FillDigests(buckets)

	// Create mismatch in ALL shards
	for i := range hashmap.ShardCount {
		_ = eng.Set(fmt.Sprintf("mismatch-%d", i), []byte("val"))
	}

	mockPullConfig := &entropy.PullConfig{
		RequesterID: "node-1",
		Root:        root,
		Shards:      shards,
		Buckets:     buckets,
	}

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		_, _, _ = eng.SyncPull(mockPullConfig)
	}
}

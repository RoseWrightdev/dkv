package dkv

import "sync"

type Key = string
type ShardID = int32
type Digest = uint64
type RootDigest = uint64
type ShardDigest = []Digest
const subBucketCount = 64

const shardCount = 128

// Value represents a single record in the database, including metadata for LWW.
type Value struct {
	Data      []byte
	Timestamp int64
	NodeID    string
	Tombstone bool
}

// shard is a single thread-safe bucket within the shardedMap.
type shard struct {
	mu          sync.RWMutex
	buckets     [subBucketCount]map[Key]Value
	subDigests  [subBucketCount]Digest
	shardDigest Digest // XOR of all subDigests
}

// shardedMap is a high-concurrency map implementation that uses multiple locks.
type shardedMap [shardCount]*shard

func newShardedMap() *shardedMap {
	var sm shardedMap
	for i := range shardCount {
		s := &shard{}
		for b := range subBucketCount {
			s.buckets[b] = make(map[Key]Value)
		}
		sm[i] = s
	}
	return &sm
}

func (sm *shardedMap) getShardByHash(hash hashKey) *shard {
	return sm[hash%shardCount]
}

// Load retrieves a value from the correct shard based on the provided hash.
func (sm *shardedMap) Load(key Key, hash hashKey) (Value, bool) {
	shard := sm.getShardByHash(hash)
	subIndex := (hash >> 16) % subBucketCount
	shard.mu.RLock()
	val, ok := shard.buckets[subIndex][key]
	shard.mu.RUnlock()
	return val, ok
}

// Store updates the value in the correct shard and maintains the rolling digest.
func (sm *shardedMap) Store(key Key, hash hashKey, val Value) {
	shard := sm.getShardByHash(hash)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	// Update sub-bucket and shard digests incrementally
	subIndex := (hash >> 16) % subBucketCount
	if existing, ok := shard.buckets[subIndex][key]; ok {
		oldItemHash := hash ^ uint64(existing.Timestamp)
		shard.subDigests[subIndex] ^= oldItemHash
		shard.shardDigest ^= oldItemHash
	}

	newItemHash := hash ^ uint64(val.Timestamp)
	shard.subDigests[subIndex] ^= newItemHash
	shard.shardDigest ^= newItemHash
	shard.buckets[subIndex][key] = val
}

// Delete removes a key from its shard and updates the rolling digest.
func (sm *shardedMap) Delete(key Key, hash hashKey) {
	shard := sm.getShardByHash(hash)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	subIndex := (hash >> 16) % subBucketCount
	if existing, ok := shard.buckets[subIndex][key]; ok {
		itemHash := uint64(hash) ^ uint64(existing.Timestamp)
		shard.subDigests[subIndex] ^= itemHash
		shard.shardDigest ^= itemHash
		delete(shard.buckets[subIndex], key)
	}
}

// FillShardDigests populates the provided map with all shard IDs and their single intermediate XOR digests.
func (sm *shardedMap) FillShardDigests(dst map[ShardID]Digest) {
	for i := range shardCount {
		shard := sm[i]
		shard.mu.RLock()
		dst[ShardID(i)] = shard.shardDigest
		shard.mu.RUnlock()
	}
}

// RootDigest returns a single XOR hash of the entire database state.
func (sm *shardedMap) RootDigest() RootDigest {
	var root RootDigest
	for i := range shardCount {
		shard := sm[i]
		shard.mu.RLock()
		root ^= shard.shardDigest
		shard.mu.RUnlock()
	}
	return root
}

// FillDigests populates the provided map with all shard IDs and their current sub-bucket hashes.
func (sm *shardedMap) FillDigests(dst map[ShardID]ShardDigest) {
	for i := range shardCount {
		shard := sm[i]
		shard.mu.RLock()
		copy(dst[ShardID(i)], shard.subDigests[:])
		shard.mu.RUnlock()
	}
}

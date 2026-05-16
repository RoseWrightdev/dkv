package dkv

import "sync"

type Key = string
type ShardID = int32
type ShardDigest = uint64

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
	mu            sync.RWMutex
	m             map[Key]Value
	rollingDigest uint64
}

// shardedMap is a high-concurrency map implementation that uses multiple locks.
type shardedMap [shardCount]*shard

func newShardedMap() *shardedMap {
	var sm shardedMap
	for i := range shardCount {
		sm[i] = &shard{m: make(map[Key]Value)}
	}
	return &sm
}

func (sm *shardedMap) getShardByHash(hash hashKey) *shard {
	return sm[hash%shardCount]
}

// Load retrieves a value from the correct shard based on the provided hash.
func (sm *shardedMap) Load(key Key, hash hashKey) (Value, bool) {
	shard := sm.getShardByHash(hash)
	shard.mu.RLock()
	val, ok := shard.m[key]
	shard.mu.RUnlock()
	return val, ok
}

// Store updates the value in the correct shard and maintains the rolling digest.
func (sm *shardedMap) Store(key Key, hash hashKey, val Value) {
	shard := sm.getShardByHash(hash)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	// Update rolling digest incrementally
	if existing, ok := shard.m[key]; ok {
		// XOR out the old contribution
		shard.rollingDigest ^= uint64(hash) ^ uint64(existing.Timestamp)
	}

	// XOR in the new contribution
	shard.rollingDigest ^= uint64(hash) ^ uint64(val.Timestamp)
	shard.m[key] = val
}

// Delete removes a key from its shard and updates the rolling digest.
func (sm *shardedMap) Delete(key Key, hash hashKey) {
	shard := sm.getShardByHash(hash)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	if existing, ok := shard.m[key]; ok {
		// XOR out the contribution before deleting
		shard.rollingDigest ^= uint64(hash) ^ uint64(existing.Timestamp)
		delete(shard.m, key)
	}
}

// Digests returns a map of all shard IDs to their current rolling hashes.
func (sm *shardedMap) Digests() map[ShardID]ShardDigest {
	digests := make(map[ShardID]ShardDigest)
	for i := range shardCount {
		shard := sm[i]
		shard.mu.RLock()
		digests[ShardID(i)] = shard.rollingDigest
		shard.mu.RUnlock()
	}
	return digests
}

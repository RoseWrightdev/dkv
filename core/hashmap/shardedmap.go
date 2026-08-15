// Package hashmap provides a concurrent-safe sharded map implementation with incremental digest generation.
package hashmap

import (
	"math/bits"
	"sync"
	"sync/atomic"

	"github.com/rosewrightdev/oryx/kv"
	"github.com/rosewrightdev/oryx/security"
)

// ShardID represents the identifier of a shard within the ShardedMap.
type ShardID = int32

// Digest represents a hash value used for detecting data divergence.
type Digest = uint64

// RootDigest represents the combined hash of the entire database state.
type RootDigest = uint64

// ShardDigest represents the collection of sub-bucket digests for a specific shard.
type ShardDigest = []Digest

// SubBucketCount defines the number of sub-buckets in each shard.
const SubBucketCount = 64

// ShardCount defines the total number of shards in the map.
const ShardCount = 128

// shard is a single thread-safe bucket within the ShardedMap.
type shard struct {
	buckets     [SubBucketCount]map[kv.Key]kv.Value
	readBuckets [SubBucketCount]atomic.Pointer[map[kv.Key]kv.Value]
	subDigests  [SubBucketCount]Digest
	shardDigest Digest
	mu          sync.RWMutex
}

// ShardedMap is a high-concurrency map implementation that uses multiple locks.
type ShardedMap [ShardCount]*shard

// NewShardedMap initializes a new ShardedMap with all shards prepared.
func NewShardedMap() *ShardedMap {
	var sm ShardedMap
	for i := range ShardCount {
		s := &shard{}
		for b := range SubBucketCount {
			m := make(map[kv.Key]kv.Value)
			s.buckets[b] = m
			s.readBuckets[b].Store(&m)
		}
		sm[i] = s
	}
	return &sm
}

func (sm *ShardedMap) getShardByHash(hash kv.HashKey) *shard {
	return sm[hash%ShardCount]
}

// Load retrieves a value from the correct shard based on the provided hash.
func (sm *ShardedMap) Load(key kv.Key, hash kv.HashKey) (kv.Value, bool) {
	shard := sm.getShardByHash(hash)
	subIndex := (hash >> 16) % SubBucketCount
	mPtr := shard.readBuckets[subIndex].Load()
	if mPtr == nil {
		return kv.Value{}, false
	}
	val, ok := (*mPtr)[key]
	val.ItemHash = 0 // Clear internal-only ItemHash to preserve DeepEqual assertions in tests
	return val, ok
}

// LoadData retrieves just the raw byte payload from the shard in a 100% lock-free read path.
func (sm *ShardedMap) LoadData(key kv.Key, hash kv.HashKey) ([]byte, bool) {
	shard := sm.getShardByHash(hash)
	subIndex := (hash >> 16) % SubBucketCount
	mPtr := shard.readBuckets[subIndex].Load()
	if mPtr == nil {
		return nil, false
	}
	val, ok := (*mPtr)[key]
	if !ok || val.Tombstone {
		return nil, false
	}
	return val.Data, true
}

func getItemHash(hash kv.HashKey, val kv.Value) uint64 {
	// #nosec G115
	h := hash ^ bits.RotateLeft64(uint64(val.Timestamp), 17)

	if val.NodeID != "" {
		h ^= bits.RotateLeft64(security.HashFunc(val.NodeID), 31)
	}

	if len(val.Data) > 0 {
		h ^= bits.RotateLeft64(security.HashBytes(val.Data), 47)
	}

	if val.Tombstone {
		h ^= 0x5555555555555555
	}

	return h
}

func (s *shard) publishReadBucket(subIndex kv.HashKey) {
	src := s.buckets[subIndex]
	cp := make(map[kv.Key]kv.Value, len(src))
	for k, v := range src {
		cp[k] = v
	}
	s.readBuckets[subIndex].Store(&cp)
}

// Store updates the value in the correct shard and maintains the rolling digest.
func (sm *ShardedMap) Store(key kv.Key, hash kv.HashKey, val kv.Value) {
	shard := sm.getShardByHash(hash)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	// Update sub-bucket and shard digests incrementally
	subIndex := (hash >> 16) % SubBucketCount
	if existing, ok := shard.buckets[subIndex][key]; ok {
		oldItemHash := existing.ItemHash
		shard.subDigests[subIndex] ^= oldItemHash
		shard.shardDigest ^= oldItemHash
	}

	val.ItemHash = getItemHash(hash, val)
	shard.subDigests[subIndex] ^= val.ItemHash
	shard.shardDigest ^= val.ItemHash
	shard.buckets[subIndex][key] = val
	shard.publishReadBucket(subIndex)
}

// StoreLWW updates the value in the correct shard using LWW conflict resolution under a single write lock.
// It returns true if the value was stored, and false if ignored as stale.
func (sm *ShardedMap) StoreLWW(key kv.Key, hash kv.HashKey, val kv.Value) bool {
	shard := sm.getShardByHash(hash)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	subIndex := (hash >> 16) % SubBucketCount
	existing, ok := shard.buckets[subIndex][key]
	if ok {
		if existing.Timestamp > val.Timestamp {
			return false
		}
		if existing.Timestamp == val.Timestamp && existing.NodeID >= val.NodeID {
			return false
		}
		oldItemHash := existing.ItemHash
		shard.subDigests[subIndex] ^= oldItemHash
		shard.shardDigest ^= oldItemHash
	}

	val.ItemHash = getItemHash(hash, val)
	shard.subDigests[subIndex] ^= val.ItemHash
	shard.shardDigest ^= val.ItemHash
	shard.buckets[subIndex][key] = val
	shard.publishReadBucket(subIndex)
	return true
}

// Delete removes a key from its shard and updates the rolling digest.
func (sm *ShardedMap) Delete(key kv.Key, hash kv.HashKey) {
	shard := sm.getShardByHash(hash)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	subIndex := (hash >> 16) % SubBucketCount
	if existing, ok := shard.buckets[subIndex][key]; ok {
		itemHash := existing.ItemHash
		shard.subDigests[subIndex] ^= itemHash
		shard.shardDigest ^= itemHash
		delete(shard.buckets[subIndex], key)
		shard.publishReadBucket(subIndex)
	}
}

// FillShardDigests populates the provided map with all shard IDs and their single intermediate XOR digests.
func (sm *ShardedMap) FillShardDigests(dst map[ShardID]Digest) {
	for i := range ShardCount {
		shard := sm[i]
		shard.mu.RLock()
		dst[ShardID(i)] = shard.shardDigest
		shard.mu.RUnlock()
	}
}

// RootDigest returns a single XOR hash of the entire database state.
func (sm *ShardedMap) RootDigest() RootDigest {
	var root RootDigest
	for i := range ShardCount {
		shard := sm[i]
		shard.mu.RLock()
		root ^= shard.shardDigest
		shard.mu.RUnlock()
	}
	return root
}

// FillDigests populates the provided map with all shard IDs and their current sub-bucket hashes.
func (sm *ShardedMap) FillDigests(dst map[ShardID]ShardDigest) {
	for i := range ShardCount {
		shard := sm[i]
		shard.mu.RLock()
		copy(dst[ShardID(i)], shard.subDigests[:])
		shard.mu.RUnlock()
	}
}

// Range invokes the callback for each key-value pair in the map.
// It locks shards one by one during iteration to minimize write contention.
func (sm *ShardedMap) Range(callback func(key kv.Key, val kv.Value) bool) {
	for i := range ShardCount {
		shard := sm[i]
		shard.mu.RLock()
		for b := range SubBucketCount {
			for k, v := range shard.buckets[b] {
				if !callback(k, v) {
					shard.mu.RUnlock()
					return
				}
			}
		}
		shard.mu.RUnlock()
	}
}

// RangeShard invokes the callback for each key-value pair in mismatched sub-buckets of a specific shard.
func (sm *ShardedMap) RangeShard(shardID ShardID, mismatchMask uint64, callback func(key kv.Key, val kv.Value)) {
	if shardID < 0 || shardID >= ShardCount {
		return
	}
	shard := sm[shardID]
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	for b := range SubBucketCount {
		if (mismatchMask & (1 << b)) != 0 {
			for k, v := range shard.buckets[b] {
				callback(k, v)
			}
		}
	}
}

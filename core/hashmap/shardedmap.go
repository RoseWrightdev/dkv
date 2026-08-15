// Package hashmap provides a concurrent-safe sharded map implementation using a persistent radix tree.
package hashmap

import (
	"math/bits"
	"sync"
	"sync/atomic"
	"unsafe"

	iradix "github.com/hashicorp/go-immutable-radix"
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

// stringToBytes performs a zero-allocation conversion from string to []byte.
// WARNING: The returned slice is read-only and must not be modified or retained.
func stringToBytes(s string) []byte {
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

// valWrapper wraps an atomic pointer to kv.Value, enabling atomic swap on updates
// without modifying the persistent radix tree structure.
type valWrapper struct {
	ptr atomic.Pointer[kv.Value]
}

// shard is a single thread-safe bucket within the ShardedMap.
type shard struct {
	buckets     [SubBucketCount]*iradix.Tree
	readBuckets [SubBucketCount]atomic.Pointer[iradix.Tree]
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
			t := iradix.New()
			s.buckets[b] = t
			s.readBuckets[b].Store(t)
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
	treePtr := shard.readBuckets[subIndex].Load()
	if treePtr == nil {
		return kv.Value{}, false
	}
	rawVal, ok := treePtr.Get(stringToBytes(key))
	if !ok {
		return kv.Value{}, false
	}
	wrapper := rawVal.(*valWrapper)
	val := wrapper.ptr.Load()
	if val == nil {
		return kv.Value{}, false
	}
	v := *val
	v.ItemHash = 0 // Clear internal-only ItemHash to preserve DeepEqual assertions in tests
	return v, true
}

// LoadData retrieves just the raw byte payload from the shard in a 100% lock-free read path.
func (sm *ShardedMap) LoadData(key kv.Key, hash kv.HashKey) ([]byte, bool) {
	shard := sm.getShardByHash(hash)
	subIndex := (hash >> 16) % SubBucketCount
	treePtr := shard.readBuckets[subIndex].Load()
	if treePtr == nil {
		return nil, false
	}
	rawVal, ok := treePtr.Get(stringToBytes(key))
	if !ok {
		return nil, false
	}
	wrapper := rawVal.(*valWrapper)
	val := wrapper.ptr.Load()
	if val == nil || val.Tombstone {
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

// Store updates the value in the correct shard and maintains the rolling digest.
func (sm *ShardedMap) Store(key kv.Key, hash kv.HashKey, val kv.Value) {
	shard := sm.getShardByHash(hash)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	subIndex := (hash >> 16) % SubBucketCount
	tree := shard.buckets[subIndex]

	rawVal, ok := tree.Get(stringToBytes(key))
	if ok {
		wrapper := rawVal.(*valWrapper)
		existing := wrapper.ptr.Load()
		if existing != nil {
			oldItemHash := existing.ItemHash
			shard.subDigests[subIndex] ^= oldItemHash
			shard.shardDigest ^= oldItemHash
		}
		val.ItemHash = getItemHash(hash, val)
		shard.subDigests[subIndex] ^= val.ItemHash
		shard.shardDigest ^= val.ItemHash

		// In-place atomic pointer swap (no tree nodes allocated or path copied!)
		wrapper.ptr.Store(&val)
		return
	}

	// New key insertion
	val.ItemHash = getItemHash(hash, val)
	shard.subDigests[subIndex] ^= val.ItemHash
	shard.shardDigest ^= val.ItemHash

	wrapper := &valWrapper{}
	wrapper.ptr.Store(&val)

	newTree, _, _ := tree.Insert([]byte(key), wrapper)
	shard.buckets[subIndex] = newTree
	shard.readBuckets[subIndex].Store(newTree)
}

// StoreLWW updates the value in the correct shard using LWW conflict resolution under a single write lock.
// It returns true if the value was stored, and false if ignored as stale.
func (sm *ShardedMap) StoreLWW(key kv.Key, hash kv.HashKey, val kv.Value) bool {
	shard := sm.getShardByHash(hash)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	subIndex := (hash >> 16) % SubBucketCount
	tree := shard.buckets[subIndex]

	rawVal, ok := tree.Get(stringToBytes(key))
	if ok {
		wrapper := rawVal.(*valWrapper)
		existing := wrapper.ptr.Load()
		if existing != nil {
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

		// In-place atomic pointer swap (no tree nodes allocated or path copied!)
		wrapper.ptr.Store(&val)
		return true
	}

	// New key insertion
	val.ItemHash = getItemHash(hash, val)
	shard.subDigests[subIndex] ^= val.ItemHash
	shard.shardDigest ^= val.ItemHash

	wrapper := &valWrapper{}
	wrapper.ptr.Store(&val)

	newTree, _, _ := tree.Insert([]byte(key), wrapper)
	shard.buckets[subIndex] = newTree
	shard.readBuckets[subIndex].Store(newTree)
	return true
}

// Delete removes a key from its shard and updates the rolling digest.
func (sm *ShardedMap) Delete(key kv.Key, hash kv.HashKey) {
	shard := sm.getShardByHash(hash)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	subIndex := (hash >> 16) % SubBucketCount
	tree := shard.buckets[subIndex]

	rawVal, ok := tree.Get(stringToBytes(key))
	if ok {
		wrapper := rawVal.(*valWrapper)
		existing := wrapper.ptr.Load()
		if existing != nil {
			itemHash := existing.ItemHash
			shard.subDigests[subIndex] ^= itemHash
			shard.shardDigest ^= itemHash
		}
		newTree, _, _ := tree.Delete([]byte(key))
		shard.buckets[subIndex] = newTree
		shard.readBuckets[subIndex].Store(newTree)
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
			tree := shard.buckets[b]
			it := tree.Root().Iterator()
			for k, v, ok := it.Next(); ok; k, v, ok = it.Next() {
				wrapper := v.(*valWrapper)
				val := wrapper.ptr.Load()
				if val != nil {
					if !callback(kv.Key(k), *val) {
						shard.mu.RUnlock()
						return
					}
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
			tree := shard.buckets[b]
			it := tree.Root().Iterator()
			for k, v, ok := it.Next(); ok; k, v, ok = it.Next() {
				wrapper := v.(*valWrapper)
				val := wrapper.ptr.Load()
				if val != nil {
					callback(kv.Key(k), *val)
				}
			}
		}
	}
}

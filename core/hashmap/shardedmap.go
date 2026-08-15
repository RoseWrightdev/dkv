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
	subCounts   [SubBucketCount]uint32
	shardDigest Digest
	shardCount  uint32
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

// mixCount folds an item count into a digest so an empty bucket can never
// collide with a non-empty one whose raw XOR happens to land on 0 (#85).
func mixCount(count uint32) uint64 {
	x := uint64(count) + 1
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

// lwwWins reports whether incoming should replace existing under last-write-wins.
//
// Ordering is the tuple (Timestamp, NodeID, ItemHash). The first two are the
// usual LWW keys; ItemHash breaks the remaining tie so that two writes carrying
// the same timestamp and the same NodeID — rapid consecutive writes from one
// node, or any client that supplies its own timestamp — still resolve instead
// of the newer one being unconditionally discarded.
//
// The third key has to be a deterministic function of the value rather than
// arrival order. Preferring whichever write landed last would let two replicas
// that saw the pair in different orders keep accepting each other's version
// forever; ranking by ItemHash makes every replica pick the same winner. Two
// byte-identical writes compare equal and are correctly a no-op.
func lwwWins(existing, incoming kv.Value) bool {
	if existing.Timestamp != incoming.Timestamp {
		return incoming.Timestamp > existing.Timestamp
	}
	if existing.NodeID != incoming.NodeID {
		return incoming.NodeID > existing.NodeID
	}
	return incoming.ItemHash > existing.ItemHash
}

// Store updates the value in the correct shard and maintains the rolling digest.
func (sm *ShardedMap) Store(key kv.Key, hash kv.HashKey, val kv.Value) {
	// 1. Locate the correct shard and acquire its write lock.
	shard := sm.getShardByHash(hash)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	// 2. Identify the sub-bucket and load the active Radix Tree.
	subIndex := (hash >> 16) % SubBucketCount
	tree := shard.buckets[subIndex]

	// 3. Query the tree to check if the key already exists.
	// We use stringToBytes for zero-allocation lookup since Get is read-only.
	rawVal, ok := tree.Get(stringToBytes(key))
	if ok {
		// Key exists: perform in-place update.
		wrapper := rawVal.(*valWrapper)
		existing := wrapper.ptr.Load()
		if existing != nil {
			// Remove the old item's hash from the rolling digests using XOR.
			oldItemHash := existing.ItemHash
			shard.subDigests[subIndex] ^= oldItemHash
			shard.shardDigest ^= oldItemHash
		}
		// Add the new item's hash to the rolling digests.
		val.ItemHash = getItemHash(hash, val)
		shard.subDigests[subIndex] ^= val.ItemHash
		shard.shardDigest ^= val.ItemHash

		// In-place atomic pointer swap (no tree nodes allocated or path copied!)
		wrapper.ptr.Store(&val)
		return
	}

	// 4. Key does not exist: Perform new key insertion.
	val.ItemHash = getItemHash(hash, val)
	shard.subDigests[subIndex] ^= val.ItemHash
	shard.shardDigest ^= val.ItemHash

	// Wrap the value in an atomic pointer wrapper.
	wrapper := &valWrapper{}
	wrapper.ptr.Store(&val)

	// Insert the key into the tree (triggers path copying) and publish the new tree root.
	newTree, _, _ := tree.Insert([]byte(key), wrapper)
	shard.buckets[subIndex] = newTree
	shard.readBuckets[subIndex].Store(newTree)
	shard.subCounts[subIndex]++
	shard.shardCount++
}

// StoreLWW updates the value in the correct shard using LWW conflict resolution under a single write lock.
// It returns true if the value was stored, and false if ignored as stale.
func (sm *ShardedMap) StoreLWW(key kv.Key, hash kv.HashKey, val kv.Value) bool {
	// 1. Locate the correct shard and acquire its write lock.
	shard := sm.getShardByHash(hash)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	// 2. Identify the sub-bucket and load the active Radix Tree.
	subIndex := (hash >> 16) % SubBucketCount
	tree := shard.buckets[subIndex]

	// The item hash doubles as the final LWW tie-breaker, so compute it before
	// conflict resolution rather than after.
	val.ItemHash = getItemHash(hash, val)

	// 3. Query the tree to check if the key already exists.
	rawVal, ok := tree.Get(stringToBytes(key))
	if ok {
		// Key exists: perform conflict resolution and update in-place.
		wrapper := rawVal.(*valWrapper)
		existing := wrapper.ptr.Load()
		if existing != nil {
			// LWW (Last-Write-Wins) conflict resolution.
			if !lwwWins(*existing, val) {
				return false
			}
			// Remove the old item's hash from the rolling digests using XOR.
			oldItemHash := existing.ItemHash
			shard.subDigests[subIndex] ^= oldItemHash
			shard.shardDigest ^= oldItemHash
		}

		// Add the new item's hash to the rolling digests.
		shard.subDigests[subIndex] ^= val.ItemHash
		shard.shardDigest ^= val.ItemHash

		// In-place atomic pointer swap (no tree nodes allocated or path copied!)
		wrapper.ptr.Store(&val)
		return true
	}

	// 4. Key does not exist: Perform new key insertion.
	shard.subDigests[subIndex] ^= val.ItemHash
	shard.shardDigest ^= val.ItemHash

	// Wrap the value in an atomic pointer wrapper.
	wrapper := &valWrapper{}
	wrapper.ptr.Store(&val)

	// Insert the key into the tree (triggers path copying) and publish the new tree root.
	newTree, _, _ := tree.Insert([]byte(key), wrapper)
	shard.buckets[subIndex] = newTree
	shard.readBuckets[subIndex].Store(newTree)
	shard.subCounts[subIndex]++
	shard.shardCount++
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
		shard.subCounts[subIndex]--
		shard.shardCount--
	}
}

// FillShardDigests populates the provided map with all shard IDs and their single intermediate XOR digests.
func (sm *ShardedMap) FillShardDigests(dst map[ShardID]Digest) {
	for i := range ShardCount {
		shard := sm[i]
		shard.mu.RLock()
		dst[ShardID(i)] = shard.shardDigest ^ mixCount(shard.shardCount)
		shard.mu.RUnlock()
	}
}

// RootDigest returns a single XOR hash of the entire database state.
func (sm *ShardedMap) RootDigest() RootDigest {
	var root RootDigest
	for i := range ShardCount {
		shard := sm[i]
		shard.mu.RLock()
		root ^= shard.shardDigest ^ mixCount(shard.shardCount)
		shard.mu.RUnlock()
	}
	return root
}

// FillDigests populates the provided map with all shard IDs and their current sub-bucket hashes.
//
// Entries that are missing or too short are allocated. Callers that recycle a
// destination map still avoid the allocation, but a plain make(map[...]) no
// longer copies into nil slices and comes back silently empty.
func (sm *ShardedMap) FillDigests(dst map[ShardID]ShardDigest) {
	if dst == nil {
		return
	}
	for i := range ShardCount {
		id := ShardID(i)
		buf := dst[id]
		if len(buf) < SubBucketCount {
			buf = make(ShardDigest, SubBucketCount)
			dst[id] = buf
		}

		shard := sm[i]
		shard.mu.RLock()
		for b := range SubBucketCount {
			buf[b] = shard.subDigests[b] ^ mixCount(shard.subCounts[b])
		}
		shard.mu.RUnlock()
	}
}

// Range invokes the callback for each key-value pair in the map.
//
// The callback runs without any shard lock held. Range's callers encode each
// entry to disk (snapshotting) or to the network (state transfer), so holding
// shard.mu across those calls blocked every Store, StoreLWW and Delete on the
// shard for the full duration of that I/O. Instead each shard's radix tree
// roots are copied under a momentary read lock; the trees are persistent and
// immutable, so iterating the copies afterwards is safe and lock-free.
//
// The trade-off is that Range is weakly consistent, which it already was across
// shard boundaries: a key updated after its shard's roots were copied may be
// reported with either the old or the new value, and a key inserted or deleted
// afterwards may be missed or still reported. Callers reconcile snapshots
// against the WAL, which is what makes that acceptable here.
func (sm *ShardedMap) Range(callback func(key kv.Key, val kv.Value) bool) {
	var roots [SubBucketCount]*iradix.Tree

	for i := range ShardCount {
		shard := sm[i]
		shard.mu.RLock()
		copy(roots[:], shard.buckets[:])
		shard.mu.RUnlock()

		for b := range SubBucketCount {
			if roots[b] == nil {
				continue
			}
			it := roots[b].Root().Iterator()
			for k, v, ok := it.Next(); ok; k, v, ok = it.Next() {
				wrapper := v.(*valWrapper)
				val := wrapper.ptr.Load()
				if val != nil {
					if !callback(kv.Key(k), *val) {
						return
					}
				}
			}
		}
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

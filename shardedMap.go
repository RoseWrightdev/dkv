package dkv

import "sync"

type Key = string
type ShardID = int32
type ShardDigest = uint64

const shardCount = 128

type Value struct {
	Data      []byte
	Timestamp int64
	NodeID    string
	Tombstone bool
}

type shard struct {
	mu sync.RWMutex
	m  map[Key]Value
}

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

func (sm *shardedMap) Load(key Key, hash hashKey) (Value, bool) {
	shard := sm.getShardByHash(hash)
	shard.mu.RLock()
	val, ok := shard.m[key]
	shard.mu.RUnlock()
	return val, ok
}

func (sm *shardedMap) Store(key Key, hash hashKey, value Value) {
	shard := sm.getShardByHash(hash)
	shard.mu.Lock()
	shard.m[key] = value
	shard.mu.Unlock()
}

func (sm *shardedMap) Delete(key Key, hash hashKey) {
	shard := sm.getShardByHash(hash)
	shard.mu.Lock()
	delete(shard.m, key)
	shard.mu.Unlock()
}

func (sm *shardedMap) Digests() map[ShardID]ShardDigest {
	digests := make(map[ShardID]ShardDigest)
	for i := range shardCount {
		digests[ShardID(i)] = sm[i].digest()
	}
	return digests
}

func (s *shard) digest() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var h uint64
	for k, v := range s.m {
		kh := hashFunc(k)
		// XOR is commutative, making the digest order-independent
		h ^= uint64(kh) ^ uint64(v.Timestamp)
	}
	return h
}

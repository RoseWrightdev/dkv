package dkv

import "sync"

type Key = string
type Value = []byte

const shardCount = 128

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

func (sm *shardedMap) Range(fn func(k, v any) bool) {
	for i := range shardCount {
		shard := sm[i]
		shard.mu.RLock()
		for k, v := range shard.m {
			if !fn(k, v) {
				shard.mu.RUnlock()
				return
			}
		}
		shard.mu.RUnlock()
	}
}

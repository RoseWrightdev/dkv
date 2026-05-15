package dkv

import "sync"

type Key = string
type Value = []byte

const shardCount = 64

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

func (sm *shardedMap) getShard(key Key) *shard {
	// Simple FNV-1a inline hash for shard selection
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	var hash uint64 = offset64
	for i := 0; i < len(key); i++ {
		hash ^= uint64(key[i])
		hash *= prime64
	}
	return sm[hash%shardCount]
}

func (sm *shardedMap) Store(key Key, value Value) {
	shard := sm.getShard(key)
	shard.mu.Lock()
	shard.m[key] = value
	shard.mu.Unlock()
}

func (sm *shardedMap) Delete(key Key) {
	shard := sm.getShard(key)
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

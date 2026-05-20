package dkv

import (
	"context"
	"math/rand/v2"
	"sync"
	"time"
)

type hashKey = uint64

// EvictReason defines the category/reason for cache eviction (e.g. TTL or Capacity).
type EvictReason int

const (
	// EvictReasonTTL indicates the entry expired based on its lifespan.
	EvictReasonTTL EvictReason = iota
	// EvictReasonCapacity indicates the entry was evicted to free up memory due to reaching limit.
	EvictReasonCapacity
)

// Evictor defines the interface for cache invalidation.
type Evictor interface {
	publish(key Key, hash hashKey)
	publishDelete(key Key, hash hashKey)
	start()
	stop()
	SetEvictCallback(func(Key, EvictReason) error)
}

type entry struct {
	expiry time.Time
	prev   *entry
	next   *entry
	key    string
	hash   hashKey
}

type lruMsg struct {
	key  string
	hash hashKey
}

type evictMsg struct {
	key    string
	reason EvictReason
}

type lruShard struct {
	ctx      context.Context
	tail     *entry
	ch       chan lruMsg
	delCh    chan hashKey
	evictCh  chan evictMsg
	head     *entry
	cancel   context.CancelFunc
	cache    map[hashKey]*entry
	onEvict  func(Key, EvictReason) error
	pool     *sync.Pool
	wg       sync.WaitGroup
	ttl      time.Duration
	mu       sync.Mutex
	capacity uint32
}

// LeastRecentlyUsed implements a thread-safe, sharded Least Recently Used (LRU) eviction service.
type LeastRecentlyUsed struct {
	shards []*lruShard
	count  int
}

// LRUConfig defines the configuration parameters for the LeastRecentlyUsed eviction service.
type LRUConfig struct {
	Capacity   uint32
	TTL        time.Duration
	ShardCount int
}

// NewLRU creates and initializes a new LeastRecentlyUsed eviction service.
func NewLRU(config LRUConfig) *LeastRecentlyUsed {
	shardCount := config.ShardCount
	if shardCount <= 0 {
		panic("dkv: LRU ShardCount must be greater than 0")
	}
	shards := make([]*lruShard, shardCount)

	// #nosec G115
	shardCountU := uint32(shardCount)

	// Distribute capacity across shards
	shardCap := config.Capacity / shardCountU
	if shardCap == 0 {
		panic("dkv: LRU Capacity must be at least equal to ShardCount")
	}

	pool := &sync.Pool{
		New: func() any {
			return &entry{}
		},
	}

	for i := range shardCount {
		ctx, cancel := context.WithCancel(context.Background())
		shards[i] = &lruShard{
			ctx:      ctx,
			cancel:   cancel,
			ch:       make(chan lruMsg, max(shardCap/2, 8192)),
			delCh:    make(chan uint64, max(shardCap/2, 8192)),
			evictCh:  make(chan evictMsg, max(shardCap/4, 4096)),
			capacity: shardCap,
			cache:    make(map[hashKey]*entry),
			ttl:      config.TTL,
			pool:     pool,
		}
	}

	return &LeastRecentlyUsed{
		shards: shards,
		count:  shardCount,
	}
}

func (lru *LeastRecentlyUsed) start() {
	for _, s := range lru.shards {
		s.start()
	}
}

func (s *lruShard) start() {
	s.wg.Add(1)
	go s.run()
}

func (lru *LeastRecentlyUsed) stop() {
	for _, s := range lru.shards {
		s.stop()
	}
}

func (s *lruShard) stop() {
	s.cancel()
	s.wg.Wait()
}

func (lru *LeastRecentlyUsed) publish(key Key, hash hashKey) {
	// Fast-path filter: immediately discard 50% of telemetry events
	// before executing any memory accesses, sharding, or queue checks.
	if rand.Uint32()&1 != 0 {
		return
	}

	shard := lru.getShardByHash(hash)
	if !shouldSample(len(shard.ch), cap(shard.ch)) {
		return
	}

	select {
	case shard.ch <- lruMsg{key: key, hash: hash}:
	default:
	}
}

func shouldSample(qLen, qCap int) bool {
	// Bypass completely if the channel is >= 80% full to eliminate lock contention.
	if qLen*5 >= qCap*4 {
		return false
	}

	// todo: add configuration options
	// 10-Tier Dynamic Exponential-Decay Sampling Tiers:
	// - < 1% full   : Sample 1-in-2    (mask 0, always publish remaining 50%)
	// - 1% - 5%     : Sample 1-in-4    (mask 1, discard 50% of remaining)
	// - 5% - 10%    : Sample 1-in-8    (mask 3, discard 75% of remaining)
	// - 10% - 20%   : Sample 1-in-16   (mask 7, discard 87.5% of remaining)
	// - 20% - 30%   : Sample 1-in-32   (mask 15)
	// - 30% - 40%   : Sample 1-in-64   (mask 31)
	// - 40% - 50%   : Sample 1-in-128  (mask 63)
	// - 50% - 60%   : Sample 1-in-256  (mask 127)
	// - 60% - 70%   : Sample 1-in-512  (mask 255)
	// - 70% - 80%   : Sample 1-in-1024 (mask 511)
	var mask uint32
	if qLen*100 < qCap {
		mask = 0
	} else if qLen*20 < qCap {
		mask = 1
	} else if qLen*10 < qCap {
		mask = 3
	} else if qLen*5 < qCap {
		mask = 7
	} else if qLen*10 < qCap*3 {
		mask = 15
	} else if qLen*5 < qCap*2 {
		mask = 31
	} else if qLen*2 < qCap {
		mask = 63
	} else if qLen*5 < qCap*3 {
		mask = 127
	} else if qLen*10 < qCap*7 {
		mask = 255
	} else {
		mask = 511
	}

	return mask == 0 || rand.Uint32()&mask == 0
}

func (lru *LeastRecentlyUsed) seen(key Key, hash hashKey) {
	lru.getShardByHash(hash).seen(key, hash)
}

func (lru *LeastRecentlyUsed) publishDelete(_ Key, hash hashKey) {
	shard := lru.getShardByHash(hash)
	select {
	case shard.delCh <- hash:
	default:
	}
}

func (lru *LeastRecentlyUsed) getShardByHash(hash hashKey) *lruShard {
	// #nosec G115
	countU := uint64(lru.count)
	idx := hash % countU
	// #nosec G115
	return lru.shards[int(idx)]
}

func (s *lruShard) run() {
	defer s.wg.Done()
	ticker := time.NewTicker(s.ttl / 10)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case msg := <-s.ch:
			s.seen(msg.key, msg.hash)
		case hKey := <-s.delCh:
			s.delete(hKey)
		case msg := <-s.evictCh:
			if s.onEvict != nil {
				_ = s.onEvict(msg.key, msg.reason)
			}
		case <-ticker.C:
			s.evictExpired()
		}
	}
}

func (s *lruShard) seen(key string, hkey hashKey) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	// #nosec G404
	jitter := time.Duration(rand.Int64N(int64(s.ttl / 10)))
	expiry := now.Add(s.ttl + jitter)

	if e, ok := s.cache[hkey]; ok {
		e.expiry = expiry
		s.remove(e)
		s.pushFront(e)
		return
	}

	// #nosec G115
	if uint32(len(s.cache)) >= s.capacity {
		s.evictOldest()
	}

	e := s.pool.Get().(*entry)
	e.key = key
	e.hash = hkey
	e.expiry = expiry
	s.cache[hkey] = e
	s.pushFront(e)
}

func (s *lruShard) evictOldest() {
	if s.tail == nil {
		return
	}
	e := s.tail
	s.remove(e)
	delete(s.cache, e.hash)

	select {
	case s.evictCh <- evictMsg{key: e.key, reason: EvictReasonCapacity}:
	default:
	}

	e.key = ""
	s.pool.Put(e)
}

func (s *lruShard) evictExpired() {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	for s.tail != nil {
		e := s.tail
		if e.expiry.After(now) {
			break
		}

		s.remove(e)
		delete(s.cache, e.hash)

		select {
		case s.evictCh <- evictMsg{key: e.key, reason: EvictReasonTTL}:
		default:
		}

		e.key = ""
		s.pool.Put(e)
	}
}

func (s *lruShard) delete(hKey hashKey) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if e, ok := s.cache[hKey]; ok {
		s.remove(e)
		delete(s.cache, hKey)
		e.key = ""
		s.pool.Put(e)
	}
}

func (s *lruShard) remove(e *entry) {
	if e.prev != nil {
		e.prev.next = e.next
	} else {
		s.head = e.next
	}
	if e.next != nil {
		e.next.prev = e.prev
	} else {
		s.tail = e.prev
	}
	e.prev = nil
	e.next = nil
}

func (s *lruShard) pushFront(e *entry) {
	e.next = s.head
	e.prev = nil
	if s.head != nil {
		s.head.prev = e
	}
	s.head = e
	if s.tail == nil {
		s.tail = e
	}
}

// SetEvictCallback sets the function to be called when an entry is evicted.
func (lru *LeastRecentlyUsed) SetEvictCallback(fn func(Key, EvictReason) error) {
	for _, s := range lru.shards {
		s.onEvict = fn
	}
}

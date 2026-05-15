package dkv

import (
	"context"
	"math/rand/v2"
	"sync"
	"time"
)

type hashKey = uint64

type Evictor interface {
	publish(key Key, hash hashKey)
	publishDelete(key Key, hash hashKey)
	start()
	stop()
	SetEvictCallback(func(Key) error)
}

type entry struct {
	key    string
	hash   hashKey
	expiry time.Time
	prev   *entry
	next   *entry
}

type lruMsg struct {
	key  string
	hash hashKey
}

type lruShard struct {
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	mu       sync.Mutex
	ch       chan lruMsg
	delCh    chan hashKey
	evictCh  chan string
	capacity uint32
	head     *entry
	tail     *entry
	cache    map[hashKey]*entry
	ttl      time.Duration
	onEvict  func(Key) error
	pool     *sync.Pool
}

type LeastRecentlyUsed struct {
	shards []*lruShard
	count  int
}

type LRUConfig struct {
	Capacity   uint32
	TTL        time.Duration
	ShardCount int
}

func NewLRU(config LRUConfig) *LeastRecentlyUsed {
	shardCount := config.ShardCount
	if shardCount <= 0 {
		panic("dkv: LRU ShardCount must be greater than 0")
	}
	shards := make([]*lruShard, shardCount)

	// Distribute capacity across shards
	shardCap := config.Capacity / uint32(shardCount)
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
			ch:       make(chan lruMsg, max(shardCap/2, 1024)),
			delCh:    make(chan uint64, max(shardCap/4, 1024)),
			evictCh:  make(chan string, max(shardCap/4, 1024)),
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
	s.wg.Add(3)
	go s.subscriber()
	go s.deleter()
	go s.evictor()
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
	shard := lru.getShardByHash(hash)
	select {
	case shard.ch <- lruMsg{key: key, hash: hash}:
	default:
	}
}

func (lru *LeastRecentlyUsed) seen(key Key, hash hashKey) {
	lru.getShardByHash(hash).seen(key, hash)
}

func (lru *LeastRecentlyUsed) publishDelete(key Key, hash hashKey) {
	shard := lru.getShardByHash(hash)
	select {
	case shard.delCh <- hash:
	default:
	}
}

func (lru *LeastRecentlyUsed) getShardByHash(hash hashKey) *lruShard {
	return lru.shards[hash%hashKey(lru.count)]
}

func (s *lruShard) subscriber() {
	defer s.wg.Done()
	for {
		select {
		case <-s.ctx.Done():
			return
		case msg := <-s.ch:
			s.seen(msg.key, msg.hash)
		}
	}
}

func (s *lruShard) deleter() {
	defer s.wg.Done()
	for {
		select {
		case <-s.ctx.Done():
			return
		case hKey := <-s.delCh:
			s.delete(hKey)
		}
	}
}

func (s *lruShard) evictor() {
	defer s.wg.Done()
	ticker := time.NewTicker(s.ttl / 10)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.evictExpired()
		case key := <-s.evictCh:
			if s.onEvict != nil {
				_ = s.onEvict(key)
			}
		}
	}
}

func (s *lruShard) seen(key string, hkey hashKey) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	jitter := time.Duration(rand.Int64N(int64(s.ttl / 10)))
	expiry := now.Add(s.ttl + jitter)

	if e, ok := s.cache[hkey]; ok {
		e.expiry = expiry
		s.remove(e)
		s.pushFront(e)
		return
	}

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
	case s.evictCh <- e.key:
	default:
	}

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
		case s.evictCh <- e.key:
		default:
		}

		s.pool.Put(e)
	}
}

func (s *lruShard) delete(hKey hashKey) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if e, ok := s.cache[hKey]; ok {
		s.remove(e)
		delete(s.cache, hKey)
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

func (lru *LeastRecentlyUsed) SetEvictCallback(fn func(Key) error) {
	for _, s := range lru.shards {
		s.onEvict = fn
	}
}

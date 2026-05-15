package dkv

import (
	"context"
	"math/rand/v2"
	"sync"
	"time"
)

type hashKey = uint64

type Evictor interface {
	publish(key Key)
	publishDelete(key Key)
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

type LeastRecentlyUsed struct {
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	mu        sync.Mutex
	ch        chan string
	delCh     chan hashKey
	evictCh   chan string
	capacity  uint32
	head      *entry
	tail      *entry
	size      uint32
	cache     map[hashKey]*entry
	ttl       time.Duration
	onEvict   func(Key) error
	entryPool sync.Pool
}

func NewLRU(capacity uint32, ttl time.Duration) *LeastRecentlyUsed {
	ctx, cancel := context.WithCancel(context.Background())

	chSize := max(capacity/2, 1024)
	delSize := max(capacity/4, 1024)
	evictSize := max(capacity/4, 1024)

	return &LeastRecentlyUsed{
		ctx:      ctx,
		cancel:   cancel,
		ch:       make(chan string, chSize),
		delCh:    make(chan hashKey, delSize),
		evictCh:  make(chan string, evictSize),
		capacity: capacity,
		cache:    make(map[hashKey]*entry),
		ttl:      ttl,
		entryPool: sync.Pool{
			New: func() any {
				return &entry{}
			},
		},
	}
}

func (lru *LeastRecentlyUsed) start() {
	lru.wg.Add(3)
	go lru.subscriber()
	go lru.deleter()
	go lru.evictor()
}

func (lru *LeastRecentlyUsed) stop() {
	lru.cancel()
	lru.wg.Wait()
}

func (lru *LeastRecentlyUsed) publish(key Key) {
	select {
	case lru.ch <- key:
	default:
	}
}

func (lru *LeastRecentlyUsed) publishDelete(key Key) {
	hKey := lru.hashKey(key)
	select {
	case lru.delCh <- hKey:
	default:
	}
}

func (lru *LeastRecentlyUsed) subscriber() {
	defer lru.wg.Done()
	for {
		select {
		case <-lru.ctx.Done():
			return
		case key := <-lru.ch:
			lru.seen(key)
		}
	}
}

func (lru *LeastRecentlyUsed) deleter() {
	defer lru.wg.Done()
	for {
		select {
		case <-lru.ctx.Done():
			return
		case hKey := <-lru.delCh:
			lru.delete(hKey)
		}
	}
}

func (lru *LeastRecentlyUsed) evictor() {
	defer lru.wg.Done()
	ticker := time.NewTicker(lru.ttl / 10)
	defer ticker.Stop()

	for {
		select {
		case <-lru.ctx.Done():
			return
		case <-ticker.C:
			lru.evictExpired()
		case key := <-lru.evictCh:
			if lru.onEvict != nil {
				_ = lru.onEvict(key)
			}
		}
	}
}

func (lru *LeastRecentlyUsed) seen(key string) {
	hkey := lru.hashKey(key)
	lru.mu.Lock()
	defer lru.mu.Unlock()

	now := time.Now()
	jitter := time.Duration(rand.Int64N(int64(lru.ttl / 10)))
	expiry := now.Add(lru.ttl + jitter)

	if e, ok := lru.cache[hkey]; ok {
		e.expiry = expiry
		lru.remove(e)
		lru.pushFront(e)
		return
	}

	if uint32(len(lru.cache)) >= lru.capacity {
		lru.evictOldest()
	}

	e := lru.entryPool.Get().(*entry)
	e.key = key
	e.hash = hkey
	e.expiry = expiry
	lru.cache[hkey] = e
	lru.pushFront(e)
}

func (lru *LeastRecentlyUsed) evictOldest() {
	if lru.tail == nil {
		return
	}
	e := lru.tail
	lru.remove(e)
	delete(lru.cache, e.hash)

	select {
	case lru.evictCh <- e.key:
	default:
	}

	lru.entryPool.Put(e)
}

func (lru *LeastRecentlyUsed) evictExpired() {
	lru.mu.Lock()
	defer lru.mu.Unlock()

	now := time.Now()
	for lru.tail != nil {
		e := lru.tail
		if e.expiry.After(now) {
			break
		}

		lru.remove(e)
		delete(lru.cache, e.hash)

		select {
		case lru.evictCh <- e.key:
		default:
		}

		lru.entryPool.Put(e)
	}
}

func (lru *LeastRecentlyUsed) delete(hKey hashKey) {
	lru.mu.Lock()
	defer lru.mu.Unlock()

	if e, ok := lru.cache[hKey]; ok {
		lru.remove(e)
		delete(lru.cache, hKey)
		lru.entryPool.Put(e)
	}
}

// Helpers for intrusive list
func (lru *LeastRecentlyUsed) remove(e *entry) {
	if e.prev != nil {
		e.prev.next = e.next
	} else {
		lru.head = e.next
	}
	if e.next != nil {
		e.next.prev = e.prev
	} else {
		lru.tail = e.prev
	}
	e.prev = nil
	e.next = nil
}

func (lru *LeastRecentlyUsed) pushFront(e *entry) {
	e.next = lru.head
	e.prev = nil
	if lru.head != nil {
		lru.head.prev = e
	}
	lru.head = e
	if lru.tail == nil {
		lru.tail = e
	}
}

func (lru *LeastRecentlyUsed) hashKey(key string) hashKey {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	var hash uint64 = offset64
	for i := 0; i < len(key); i++ {
		hash ^= uint64(key[i])
		hash *= prime64
	}
	return hash
}

func (lru *LeastRecentlyUsed) SetEvictCallback(fn func(Key) error) {
	lru.onEvict = fn
}

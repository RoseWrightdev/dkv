package dkv

import (
	"container/list"
	"context"
	"hash"
	"hash/fnv"
	"math/rand/v2"
	"sync"
	"time"
)

type hashKey = uint64

type Evictor interface {
	publish(key Key)
	publishDelete(key Key)
	SetEvictCallback(fn func(Key) error)
	start()
	stop()
}

type entry struct {
	key    string
	hash   hashKey
	expiry time.Time
}

type LeastRecentlyUsed struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	mu     sync.Mutex

	ch      chan string
	delCh   chan hashKey
	evictCh chan string

	capacity   uint32
	ll         *list.List
	cache      map[hashKey]*list.Element
	ttl        time.Duration
	onEvict    func(Key) error
	hasherPool sync.Pool
}

func NewLRU(capacity uint32, ttl time.Duration) *LeastRecentlyUsed {
	ctx, cancel := context.WithCancel(context.Background())
	if ttl < 0 {
		panic("LRU time-to-live must be greater than 0.")
	}

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
		ll:       list.New(),
		cache:    make(map[hashKey]*list.Element),
		ttl:      ttl,
		hasherPool: sync.Pool{
			New: func() any {
				return fnv.New64a()
			},
		},
	}
}

func (lru *LeastRecentlyUsed) start() {
	lru.wg.Add(3)
	go lru.subscriber(lru.ctx)
	go lru.ttlReaper(lru.ctx)
	go lru.evictionWorker(lru.ctx)
}

func (lru *LeastRecentlyUsed) stop() {
	lru.cancel()
	lru.wg.Wait()
}

func (lru *LeastRecentlyUsed) publish(key string) {
	select {
	case lru.ch <- key:
	default:
	}
}

func (lru *LeastRecentlyUsed) publishDelete(key string) {
	hkey := lru.hashKey(key)
	lru.delCh <- hkey
}

func (lru *LeastRecentlyUsed) subscriber(ctx context.Context) {
	defer lru.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case key := <-lru.ch:
			lru.seen(key)
		case hkey := <-lru.delCh:
			lru.delete(hkey)
		}
	}
}

func (lru *LeastRecentlyUsed) evictionWorker(ctx context.Context) {
	defer lru.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case key := <-lru.evictCh:
			lru.onEvict(key)
		}
	}
}

func (lru *LeastRecentlyUsed) evictExpired() {
	lru.mu.Lock()
	defer lru.mu.Unlock()

	now := time.Now()
	for lru.ll.Len() > 0 {
		elm := lru.ll.Back()
		if elm == nil {
			break
		}

		e := elm.Value.(*entry)
		if e.expiry.After(now) {
			break
		}

		lru.ll.Remove(elm)
		delete(lru.cache, e.hash)

		select {
		case lru.evictCh <- e.key:
		default:
		}
	}
}

func (lru *LeastRecentlyUsed) seen(key string) {
	hKey := lru.hashKey(key)
	lru.mu.Lock()
	defer lru.mu.Unlock()

	randErr := time.Duration(rand.IntN(50)) * time.Microsecond
	expiry := time.Now().Add(lru.ttl + randErr)

	if ent, ok := lru.cache[hKey]; ok {
		lru.ll.MoveToFront(ent)
		ent.Value.(*entry).expiry = expiry
		return
	}

	ent := lru.ll.PushFront(&entry{key: key, hash: hKey, expiry: expiry})
	lru.cache[hKey] = ent

	if uint32(lru.ll.Len()) > lru.capacity {
		// delete oldest
		elm := lru.ll.Back()
		if elm != nil {
			lru.ll.Remove(elm)
			e := elm.Value.(*entry)
			delete(lru.cache, e.hash)

			select {
			case lru.evictCh <- e.key:
			default:
			}
		}
	}
}

func (lru *LeastRecentlyUsed) delete(hKey hashKey) {
	lru.mu.Lock()
	defer lru.mu.Unlock()

	if ent, ok := lru.cache[hKey]; ok {
		lru.ll.Remove(ent)
		delete(lru.cache, hKey)
	}
}

func (lru *LeastRecentlyUsed) ttlReaper(ctx context.Context) {
	defer lru.wg.Done()
	ticker := time.NewTicker(lru.ttl / 2)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			lru.evictExpired()
		}
	}
}

func (lru *LeastRecentlyUsed) hashKey(key string) hashKey {
	h := lru.hasherPool.Get().(hash.Hash64)
	defer lru.hasherPool.Put(h)
	h.Reset()
	h.Write([]byte(key))
	return h.Sum64()
}

func (lru *LeastRecentlyUsed) SetEvictCallback(fn func(Key) error) {
	lru.onEvict = fn
}

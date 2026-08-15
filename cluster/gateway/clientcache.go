package gateway

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rosewrightdev/oryx/cluster/mesh"
	"golang.org/x/sync/singleflight"
	"google.golang.org/grpc/credentials"
)

// ClientCache caches gRPC clients for each peer node to avoid
// recreating network connections repeatedly during proxy routing.
type ClientCache struct {
	creds   credentials.TransportCredentials
	clients sync.Map
	dial    singleflight.Group
	mu      sync.Mutex
	closed  atomic.Bool
}

// NewClientCache initializes a ClientCache instance.
func NewClientCache(creds credentials.TransportCredentials) *ClientCache {
	return &ClientCache{creds: creds}
}

// newClient is a package-level indirection to NewClient so tests can count
// or delay dial attempts without changing ClientCache's public API.
var newClient = NewClient

// Get loads a cached Client for a given PeerAddress or constructs a new one if missing.
func (cc *ClientCache) Get(addr mesh.PeerAddress) (*Client, error) {
	if cc.closed.Load() {
		return nil, fmt.Errorf("client cache is closed")
	}

	// Fast path: optimistic read
	if val, ok := cc.clients.Load(addr); ok {
		return val.(*Client), nil
	}

	// Slow path: concurrent misses for the same address dedupe onto one
	// dial via singleflight, instead of each racing to open one (#73).
	v, err, _ := cc.dial.Do(string(addr), func() (any, error) {
		if val, ok := cc.clients.Load(addr); ok {
			return val.(*Client), nil
		}

		client, err := newClient(string(addr), 1*time.Second, cc.creds)
		if err != nil {
			return nil, err
		}

		cc.mu.Lock()
		if cc.closed.Load() {
			cc.mu.Unlock()
			_ = client.Close()
			return nil, fmt.Errorf("client cache is closed")
		}

		actual, loaded := cc.clients.LoadOrStore(addr, client)
		cc.mu.Unlock()

		if loaded {
			// Another goroutine beat us to it, close the one we just created to prevent leak
			_ = client.Close()
			return actual.(*Client), nil
		}

		return client, nil
	})
	if err != nil {
		return nil, err
	}
	return v.(*Client), nil
}

// Close terminates all active gRPC clients inside the cache.
func (cc *ClientCache) Close() {
	if !cc.closed.CompareAndSwap(false, true) {
		return // already closed
	}

	cc.mu.Lock()
	defer cc.mu.Unlock()

	cc.clients.Range(func(key, value any) bool {
		client := value.(*Client)
		_ = client.Close()
		cc.clients.Delete(key)
		return true
	})
}

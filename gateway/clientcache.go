package gateway

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rosewrightdev/dkv/internal/mesh"
	"google.golang.org/grpc/credentials"
)

// ClientCache caches gRPC clients for each peer node to avoid
// recreating network connections repeatedly during proxy routing.
type ClientCache struct {
	creds   credentials.TransportCredentials
	clients sync.Map
	mu      sync.Mutex
	closed  atomic.Bool
}

// NewClientCache initializes a ClientCache instance.
func NewClientCache(creds credentials.TransportCredentials) *ClientCache {
	return &ClientCache{creds: creds}
}

// Get loads a cached Client for a given PeerAddress or constructs a new one if missing.
func (cc *ClientCache) Get(addr mesh.PeerAddress) (*Client, error) {
	if cc.closed.Load() {
		return nil, fmt.Errorf("client cache is closed")
	}

	// Fast path: optimistic read
	if val, ok := cc.clients.Load(addr); ok {
		return val.(*Client), nil
	}

	// Slow path: create client and load or store
	client, err := NewClient(string(addr), 1*time.Second, cc.creds)
	if err != nil {
		return nil, err
	}

	// Re-check after dial
	if cc.closed.Load() {
		_ = client.Close()
		return nil, fmt.Errorf("client cache is closed")
	}

	actual, loaded := cc.clients.LoadOrStore(addr, client)
	if loaded {
		// Another goroutine beat us to it, close the one we just created to prevent leak
		_ = client.Close()
		return actual.(*Client), nil
	}

	return client, nil
}

// Close terminates all active gRPC clients inside the cache.
func (cc *ClientCache) Close() {
	if !cc.closed.CompareAndSwap(false, true) {
		return // already closed
	}

	cc.mu.Lock()
	defer cc.mu.Unlock()

	cc.clients.Range(func(_, value any) bool {
		client := value.(*Client)
		_ = client.Close()
		return true
	})
}

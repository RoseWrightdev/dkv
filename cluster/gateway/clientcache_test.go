package gateway

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

func TestClientCache_ConcurrentClose(t *testing.T) {
	creds := insecure.NewCredentials()
	cc := NewClientCache(creds)

	// Pre-populate some client
	_, _ = cc.Get("127.0.0.1:9091")

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for range 50 {
			_, _ = cc.Get("127.0.0.1:9091")
			time.Sleep(1 * time.Millisecond)
		}
	}()

	go func() {
		defer wg.Done()
		time.Sleep(5 * time.Millisecond)
		cc.Close()
	}()

	wg.Wait()
	// If it didn't panic, we are good.
	assert.True(t, cc.closed.Load(), "Cache should be marked closed")
	_, err := cc.Get("127.0.0.1:9091")
	assert.Error(t, err)
}

// TestClientCache_ConcurrentMissesDialOnce pins #73: concurrent misses for
// the same address must dial exactly once, not race independent dials.
func TestClientCache_ConcurrentMissesDialOnce(t *testing.T) {
	var dials atomic.Int32
	release := make(chan struct{})

	original := newClient
	t.Cleanup(func() { newClient = original })
	newClient = func(addr string, timeout time.Duration, creds credentials.TransportCredentials) (*Client, error) {
		dials.Add(1)
		<-release // hold every dialer open until the test releases them together
		return original(addr, timeout, creds)
	}

	cc := NewClientCache(insecure.NewCredentials())
	defer cc.Close()

	const n = 20
	results := make(chan *Client, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for range n {
		go func() {
			defer wg.Done()
			c, err := cc.Get("127.0.0.1:9092")
			assert.NoError(t, err)
			results <- c
		}()
	}

	// Give every goroutine a chance to reach the dial before releasing it.
	time.Sleep(20 * time.Millisecond)
	assert.Equal(t, int32(1), dials.Load(), "concurrent misses for the same address must dial only once")
	close(release)

	wg.Wait()
	close(results)

	var first *Client
	for c := range results {
		if first == nil {
			first = c
		}
		assert.Same(t, first, c, "every concurrent caller must receive the same client")
	}
}

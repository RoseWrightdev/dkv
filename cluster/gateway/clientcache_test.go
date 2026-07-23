package gateway

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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

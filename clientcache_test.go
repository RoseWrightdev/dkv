package dkv

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/credentials/insecure"
)

func TestClientCache_ConcurrentClose(t *testing.T) {
	creds := insecure.NewCredentials()
	cc := newClientCache(creds)

	// Pre-populate some client
	_, _ = cc.get("127.0.0.1:9091")

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for range 50 {
			_, _ = cc.get("127.0.0.1:9091")
			time.Sleep(1 * time.Millisecond)
		}
	}()

	go func() {
		defer wg.Done()
		time.Sleep(5 * time.Millisecond)
		cc.close()
	}()

	wg.Wait()
	// If it didn't panic, we are good.
	assert.True(t, cc.closed)
	_, err := cc.get("127.0.0.1:9091")
	assert.Error(t, err)
}

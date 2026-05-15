package dkv_test

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/rosewrightdev/dkv"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDkvIntegration(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "dkv-integ-*")
	defer os.RemoveAll(tmpDir)

	walPath := filepath.Join(tmpDir, "wal.bin")
	sssPath := filepath.Join(tmpDir, "sss.gob")

	eng, err := dkv.NewEngineBuilder().
		SetWalPath(walPath).SetSssPath(sssPath).
		SetWalSyncInterval(time.Hour).SetSssInterval(time.Hour).
		SetWalBufferSize(64 * 1024). // Added
		SetEvictionService(dkv.NewLRU(100, time.Hour)).
		GetEngine()
	require.NoError(t, err)

	server := dkv.NewServer(eng)
	lis, _ := net.Listen("tcp", "127.0.0.1:0")
	addr := lis.Addr().String()
	go func() { _ = server.Run(lis) }()
	defer server.Stop()

	client, _ := dkv.NewInsecureClient(addr, time.Second)
	defer client.Close()

	t.Run("CRUD", func(t *testing.T) {
		err := client.Set("foo", []byte("bar"))
		assert.NoError(t, err)

		val, ok, err := client.Get("foo")
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, []byte("bar"), val)

		err = client.Delete("foo")
		assert.NoError(t, err)

		_, ok, _ = client.Get("foo")
		assert.False(t, ok)
	})

	t.Run("LRU_Eviction", func(t *testing.T) {
		for i := 0; i < 150; i++ {
			_ = client.Set(fmt.Sprintf("key-%d", i), []byte("v"))
		}
		// "key-0" should be evicted as capacity is 100
		_, ok, _ := client.Get("key-0")
		assert.False(t, ok)
	})
}

func TestDkvHighPressure(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "dkv-pressure-*")
	defer os.RemoveAll(tmpDir)

	walPath := filepath.Join(tmpDir, "wal.bin")
	sssPath := filepath.Join(tmpDir, "snapshot.gob")

	eng, err := dkv.NewEngineBuilder().
		SetWalPath(walPath).SetSssPath(sssPath).
		SetWalSyncInterval(10 * time.Millisecond).
		SetSssInterval(time.Hour).
		SetWalBufferSize(64 * 1024).
		SetEvictionService(dkv.NewLRU(1000, time.Hour)).
		GetEngine()
	require.NoError(t, err)
	
	server := dkv.NewServer(eng)
	lis, _ := net.Listen("tcp", "127.0.0.1:0")
	addr := lis.Addr().String()
	go func() { _ = server.Run(lis) }()
	defer server.Stop()

	t.Run("MultiClient_Concurrency", func(t *testing.T) {
		const clientCount = 10
		const opsPerClient = 100
		var wg sync.WaitGroup
		wg.Add(clientCount)

		for i := range clientCount {
			go func(id int) {
				defer wg.Done()
				client, _ := dkv.NewInsecureClient(addr, time.Second)
				defer client.Close()

				for j := range opsPerClient {
					key := fmt.Sprintf("client-%d-key-%d", id, j)
					_ = client.Set(key, []byte("value"))
					val, ok, _ := client.Get(key)
					if !ok || string(val) != "value" {
						t.Errorf("Data mismatch for %s", key)
					}
				}
			}(i)
		}
		wg.Wait()
	})

	t.Run("MassiveDataset_Recovery", func(t *testing.T) {
		client, _ := dkv.NewInsecureClient(addr, time.Second)
		for i := range 10000 {
			_ = client.Set(fmt.Sprintf("bulk-%d", i), []byte("data"))
		}
		_ = eng.Snapshot()

		server.Stop()
		client.Close()

		eng2, err := dkv.NewEngineBuilder().
			SetWalPath(walPath).SetSssPath(sssPath).
			SetWalSyncInterval(10 * time.Millisecond).
			SetSssInterval(time.Hour).
			SetWalBufferSize(64 * 1024).
			SetEvictionService(dkv.NewLRU(20000, time.Hour)).
			GetEngine()
		require.NoError(t, err)
		
		server2 := dkv.NewServer(eng2)
		lis2, _ := net.Listen("tcp", addr)
		go func() { _ = server2.Run(lis2) }()
		defer server2.Stop()

		client2, _ := dkv.NewInsecureClient(addr, time.Second)
		defer client2.Close()

		val, ok, _ := client2.Get("bulk-9999")
		assert.True(t, ok)
		assert.Equal(t, []byte("data"), val)
	})
}

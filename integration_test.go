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

	walDir := filepath.Join(tmpDir, "wal")
	sssPath := filepath.Join(tmpDir, "sss.gob")

	eng, err := dkv.NewEngineBuilder().
		Default().
		SetWalPath(walDir).SetSssPath(sssPath).
		SetWalSyncInterval(time.Hour).SetSssInterval(time.Hour).
		SetWalBufferSize(64 * 1024).
		SetWalSegments(4).
		SetEvictionService(dkv.NewLRU(dkv.LRUConfig{Capacity: 100, TTL: time.Hour, ShardCount: 16})).
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

	walDir := filepath.Join(tmpDir, "wal")
	sssPath := filepath.Join(tmpDir, "snapshot.gob")

	eng, err := dkv.NewEngineBuilder().
		Default().
		SetWalPath(walDir).SetSssPath(sssPath).
		SetWalSyncInterval(10 * time.Millisecond).
		SetSssInterval(time.Hour).
		SetWalBufferSize(64 * 1024).
		SetWalSegments(4).
		SetEvictionService(dkv.NewLRU(dkv.LRUConfig{Capacity: 1000, TTL: time.Hour, ShardCount: 16})).
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
		for i := range 1000 {
			_ = client.Set(fmt.Sprintf("bulk-%d", i), []byte("data"))
		}
		_ = eng.Snapshot()

		server.Stop()
		client.Close()

		eng2, err := dkv.NewEngineBuilder().
			Default().
			SetWalPath(walDir).SetSssPath(sssPath).
			SetWalSyncInterval(10 * time.Millisecond).
			SetSssInterval(time.Hour).
			SetWalBufferSize(64 * 1024).
			SetWalSegments(4).
			SetEvictionService(dkv.NewLRU(dkv.LRUConfig{Capacity: 2000, TTL: time.Hour, ShardCount: 16})).
			GetEngine()
		require.NoError(t, err)

		server2 := dkv.NewServer(eng2)
		lis2, _ := net.Listen("tcp", addr)
		go func() { _ = server2.Run(lis2) }()
		defer server2.Stop()

		client2, _ := dkv.NewInsecureClient(addr, time.Second)
		defer client2.Close()

		val, ok, _ := client2.Get("bulk-999")
		assert.True(t, ok)
		assert.Equal(t, []byte("data"), val)
	})
}

func TestDistributedCluster(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "dkv-cluster-*")
	defer os.RemoveAll(tmpDir)

	createNode := func(name string, gossipPort, grpcPort int, seeds []string) (dkv.Engine, *dkv.Grpc, string) {
		nodeDir := filepath.Join(tmpDir, name)
		os.MkdirAll(nodeDir, 0755)

		eng, err := dkv.NewEngineBuilder().
			Default().
			SetWalPath(filepath.Join(nodeDir, "wal")).
			SetSssPath(filepath.Join(nodeDir, "sss.gob")).
			SetGossipInterval(500 * time.Millisecond).
			SetClusterConfig(dkv.ClusterConfig{
				NodeName:  name,
				BindPort:  gossipPort,
				SeedNodes: seeds,
				GrpcPort:  grpcPort,
			}).
			GetEngine()
		if err != nil {
			t.Fatal(err)
		}

		srv := dkv.NewServer(eng)
		addr := fmt.Sprintf("127.0.0.1:%d", grpcPort)
		lis, err := net.Listen("tcp", addr)
		if err != nil {
			t.Fatal(err)
		}
		go func() { _ = srv.Run(lis) }()
		return eng, srv, addr
	}

	// Start Node 1
	_, s1, addr1 := createNode("node1", 12001, 13001, nil)
	defer s1.Stop()

	// Start Node 2
	_, s2, addr2 := createNode("node2", 12002, 13002, []string{"127.0.0.1:12001"})
	defer s2.Stop()

	// Wait for discovery
	time.Sleep(1 * time.Second)

	c1, _ := dkv.NewInsecureClient(addr1, time.Second)
	c2, _ := dkv.NewInsecureClient(addr2, time.Second)
	defer c1.Close()
	defer c2.Close()

	t.Run("Replication_Gossip", func(t *testing.T) {
		err := c1.Set("key1", []byte("val1"))
		assert.NoError(t, err)

		// Wait for gossip
		time.Sleep(500 * time.Millisecond)

		val, ok, _ := c2.Get("key1")
		assert.True(t, ok, "Data should replicate to node 2")
		assert.Equal(t, []byte("val1"), val)
	})

	t.Run("EventualConsistency_Sync", func(t *testing.T) {
		// Stop node 2
		s2.Stop()
		time.Sleep(200 * time.Millisecond)

		// Set key on node 1 while node 2 is down
		err := c1.Set("key2", []byte("val2"))
		assert.NoError(t, err)

		// Restart Node 2
		_, s2_new, _ := createNode("node2", 12002, 13002, []string{"127.0.0.1:12001"})
		defer s2_new.Stop()

		// Wait for anti-entropy sync (interval is 500ms)
		time.Sleep(2 * time.Second)

		c2_new, _ := dkv.NewInsecureClient(addr2, time.Second)
		defer c2_new.Close()

		val, ok, _ := c2_new.Get("key2")
		assert.True(t, ok, "Node 2 should have synced key2 via Anti-Entropy after restart")
		assert.Equal(t, []byte("val2"), val)
	})
}

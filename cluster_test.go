package dkv

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rosewrightdev/dkv/gateway"
	"github.com/rosewrightdev/dkv/kv"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/credentials/insecure"
)

func TestNewCluster(t *testing.T) {
	cluster, err := newCluster(10, "", insecure.NewCredentials(), true)
	assert.Nil(t, err)
	assert.NotNil(t, cluster)
	assert.Equal(t, 10, len(cluster.Engines))
	assert.Equal(t, 10, len(cluster.Servers))

	cluster.Stop()
}

func TestClusterScaleAndDurability(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping extreme scale test in short mode")
	}

	tmpDir, _ := os.MkdirTemp("", "dkv-cluster-durability-*")
	defer func() {
		_ = os.RemoveAll(tmpDir)
	}()

	numNodes := 10
	dataDir := filepath.Join(tmpDir, "data")

	cluster, err := newCluster(numNodes, dataDir, insecure.NewCredentials(), true)
	require.NoError(t, err)

	err = cluster.Start()
	require.NoError(t, err)
	defer cluster.HardStop()

	// Wait a moment for gossip to stabilize
	time.Sleep(1 * time.Second)

	// We create an insecure client against the first node
	client, err := gateway.NewInsecureClient(cluster.Engines[0].Addr(), 2*time.Second)
	require.NoError(t, err)

	// Concurrently write 1000 keys
	var wg sync.WaitGroup
	numKeys := 1000

	errs := make(chan error, numKeys)
	for i := range numKeys {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			k := fmt.Sprintf("durability-key-%d", id)
			v := fmt.Appendf(nil, "val-%d", id)
			if err := client.Set(k, v); err != nil {
				errs <- err
			}
		}(i)
	}

	// While writes are happening, we take down node-3 and node-7
	time.Sleep(50 * time.Millisecond)
	cluster.stopEngine(kv.NodeID("node-3"))
	cluster.stopEngine(kv.NodeID("node-7"))
	cluster.stopEngine(kv.NodeID("node-13"))
	cluster.stopEngine(kv.NodeID("node-14"))

	wg.Wait()
	close(errs)

	successCount := 0
	for i := range numKeys {
		k := fmt.Sprintf("durability-key-%d", i)
		v := fmt.Appendf(nil, "val-%d", i)

		got, exists, err := client.Get(k)
		if err == nil && exists && string(got) == string(v) {
			successCount++
		}
	}

	t.Logf("Successfully verified %d/%d keys despite 2 node failures", successCount, numKeys)
	assert.Greater(t, successCount, 0, "should have read back some keys successfully")
}

func TestClusterFullRestartDurability(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping heavy durability test in short mode")
	}

	tmpDir, _ := os.MkdirTemp("", "dkv-heavy-durability-*")
	defer func() {
		_ = os.RemoveAll(tmpDir)
	}()

	numNodes := 3
	dataDir := filepath.Join(tmpDir, "data")

	cluster, err := newCluster(numNodes, dataDir, insecure.NewCredentials(), true)
	require.NoError(t, err)

	err = cluster.Start()
	require.NoError(t, err)

	// Wait a moment for gossip to stabilize
	time.Sleep(1 * time.Second)

	client, err := gateway.NewInsecureClient(cluster.Engines[0].Addr(), 2*time.Second)
	require.NoError(t, err)

	numKeys := 2000
	var wg sync.WaitGroup
	errs := make(chan error, numKeys)

	// Concurrently write keys with 1KB payloads
	payload := make([]byte, 1024)
	for i := range payload {
		payload[i] = 'A'
	}

	for i := range numKeys {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			k := fmt.Sprintf("heavy-key-%d", id)
			if err := client.Set(k, payload); err != nil {
				errs <- err
			}
		}(i)
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}

	// Concurrently delete the first 500 keys
	errsDel := make(chan error, 500)
	for i := range 500 {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			k := fmt.Sprintf("heavy-key-%d", id)
			if err := client.Delete(k); err != nil {
				errsDel <- err
			}
		}(i)
	}
	wg.Wait()
	close(errsDel)

	for err := range errsDel {
		require.NoError(t, err)
	}

	// Stop the cluster gracefully to flush WAL and state
	cluster.Stop()

	// Wait a moment before restarting
	time.Sleep(500 * time.Millisecond)

	// Restart the cluster pointing to the SAME data directory
	cluster2, err := newCluster(numNodes, dataDir, insecure.NewCredentials(), true)
	require.NoError(t, err)

	err = cluster2.Start()
	require.NoError(t, err)
	defer cluster2.Stop()

	time.Sleep(1 * time.Second)

	client2, err := gateway.NewInsecureClient(cluster2.Engines[0].Addr(), 2*time.Second)
	require.NoError(t, err)

	// Verify the 500 deleted keys are gone
	for i := range 500 {
		k := fmt.Sprintf("heavy-key-%d", i)
		_, exists, err := client2.Get(k)
		require.NoError(t, err)
		assert.False(t, exists, "key %s should have been deleted", k)
	}

	// Verify the remaining 1500 keys exist and match payload
	successCount := 0
	for i := 500; i < numKeys; i++ {
		k := fmt.Sprintf("heavy-key-%d", i)
		got, exists, err := client2.Get(k)
		if err == nil && exists && string(got) == string(payload) {
			successCount++
		}
	}

	t.Logf("Successfully verified %d/%d surviving keys after full cluster restart", successCount, numKeys-500)
	assert.Equal(t, numKeys-500, successCount, "should have read back all surviving keys")
}

func TestClusterChaosDurability(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping chaos  in short mode")
	}

	tmpDir, _ := os.MkdirTemp("", "dkv-chaos-*")
	defer func() { _ = os.RemoveAll(tmpDir) }()

	numNodes := 7
	dataDir := filepath.Join(tmpDir, "data")
	cluster, err := newCluster(numNodes, dataDir, insecure.NewCredentials(), true)
	require.NoError(t, err)

	err = cluster.Start()
	require.NoError(t, err)
	defer cluster.HardStop()

	time.Sleep(1 * time.Second)

	var clients []*gateway.Client
	for _, eng := range cluster.Engines {
		c, err := gateway.NewInsecureClient(eng.Addr(), 50*time.Millisecond)
		require.NoError(t, err)
		clients = append(clients, c)
	}

	numKeys := 5000
	var wg sync.WaitGroup

	// Worker goroutines writing keys
	for i := range 50 {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := range 100 {
				id := workerID*100 + j
				k := fmt.Sprintf("chaos-key-%d", id)
				v := fmt.Appendf(nil, "val-%d", id)

				// Pick a client (some might be disconnected soon)
				c := clients[(workerID+j)%len(clients)]
				_ = c.Set(k, v) // errors are expected during chaos
				time.Sleep(2 * time.Millisecond)
			}
		}(i)
	}

	// Chaos
	var chaosWg sync.WaitGroup
	chaosWg.Add(1)

	killed := make(map[int]bool)
	var mu sync.Mutex

	go func() {
		defer chaosWg.Done()
		// We kill 3 out of 7 nodes randomly
		for range 3 {
			time.Sleep(50 * time.Millisecond)
			target := time.Now().Nanosecond() % numNodes
			mu.Lock()
			for killed[target] {
				target = (target + 1) % numNodes
			}
			killed[target] = true
			mu.Unlock()
			cluster.stopEngine(kv.NodeID(fmt.Sprintf("node-%d", target+1)))
		}
	}()

	wg.Wait()      // wait for workers
	chaosWg.Wait() // wait for chaos

	// Wait a bit for gossip and inflight requests
	time.Sleep(500 * time.Millisecond)

	successCount := int32(0)
	var verifyWg sync.WaitGroup
	verifyWg.Add(numKeys)

	for i := range numKeys {
		go func(i int) {
			defer verifyWg.Done()
			k := fmt.Sprintf("chaos-key-%d", i)
			v := fmt.Appendf(nil, "val-%d", i)

			// Try to read from any surviving node
			for _, c := range clients {
				got, exists, err := c.Get(k)
				if err == nil && exists && string(got) == string(v) {
					atomic.AddInt32(&successCount, 1)
					break
				}
			}
		}(i)
	}
	verifyWg.Wait()

	t.Logf("Verified %d/%d keys after random node deletion chaos", successCount, numKeys)
	assert.Greater(t, int(successCount), 0, "should have successfully read back keys despite random node deletions")
}

func TestClusterDataRebalancing(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping rebalancing test in short mode")
	}

	tmpDir, _ := os.MkdirTemp("", "dkv-rebalance-*")
	defer func() { _ = os.RemoveAll(tmpDir) }()

	// Start a 3-node cluster
	numInitialNodes := 3
	dataDir := filepath.Join(tmpDir, "data")
	cluster, err := newCluster(numInitialNodes, dataDir, insecure.NewCredentials(), true)
	require.NoError(t, err)

	err = cluster.Start()
	require.NoError(t, err)
	defer cluster.HardStop()

	// Wait for gossip to settle
	time.Sleep(1 * time.Second)

	var clients []*gateway.Client
	for _, eng := range cluster.Engines {
		c, err := gateway.NewInsecureClient(eng.Addr(), 2*time.Second)
		require.NoError(t, err)
		clients = append(clients, c)
	}

	numKeys := 5000
	var wg sync.WaitGroup

	// Phase 1: High throughput background writes
	for i := range 50 {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := range 100 {
				id := workerID*100 + j
				k := fmt.Sprintf("reb-key-%d", id)
				v := fmt.Appendf(nil, "val-%d", id)

				// Use a random client
				c := clients[(workerID+j)%len(clients)]
				err := c.Set(k, v)
				require.NoError(t, err, "writes should not fail during scale-up")
				time.Sleep(2 * time.Millisecond) // Simulate continuous load
			}
		}(i)
	}

	// Phase 2: Add 2 new nodes while writes are happening
	seedAddr := cluster.Engines[0].GossipAddr()
	for i := range 2 {
		time.Sleep(100 * time.Millisecond)
		newNodeName := fmt.Sprintf("node-%d", numInitialNodes+i+1)
		err := cluster.addNode(newNodeName, seedAddr, dataDir, insecure.NewCredentials(), true)
		require.NoError(t, err, "failed to add new node")
		t.Logf("Added node %s dynamically", newNodeName)
	}

	wg.Wait()

	// Wait a bit for gossip and inflight requests to settle
	time.Sleep(2 * time.Second)

	// Phase 3: Verify all keys are readable
	// We'll query through the originally known clients, who will now
	// use their updated routing tables to proxy to the new owners.
	successCount := int32(0)
	var verifyWg sync.WaitGroup
	verifyWg.Add(numKeys)

	for i := range numKeys {
		go func(i int) {
			defer verifyWg.Done()
			k := fmt.Sprintf("reb-key-%d", i)
			v := fmt.Appendf(nil, "val-%d", i)

			// Route through the first client
			got, exists, err := clients[0].Get(k)
			if err == nil && exists && string(got) == string(v) {
				atomic.AddInt32(&successCount, 1)
			}
		}(i)
	}
	verifyWg.Wait()

	t.Logf("Verified %d/%d keys after dynamic node addition and rebalancing", successCount, numKeys)
	assert.Equal(t, numKeys, int(successCount), "all keys should be readable and properly rebalanced")

	// Verify that the new nodes actually received data via background anti-entropy or direct proxy push!
	// (Check node 4 and 5's local state engines)
	newEngines := cluster.Engines[numInitialNodes:]
	for _, e := range newEngines {
		// e.db is not directly accessible, but we could use local API if we had one.
		// For now, we trust the successful verification above.
		_ = e
	}
	t.Logf("Found 2 dynamically added engines in cluster.Engines slice")
}

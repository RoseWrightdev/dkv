package dkv_test

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rosewrightdev/dkv"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEngineOperations(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "dkv-test-*")
	defer os.RemoveAll(tmpDir)

	walPath := filepath.Join(tmpDir, "wal")
	sssPath := filepath.Join(tmpDir, "snapshot.bin")

	eng, err := dkv.NewEngineBuilder().
		Default().
		SetWalPath(walPath).
		SetSssPath(sssPath).
		SingleNode().
		GetEngine()

	require.NoError(t, err)
	eng.Start()
	defer eng.Stop()

	key, val := "foo", []byte("bar")

	// Set
	err = eng.Set(key, val)
	assert.NoError(t, err)

	// Get
	got, ok := eng.Get(key)
	assert.True(t, ok)
	assert.Equal(t, val, got)

	// Update
	val2 := []byte("baz")
	err = eng.Set(key, val2)
	assert.NoError(t, err)
	got, ok = eng.Get(key)
	assert.True(t, ok)
	assert.Equal(t, val2, got)

	// Delete
	err = eng.Delete(key)
	assert.NoError(t, err)
	_, ok = eng.Get(key)
	assert.False(t, ok)
}

func TestClusterScale(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping scale test in short mode")
	}

	tmpDir, _ := os.MkdirTemp("", "dkv-scale-*")
	defer os.RemoveAll(tmpDir)

	count := 3
	var engines []dkv.Engine
	var clients []*dkv.Client

	seedPort := 11001
	seedAddr := fmt.Sprintf("127.0.0.1:%d", seedPort)

	for i := 0; i < count; i++ {
		name := fmt.Sprintf("node-%d", i)
		nodeDir := filepath.Join(tmpDir, name)
		os.MkdirAll(nodeDir, 0755)

		gossipPort := 11001 + i
		grpcPort := 12001 + i

		eb := dkv.NewEngineBuilder().
			Default().
			SetWalPath(filepath.Join(nodeDir, "wal")).
			SetSssPath(filepath.Join(nodeDir, "sss.gob")).
			SetGossipInterval(500 * time.Millisecond).
			SetNodeName(name).
			SetBindPort(gossipPort).
			SetGrpcPort(grpcPort)

		if i > 0 {
			eb.SetSeedNodes([]string{seedAddr})
		}

		eng, err := eb.GetEngine()
		require.NoError(t, err)
		engines = append(engines, eng)

		lis, _ := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", grpcPort))
		server := dkv.NewServer(eng)
		go func() { _ = server.Run(lis) }()

		client, _ := dkv.NewInsecureClient(fmt.Sprintf("127.0.0.1:%d", grpcPort), time.Second)
		clients = append(clients, client)
	}

	for _, e := range engines {
		e.Start()
	}
	defer func() {
		for _, e := range engines {
			e.Stop()
		}
	}()

	// Wait for stabilization
	time.Sleep(2 * time.Second)

	// Parallel writes to different nodes
	for i := 0; i < 100; i++ {
		go func(id int) {
			k := fmt.Sprintf("key-%d", id)
			v := []byte(fmt.Sprintf("val-%d", id))
			client := clients[id%count]
			_ = client.Set(k, v)
		}(i)
	}

	time.Sleep(2 * time.Second)

	// Verify replication (read from different nodes)
	for i := 0; i < 100; i++ {
		k := fmt.Sprintf("key-%d", i)
		v := []byte(fmt.Sprintf("val-%d", i))
		client := clients[(i+1)%count]
		got, exists, err := client.Get(k)
		assert.NoError(t, err)
		assert.True(t, exists, "Key %s should exist on node %d", k, (i+1)%count)
		assert.Equal(t, v, got)
	}
}

func TestAntiEntropyRecovery(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "dkv-recovery-*")
	defer os.RemoveAll(tmpDir)

	// Node 1 setup
	n1Dir := filepath.Join(tmpDir, "node1")
	os.MkdirAll(n1Dir, 0755)
	eng1, _ := dkv.NewEngineBuilder().
		Default().
		SetWalPath(filepath.Join(n1Dir, "wal")).
		SetSssPath(filepath.Join(n1Dir, "sss.gob")).
		SetGossipInterval(500 * time.Millisecond).
		SetNodeName("node1").
		SetBindPort(13001).
		SetGrpcPort(14001).
		GetEngine()

	lis1, _ := net.Listen("tcp", "127.0.0.1:14001")
	server1 := dkv.NewServer(eng1)
	go func() { _ = server1.Run(lis1) }()
	eng1.Start()
	defer eng1.Stop()

	// Write data while Node 2 is DOWN
	for i := 0; i < 10; i++ {
		_ = eng1.Set(fmt.Sprintf("rec-%d", i), []byte("data"))
	}

	// Node 2 setup (joins Node 1)
	n2Dir := filepath.Join(tmpDir, "node2")
	os.MkdirAll(n2Dir, 0755)
	eng2, _ := dkv.NewEngineBuilder().
		Default().
		SetWalPath(filepath.Join(n2Dir, "wal")).
		SetSssPath(filepath.Join(n2Dir, "sss.gob")).
		SetGossipInterval(500 * time.Millisecond).
		SetNodeName("node2").
		SetBindPort(13002).
		SetSeedNodes([]string{"127.0.0.1:13001"}).
		SetGrpcPort(14002).
		GetEngine()

	lis2, _ := net.Listen("tcp", "127.0.0.1:14002")
	server2 := dkv.NewServer(eng2)
	go func() { _ = server2.Run(lis2) }()
	eng2.Start()
	defer eng2.Stop()

	// Wait for anti-entropy to catch up Node 2
	time.Sleep(3 * time.Second)

	// Verify Node 2 has the data
	for i := 0; i < 10; i++ {
		_, ok := eng2.Get(fmt.Sprintf("rec-%d", i))
		assert.True(t, ok, "Node 2 should have recovered rec-%d", i)
	}
}

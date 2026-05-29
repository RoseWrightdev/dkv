package server

import (
	"fmt"
	"path/filepath"
	"sync/atomic"

	"github.com/rosewrightdev/dkv"
	"github.com/rosewrightdev/dkv/kv"
	"google.golang.org/grpc/credentials"
)

var nextBasePort int32 = 15000

func getNextBasePort(nodeCount int) int {
	for {
		current := atomic.LoadInt32(&nextBasePort)
		// #nosec G115
		next := current + int32(nodeCount*2)
		if next >= 60000 {
			// #nosec G115
			next = 15000 + int32(nodeCount*2)
		}
		if atomic.CompareAndSwapInt32(&nextBasePort, current, next) {
			return int(next) - nodeCount*2
		}
	}
}

// Cluster represents a group of dkv engines and servers.
type Cluster struct {
	Engines []dkv.Engine
	Servers []*Grpc
}

// Stop gracefully shuts down all engines and servers in the cluster.
func (c *Cluster) Stop() {
	for _, s := range c.Servers {
		s.Stop()
	}
	for _, e := range c.Engines {
		e.Stop()
	}
}

// HardStop immediately shuts down all engines and servers in the cluster.
func (c *Cluster) HardStop() {
	for _, s := range c.Servers {
		s.HardStop()
	}
}

// NewCluster creates a new cluster.
func NewCluster(nodeCount int, dataDir string, creds credentials.TransportCredentials) (*Cluster, error) {
	return newCluster(nodeCount, dataDir, creds, false)
}

func newCluster(nodeCount int, dataDir string, creds credentials.TransportCredentials, fastTest bool) (*Cluster, error) {
	cluster := &Cluster{}
	var seedAddr string
	basePort := getNextBasePort(nodeCount)

	for i := range nodeCount {
		name := fmt.Sprintf("node-%d", i+1)

		eb := dkv.NewEngineBuilder().
			Default()

		if fastTest {
			eb.FastTest()
		}

		eb.SetNodeID(kv.NodeID(name)).
			SetCreds(creds).
			SetBindPort(basePort + i*2).
			SetGrpcPort(basePort + i*2 + 1)

		if dataDir != "" {
			eb.SetWalPath(filepath.Join(dataDir, name, "wal")).
				SetSnpPath(filepath.Join(dataDir, name, "snp.gob"))
		}

		if i > 0 {
			eb.SetSeedNodes([]string{seedAddr})
		}

		engine, err := eb.Build()
		if err != nil {
			cluster.Stop()
			return nil, err
		}

		if i == 0 {
			seedAddr = engine.GossipAddr()
		}

		cluster.Engines = append(cluster.Engines, engine)
		server := NewServer(engine)
		cluster.Servers = append(cluster.Servers, server)
	}

	return cluster, nil
}

// Start starts all engines and servers in the cluster.
func (c *Cluster) Start() error {
	ch := make(chan error, len(c.Engines))
	for i, engine := range c.Engines {
		server := c.Servers[i]

		go func(e dkv.Engine, s *Grpc) {
			e.Start()
			err := s.Run()
			if err != nil {
				ch <- err
			}
		}(engine, server)

		select {
		case err := <-ch:
			c.Stop()
			return err
		default:
		}
	}

	return nil
}

// stopEngine stops a specific engine and its corresponding server for integration tests.
func (c *Cluster) stopEngine(id kv.NodeID) {
	for i, engine := range c.Engines {
		if engine.NodeID() == id {
			c.Servers[i].HardStop()
			return
		}
	}
}

func (c *Cluster) addNode(name string, seedAddr string, dataDir string, creds credentials.TransportCredentials, fastTest bool) error {
	basePort := getNextBasePort(1)

	eb := dkv.NewEngineBuilder().
		Default()

	if fastTest {
		eb.FastTest()
	}

	eb.SetNodeID(kv.NodeID(name)).
		SetCreds(creds).
		SetBindPort(basePort).
		SetGrpcPort(basePort + 1).
		SetSeedNodes([]string{seedAddr})

	if dataDir != "" {
		eb.SetWalPath(filepath.Join(dataDir, name, "wal")).
			SetSnpPath(filepath.Join(dataDir, name, "snp.gob"))
	}

	engine, err := eb.Build()
	if err != nil {
		return err
	}

	server := NewServer(engine)

	c.Engines = append(c.Engines, engine)
	c.Servers = append(c.Servers, server)

	// Start the newly added node
	go func() {
		engine.Start()
		_ = server.Run() // Run blocks
	}()

	return nil
}

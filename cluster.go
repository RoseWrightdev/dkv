package dkv

import (
	"fmt"
	"path/filepath"

	"google.golang.org/grpc/credentials"
)

// Cluster represents a group of dkv engines and servers.
type Cluster struct {
	Engines []Engine
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
	cluster := &Cluster{}
	var seedAddr string
	basePort := 55000 // use a high port range to avoid conflicts

	for i := range nodeCount {
		name := fmt.Sprintf("node-%d", i+1)

		eb := NewEngineBuilder().
			Default().
			FastTest().
			SetNodeID(NodeID(name)).
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

		go func(e Engine, s *Grpc) {
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
func (c *Cluster) stopEngine(id NodeID) {
	for i, engine := range c.Engines {
		if engine.NodeID() == id {
			c.Servers[i].HardStop()
			return
		}
	}
}

func (c *Cluster) addNode(name string, seedAddr string, dataDir string, creds credentials.TransportCredentials) error {
	basePort := 55000 + len(c.Engines)*2

	eb := NewEngineBuilder().
		Default().
		FastTest().
		SetNodeID(NodeID(name)).
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

// Package server manages server cluster nodes and high-level routing handlers.
package server

import (
	"fmt"
	"net"
	"path/filepath"
	"sync/atomic"

	"github.com/rosewrightdev/oryx"
	"github.com/rosewrightdev/oryx/kv"
	"google.golang.org/grpc/credentials"
)

var nextBasePort int32 = 18000

func getNextBasePort(nodeCount int) int {
	for {
		current := atomic.LoadInt32(&nextBasePort)
		stride := int32(nodeCount * 20)
		next := current + stride
		if next >= 60000 {
			next = 18000 + stride
		}
		if atomic.CompareAndSwapInt32(&nextBasePort, current, next) {
			return findAvailableBasePort(int(current), nodeCount)
		}
	}
}

func findAvailableBasePort(startPort int, nodeCount int) int {
	for p := startPort; p < startPort+500; p += 2 {
		available := true
		for i := range nodeCount * 2 {
			l, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", p+i))
			if err != nil {
				available = false
				break
			}
			_ = l.Close()
		}
		if available {
			return p
		}
	}
	return startPort
}

// Cluster represents a group of oryx databases and servers.
type Cluster struct {
	Databases []oryx.Database
	Servers   []*Grpc
}

// Stop gracefully shuts down all databases and servers in the cluster.
func (c *Cluster) Stop() {
	for _, s := range c.Servers {
		s.Stop()
	}
	for _, e := range c.Databases {
		e.Stop()
	}
}

// HardStop immediately shuts down all databases and servers in the cluster.
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

		dbBuilder := oryx.NewDatabaseBuilder().
			Default()

		if fastTest {
			dbBuilder.FastTest()
		}

		dbBuilder.SetNodeID(kv.NodeID(name)).
			SetCreds(creds).
			SetBindPort(basePort + i*2).
			SetGrpcPort(basePort + i*2 + 1)

		if dataDir != "" {
			dbBuilder.SetWalPath(filepath.Join(dataDir, name, "wal")).
				SetSnpPath(filepath.Join(dataDir, name, "snp.gob"))
		}

		if i > 0 {
			dbBuilder.SetSeedNodes([]string{seedAddr})
		}

		database, err := dbBuilder.Build()
		if err != nil {
			cluster.Stop()
			return nil, err
		}

		if i == 0 {
			seedAddr = database.GossipAddr()
		}

		cluster.Databases = append(cluster.Databases, database)
		server := NewServer(database)
		cluster.Servers = append(cluster.Servers, server)
	}

	return cluster, nil
}

// Start starts all databases and servers in the cluster.
func (c *Cluster) Start() error {
	ch := make(chan error, len(c.Databases))
	for i, database := range c.Databases {
		server := c.Servers[i]

		go func(d oryx.Database, s *Grpc) {
			d.Start()
			err := s.Run()
			if err != nil {
				ch <- err
			}
		}(database, server)

		select {
		case err := <-ch:
			c.Stop()
			return err
		default:
		}
	}

	return nil
}

// stopDatabase stops a specific database and its corresponding server for integration tests.
func (c *Cluster) stopDatabase(id kv.NodeID) {
	for i, database := range c.Databases {
		if database.NodeID() == id {
			c.Servers[i].HardStop()
			return
		}
	}
}

func (c *Cluster) addNode(name string, seedAddr string, dataDir string, creds credentials.TransportCredentials, fastTest bool) error {
	basePort := getNextBasePort(1)

	dbBuilder := oryx.NewDatabaseBuilder().
		Default()

	if fastTest {
		dbBuilder.FastTest()
	}

	dbBuilder.SetNodeID(kv.NodeID(name)).
		SetCreds(creds).
		SetBindPort(basePort).
		SetGrpcPort(basePort + 1).
		SetSeedNodes([]string{seedAddr})

	if dataDir != "" {
		dbBuilder.SetWalPath(filepath.Join(dataDir, name, "wal")).
			SetSnpPath(filepath.Join(dataDir, name, "snp.gob"))
	}

	database, err := dbBuilder.Build()
	if err != nil {
		return err
	}

	server := NewServer(database)

	c.Databases = append(c.Databases, database)
	c.Servers = append(c.Servers, server)

	// Start the newly added node
	go func() {
		database.Start()
		_ = server.Run() // Run blocks
	}()

	return nil
}

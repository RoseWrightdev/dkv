// Package dkv provides a highly-concurrent, partitioned key-value database engine.
package dkv

import (
	"fmt"
	"time"

	pb "github.com/rosewrightdev/dkv/api"
	"github.com/rosewrightdev/dkv/cluster"
	"github.com/rosewrightdev/dkv/cluster/entropy"
	"github.com/rosewrightdev/dkv/cluster/mesh"
	"github.com/rosewrightdev/dkv/core"
	"github.com/rosewrightdev/dkv/core/clock"
	"github.com/rosewrightdev/dkv/core/evict"
	"github.com/rosewrightdev/dkv/kv"
	"google.golang.org/grpc/credentials"
)

// Database defines the core storage and replication database interface of the dkv node.
type Database interface {
	Get(key kv.Key) ([]byte, bool)
	Set(key kv.Key, value []byte) error
	Delete(key kv.Key) error
	Owner(key kv.Key) kv.NodeID
	NodeID() kv.NodeID
	Start()
	Stop()
	SyncPull(pullConfig *entropy.PullConfig) ([]*pb.SetRequest, []*pb.DeleteRequest, error)
	SyncPush(sets []*pb.SetRequest, deletes []*pb.DeleteRequest) error
	Addr() string
	GossipAddr() string
	Mesh() mesh.Mesher
}

// Engine is a type alias for Database for backward compatibility.
type Engine = Database

// DatabaseConfig specifies the parameters required to initialize and run a dkv Database.
type DatabaseConfig struct {
	evt            evict.Evictor
	clock          clock.Clocker
	creds          credentials.TransportCredentials
	walPath        string
	snpPath        string
	meshConfig     mesh.Config
	walInterval    time.Duration
	snpInterval    time.Duration
	walSegments    int
	gossipInterval time.Duration
	walBufferSize  uint32
}

// EngineConfig is a type alias for DatabaseConfig for backward compatibility.
type EngineConfig = DatabaseConfig

func newDatabase(config DatabaseConfig) (Database, error) {
	coreConfig := core.Config{
		Evt:           config.evt,
		Clock:         config.clock,
		WalPath:       config.walPath,
		SnpPath:       config.snpPath,
		WalInterval:   config.walInterval,
		SnpInterval:   config.snpInterval,
		WalSegments:   config.walSegments,
		WalBufferSize: config.walBufferSize,
		NodeID:        config.meshConfig.NodeID,
	}

	coreEng, err := core.NewEngine(coreConfig)
	if err != nil {
		return nil, err
	}

	if config.meshConfig.SingleNode {
		return &singleNodeAdapter{
			Engine: coreEng,
			config: config.meshConfig,
		}, nil
	}

	clusterConfig := cluster.Config{
		MeshConfig:     config.meshConfig,
		Creds:          config.creds,
		GossipInterval: config.gossipInterval,
	}

	node, err := cluster.NewNode(coreEng, clusterConfig)
	if err != nil {
		return nil, err
	}

	return node, nil
}

type singleNodeAdapter struct {
	core.Engine
	config mesh.Config
}

func (s *singleNodeAdapter) Core() core.Engine {
	return s.Engine
}

func (s *singleNodeAdapter) Owner(_ kv.Key) kv.NodeID {
	return s.config.NodeID
}

func (s *singleNodeAdapter) NodeID() kv.NodeID {
	return s.config.NodeID
}

func (s *singleNodeAdapter) SyncPull(_ *entropy.PullConfig) ([]*pb.SetRequest, []*pb.DeleteRequest, error) {
	return nil, nil, nil
}

func (s *singleNodeAdapter) SyncPush(_ []*pb.SetRequest, _ []*pb.DeleteRequest) error {
	return nil
}

func (s *singleNodeAdapter) Addr() string {
	if s.config.BindAddr == "" {
		panic("dkv: bind address not configured")
	}
	return fmt.Sprintf("%s:%d", s.config.BindAddr, s.config.GrpcPort)
}

func (s *singleNodeAdapter) GossipAddr() string {
	if s.config.BindAddr == "" {
		panic("dkv: bind address not configured")
	}
	return fmt.Sprintf("%s:%d", s.config.BindAddr, s.config.BindPort)
}

func (s *singleNodeAdapter) Mesh() mesh.Mesher {
	return &mesh.NopMesh{}
}

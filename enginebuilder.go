package dkv

import (
	"fmt"
	"time"

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// EngineBuilder provides a fluent API for constructing and configuring a dkv engine.
type EngineBuilder struct {
	walPath         string
	sssPath         string
	walSyncInterval time.Duration
	sssInterval     time.Duration
	walBufferSize   uint32
	walSegments     int
	evictionService Evictor
	clock           Clock
	clusterBuilder  *ClusterConfigBuilder
	gossipInterval  time.Duration
	creds           credentials.TransportCredentials
}

// NewEngineBuilder initializes a new EngineBuilder instance with default sub-builders.
func NewEngineBuilder() *EngineBuilder {
	return &EngineBuilder{
		clusterBuilder: NewClusterConfigBuilder(),
	}
}

// NewDefaultEngine constructs a default dkv engine configuration.
func NewDefaultEngine(walPath, sssPath string) (Engine, error) {
	return NewEngineBuilder().Default().SetWalPath(walPath).SetSssPath(sssPath).GetEngine()
}

// Default populates the EngineBuilder with sensible default values.
func (eb *EngineBuilder) Default() *EngineBuilder {
	eb.walSyncInterval = 500 * time.Millisecond
	eb.sssInterval = 5 * time.Minute
	eb.walBufferSize = 64 * 1024
	eb.walSegments = 16
	eb.evictionService = NewLRU(LRUConfig{Capacity: 10000, TTL: 24 * time.Hour, ShardCount: 16})
	eb.clock = NewHLC()
	eb.gossipInterval = 10 * time.Second
	eb.clusterBuilder = NewClusterConfigBuilder()
	return eb
}

// SetWalPath sets the path to the write-ahead log directory.
func (eb *EngineBuilder) SetWalPath(path string) *EngineBuilder {
	eb.walPath = path
	return eb
}

// SetSssPath sets the path to the snapshot file.
func (eb *EngineBuilder) SetSssPath(path string) *EngineBuilder {
	eb.sssPath = path
	return eb
}

// SetSssInterval sets the snapshot interval.
func (eb *EngineBuilder) SetSssInterval(interval time.Duration) *EngineBuilder {
	eb.sssInterval = interval
	return eb
}

// SetWalSyncInterval sets the sync interval for the write-ahead log.
func (eb *EngineBuilder) SetWalSyncInterval(interval time.Duration) *EngineBuilder {
	eb.walSyncInterval = interval
	return eb
}

// SetWalBufferSize sets the buffer size for the write-ahead log.
func (eb *EngineBuilder) SetWalBufferSize(size uint32) *EngineBuilder {
	eb.walBufferSize = size
	return eb
}

// SetWalSegments sets the maximum number of log segments.
func (eb *EngineBuilder) SetWalSegments(count int) *EngineBuilder {
	eb.walSegments = count
	return eb
}

// SetEvictionService sets the eviction service instance.
func (eb *EngineBuilder) SetEvictionService(evictor Evictor) *EngineBuilder {
	eb.evictionService = evictor
	return eb
}

// SetClock sets the clock implementation for generating timestamps.
func (eb *EngineBuilder) SetClock(clock Clock) *EngineBuilder {
	eb.clock = clock
	return eb
}

// SetCluster sets the cluster configuration builder.
func (eb *EngineBuilder) SetCluster(cb *ClusterConfigBuilder) *EngineBuilder {
	eb.clusterBuilder = cb
	return eb
}

// Proxy methods for ClusterConfigBuilder
// These allow for a flatter API while maintaining modularity under the hood.

// SetNodeID sets the unique node ID for cluster identity.
func (eb *EngineBuilder) SetNodeID(id NodeID) *EngineBuilder {
	eb.clusterBuilder.SetNodeID(id)
	return eb
}

// SetReplicationFactor sets the replication factor for the cluster.
func (eb *EngineBuilder) SetReplicationFactor(n int) *EngineBuilder {
	eb.clusterBuilder.SetReplicationFactor(n)
	return eb
}

// SetBindAddr sets the bind address for gossip membership.
func (eb *EngineBuilder) SetBindAddr(addr string) *EngineBuilder {
	eb.clusterBuilder.SetBindAddr(addr)
	return eb
}

// SetBindPort sets the bind port for gossip membership.
func (eb *EngineBuilder) SetBindPort(port int) *EngineBuilder {
	eb.clusterBuilder.SetBindPort(port)
	return eb
}

// SetAdvertiseAddr sets the address advertised to other cluster nodes.
func (eb *EngineBuilder) SetAdvertiseAddr(addr string) *EngineBuilder {
	eb.clusterBuilder.SetAdvertiseAddr(addr)
	return eb
}

// SetSeedNodes sets the seed nodes to join upon startup.
func (eb *EngineBuilder) SetSeedNodes(seeds []string) *EngineBuilder {
	eb.clusterBuilder.SetSeedNodes(seeds)
	return eb
}

// SetGrpcPort sets the gRPC API port.
func (eb *EngineBuilder) SetGrpcPort(port int) *EngineBuilder {
	eb.clusterBuilder.SetGrpcPort(port)
	return eb
}

// SingleNode configures the engine to run in single-node mode.
func (eb *EngineBuilder) SingleNode() *EngineBuilder {
	eb.clusterBuilder.SingleNode()
	return eb
}

// SetGossipInterval sets the gossip communication interval.
func (eb *EngineBuilder) SetGossipInterval(interval time.Duration) *EngineBuilder {
	eb.gossipInterval = interval
	return eb
}

// SetTransportCredentials sets the transport credentials for secure node-to-node connections.
func (eb *EngineBuilder) SetTransportCredentials(creds credentials.TransportCredentials) *EngineBuilder {
	eb.creds = creds
	return eb
}

// SetInsecure configures insecure gRPC connections for development.
func (eb *EngineBuilder) SetInsecure() *EngineBuilder {
	eb.creds = insecure.NewCredentials()
	return eb
}

// FastTest optimizes cluster parameters for quick unit/integration testing.
func (eb *EngineBuilder) FastTest() *EngineBuilder {
	eb.clusterBuilder.EnableFastTest()
	return eb
}

// GetEngine validates the configuration and returns a new Engine instance.
func (eb *EngineBuilder) GetEngine() (Engine, error) {
	if isUnit(eb.walPath) {
		return nil, fmt.Errorf("required eb.walPath is unset; configure eb.walPath with SetWalPath(path string)")
	}

	if isUnit(eb.sssPath) {
		return nil, fmt.Errorf("required eb.sssPath is unset; configure eb.sssPath with SetSssPath(path string)")
	}

	if isUnit(eb.walSyncInterval) {
		return nil, fmt.Errorf("required eb.walSyncInterval is unset; configure eb.walSyncInterval with SetWalSyncInterval(interval time.Duration)")
	}

	if isUnit(eb.sssInterval) {
		return nil, fmt.Errorf("required eb.sssInterval is unset; configure eb.sssInterval with SetSssInterval(interval time.Duration)")
	}

	if eb.creds == nil {
		return nil, fmt.Errorf("transport credentials are required; use SetTransportCredentials(creds) or SetInsecure() for development")
	}

	if isUnit(eb.walBufferSize) {
		return nil, fmt.Errorf("required eb.walBufferSize is unset; configure eb.walBufferSize with SetWalBufferSize(size uint32)")
	}

	if isUnit(eb.walSegments) {
		return nil, fmt.Errorf("required eb.walSegments is unset; configure eb.walSegments with SetWalSegments(count int)")
	}

	if eb.clock == nil {
		return nil, fmt.Errorf("required eb.clock is unset; configure eb.clock with SetClock(clock Clock)")
	}

	clusterConfig := eb.clusterBuilder.Build()

	if !clusterConfig.SingleNode {
		// GrpcPort 0 is allowed for dynamic allocation (e.g., in tests)
		if isUnit(eb.gossipInterval) {
			return nil, fmt.Errorf("required eb.gossipInterval is unset for distributed mode; configure it via SetGossipInterval")
		}
	}

	config := EngineConfig{
		walPath:              eb.walPath,
		sssPath:              eb.sssPath,
		walSyncInterval:      eb.walSyncInterval,
		sssInterval:          eb.sssInterval,
		walBufferSize:        eb.walBufferSize,
		walSegments:          eb.walSegments,
		evictionService:      eb.evictionService,
		clock:                eb.clock,
		clusterConfig:        clusterConfig,
		gossipInterval:       eb.gossipInterval,
		transportCredentials: eb.creds,
	}

	return newEngine(config)
}

func isUnit[T comparable](val T) bool {
	var zero T
	return zero == val
}

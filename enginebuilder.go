package dkv

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net"
	"time"

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// EngineBuilder provides a fluent API for constructing and configuring a dkv engine.
type EngineBuilder struct {
	evt            Evictor
	clock          Clock
	creds          credentials.TransportCredentials
	meshBuilder    *MeshConfigBuilder
	walPath        string
	snpPath        string
	walInterval    time.Duration
	snpInterval    time.Duration
	walSegments    int
	gossipInterval time.Duration
	walBufferSize  uint32
}

// NewEngineBuilder initializes a new EngineBuilder instance with default sub-builders.
func NewEngineBuilder() *EngineBuilder {
	return &EngineBuilder{
		meshBuilder: NewMeshConfigBuilder(),
	}
}

// NewDefaultEngine constructs a default dkv engine configuration.
func NewDefaultEngine(walPath, snpPath string) (Engine, error) {
	return NewEngineBuilder().Default().SetWalPath(walPath).SetSnpPath(snpPath).Build()
}

// Default populates the EngineBuilder with sensible default values.
func (eb *EngineBuilder) Default() *EngineBuilder {
	eb.walInterval = 500 * time.Millisecond
	eb.snpInterval = 5 * time.Minute
	eb.walBufferSize = 64 * 1024
	eb.walSegments = 16
	eb.evt = NewLRU(LRUConfig{Capacity: 10000, TTL: 24 * time.Hour, ShardCount: 16})
	eb.clock = NewHLC()
	eb.gossipInterval = 10 * time.Second
	eb.meshBuilder = NewMeshConfigBuilder()

	// Autoselect NodeID to a SHA-256 hash
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	hash := sha256.Sum256(b)
	nodeID := hex.EncodeToString(hash[:])
	eb.meshBuilder.SetNodeID(NodeID(nodeID))

	// Find dynamic next available ports
	bindAddr := "127.0.0.1"
	gossipPort := nextAvailablePort(bindAddr, 7946)
	grpcPort := nextAvailablePort(bindAddr, 50051)

	eb.meshBuilder.SetBindAddr(bindAddr)
	eb.meshBuilder.SetBindPort(gossipPort)
	eb.meshBuilder.SetGrpcPort(grpcPort)
	eb.meshBuilder.SetReplicationFactor(3)

	eb.walPath = "data/wal"
	eb.snpPath = "data/snapshot.bin"
	return eb
}

// SetWalPath sets the path to the write-ahead log directory.
func (eb *EngineBuilder) SetWalPath(path string) *EngineBuilder {
	eb.walPath = path
	return eb
}

// SetSnpPath sets the path to the snapshot file.
func (eb *EngineBuilder) SetSnpPath(path string) *EngineBuilder {
	eb.snpPath = path
	return eb
}

// SetSnpInterval sets the snapshot interval.
func (eb *EngineBuilder) SetSnpInterval(interval time.Duration) *EngineBuilder {
	eb.snpInterval = interval
	return eb
}

// SetWalInterval sets the sync interval for the write-ahead log.
func (eb *EngineBuilder) SetWalInterval(interval time.Duration) *EngineBuilder {
	eb.walInterval = interval
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

// SetEvictor sets the eviction service instance.
func (eb *EngineBuilder) SetEvictor(evt Evictor) *EngineBuilder {
	eb.evt = evt
	return eb
}

// SetClock sets the clock implementation for generating timestamps.
func (eb *EngineBuilder) SetClock(clock Clock) *EngineBuilder {
	eb.clock = clock
	return eb
}

// SetCluster sets the cluster configuration builder.
func (eb *EngineBuilder) SetCluster(cb *MeshConfigBuilder) *EngineBuilder {
	eb.meshBuilder = cb
	return eb
}

// Proxy methods for MeshConfigBuilder
// These allow for a flatter API while maintaining modularity under the hood.

// SetNodeID sets the unique node ID for cluster identity.
func (eb *EngineBuilder) SetNodeID(id NodeID) *EngineBuilder {
	eb.meshBuilder.SetNodeID(id)
	return eb
}

// SetReplicationFactor sets the replication factor for the cluster.
func (eb *EngineBuilder) SetReplicationFactor(n int) *EngineBuilder {
	eb.meshBuilder.SetReplicationFactor(n)
	return eb
}

// SetBindAddr sets the bind address for gossip membership.
func (eb *EngineBuilder) SetBindAddr(addr string) *EngineBuilder {
	eb.meshBuilder.SetBindAddr(addr)
	return eb
}

// SetBindPort sets the bind port for gossip membership.
func (eb *EngineBuilder) SetBindPort(port int) *EngineBuilder {
	eb.meshBuilder.SetBindPort(port)
	return eb
}

// SetAdvertiseAddr sets the address advertised to other cluster nodes.
func (eb *EngineBuilder) SetAdvertiseAddr(addr string) *EngineBuilder {
	eb.meshBuilder.SetAdvertiseAddr(addr)
	return eb
}

// SetSeedNodes sets the seed nodes to join upon startup.
func (eb *EngineBuilder) SetSeedNodes(seeds []string) *EngineBuilder {
	eb.meshBuilder.SetSeedNodes(seeds)
	return eb
}

// SetGrpcPort sets the gRPC API port.
func (eb *EngineBuilder) SetGrpcPort(port int) *EngineBuilder {
	eb.meshBuilder.SetGrpcPort(port)
	return eb
}

// SingleNode configures the engine to run in single-node mode.
func (eb *EngineBuilder) SingleNode() *EngineBuilder {
	eb.meshBuilder.SingleNode()
	return eb
}

// SetGossipInterval sets the gossip communication interval.
func (eb *EngineBuilder) SetGossipInterval(interval time.Duration) *EngineBuilder {
	eb.gossipInterval = interval
	return eb
}

// Setcreds sets the transport credentials for secure node-to-node connections.
func (eb *EngineBuilder) SetCreds(creds credentials.TransportCredentials) *EngineBuilder {
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
	eb.meshBuilder.EnableFastTest()
	return eb
}

// Build validates the configuration and returns a new Engine instance.
func (eb *EngineBuilder) Build() (Engine, error) {
	if isUnit(eb.walPath) {
		return nil, fmt.Errorf("required eb.walPath is unset; configure eb.walPath with SetWalPath(path string)")
	}

	if isUnit(eb.snpPath) {
		return nil, fmt.Errorf("required eb.snpPath is unset; configure eb.snpPath with SetSnpPath(path string)")
	}

	if isUnit(eb.walInterval) {
		return nil, fmt.Errorf("required eb.walInterval is unset; configure eb.walInterval with SetWalInterval(interval time.Duration)")
	}

	if isUnit(eb.snpInterval) {
		return nil, fmt.Errorf("required eb.snpInterval is unset; configure eb.snpInterval with SetSnpInterval(interval time.Duration)")
	}

	if eb.creds == nil {
		return nil, fmt.Errorf("transport credentials are required; use Setcreds(creds) or SetInsecure() for development")
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

	MeshConfig := eb.meshBuilder.Build()

	if !MeshConfig.SingleNode {
		// GrpcPort 0 is allowed for dynamic allocation (e.g., in tests)
		if isUnit(eb.gossipInterval) {
			return nil, fmt.Errorf("required eb.gossipInterval is unset for distributed mode; configure it via SetGossipInterval")
		}
	}

	config := EngineConfig{
		walPath:        eb.walPath,
		snpPath:        eb.snpPath,
		walInterval:    eb.walInterval,
		snpInterval:    eb.snpInterval,
		walBufferSize:  eb.walBufferSize,
		walSegments:    eb.walSegments,
		evt:            eb.evt,
		clock:          eb.clock,
		meshConfig:     MeshConfig,
		gossipInterval: eb.gossipInterval,
		creds:          eb.creds,
	}

	return newEngine(config)
}

func isUnit[T comparable](val T) bool {
	var zero T
	return zero == val
}

func nextAvailablePort(addr string, startPort int) int {
	for port := startPort; port < startPort+1000; port++ {
		lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", addr, port))
		if err == nil {
			_ = lis.Close()
			return port
		}
	}
	return startPort
}

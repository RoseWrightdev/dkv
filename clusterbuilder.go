package dkv

// ClusterConfigBuilder provides a fluent API for configuring dkv's distribution layer.
type ClusterConfigBuilder struct {
	config ClusterConfig
}

// NewClusterConfigBuilder initializes a new ClusterConfigBuilder with production-ready defaults.
func NewClusterConfigBuilder() *ClusterConfigBuilder {
	return &ClusterConfigBuilder{
		config: ClusterConfig{
			SingleNode: false, // Distributed by default
			BindPort:   0,     // Auto-port by default
			GrpcPort:   0,     // Auto-port by default
		},
	}
}

// SingleNode explicitly disables the distribution layer.
func (cb *ClusterConfigBuilder) SingleNode() *ClusterConfigBuilder {
	cb.config.SingleNode = true
	return cb
}

// SetNodeID sets a unique identifier for this node in the cluster.
func (cb *ClusterConfigBuilder) SetNodeID(id NodeID) *ClusterConfigBuilder {
	cb.config.NodeID = id
	return cb
}

// SetBindAddr sets the address memberlist will bind to for gossip (UDP/TCP).
func (cb *ClusterConfigBuilder) SetBindAddr(addr string) *ClusterConfigBuilder {
	cb.config.BindAddr = addr
	return cb
}

// SetBindPort sets the port memberlist will use for gossip.
func (cb *ClusterConfigBuilder) SetBindPort(port int) *ClusterConfigBuilder {
	cb.config.BindPort = port
	return cb
}

// SetAdvertiseAddr sets the address other nodes should use to reach this node.
func (cb *ClusterConfigBuilder) SetAdvertiseAddr(addr string) *ClusterConfigBuilder {
	cb.config.AdvertiseAddr = addr
	return cb
}

// SetSeedNodes provides a list of existing nodes to join upon startup.
func (cb *ClusterConfigBuilder) SetSeedNodes(seeds []string) *ClusterConfigBuilder {
	cb.config.SeedNodes = seeds
	return cb
}

// SetGrpcPort sets the port of the dkv gRPC API.
func (cb *ClusterConfigBuilder) SetGrpcPort(port int) *ClusterConfigBuilder {
	cb.config.GrpcPort = port
	return cb
}

// EnableFastTest optimizes cluster timing for rapid test execution.
func (cb *ClusterConfigBuilder) EnableFastTest() *ClusterConfigBuilder {
	cb.config.FastTest = true
	return cb
}

// SetReplicationFactor determines how many copies of each key are stored in the cluster.
func (cb *ClusterConfigBuilder) SetReplicationFactor(n int) *ClusterConfigBuilder {
	cb.config.ReplicationFactor = n
	return cb
}

// Build returns the final ClusterConfig.
func (cb *ClusterConfigBuilder) Build() ClusterConfig {
	return cb.config
}

package dkv

// MeshConfigBuilder provides a fluent API for configuring dkv's distribution layer.
type MeshConfigBuilder struct {
	config MeshConfig
}

// NewMeshConfigBuilder initializes a new MeshConfigBuilder with production-ready defaults.
func NewMeshConfigBuilder() *MeshConfigBuilder {
	return &MeshConfigBuilder{
		config: MeshConfig{
			SingleNode: false, // Distributed by default
			BindPort:   0,     // Auto-port by default
			GrpcPort:   0,     // Auto-port by default
		},
	}
}

// SingleNode explicitly disables the distribution layer.
func (cb *MeshConfigBuilder) SingleNode() *MeshConfigBuilder {
	cb.config.SingleNode = true
	return cb
}

// SetNodeID sets a unique identifier for this node in the cluster.
func (cb *MeshConfigBuilder) SetNodeID(id NodeID) *MeshConfigBuilder {
	cb.config.NodeID = id
	return cb
}

// SetBindAddr sets the address memberlist will bind to for gossip (UDP/TCP).
func (cb *MeshConfigBuilder) SetBindAddr(addr string) *MeshConfigBuilder {
	cb.config.BindAddr = addr
	return cb
}

// SetBindPort sets the port memberlist will use for gossip.
func (cb *MeshConfigBuilder) SetBindPort(port int) *MeshConfigBuilder {
	cb.config.BindPort = port
	return cb
}

// SetAdvertiseAddr sets the address other nodes should use to reach this node.
func (cb *MeshConfigBuilder) SetAdvertiseAddr(addr string) *MeshConfigBuilder {
	cb.config.AdvertiseAddr = addr
	return cb
}

// SetSeedNodes provides a list of existing nodes to join upon startup.
func (cb *MeshConfigBuilder) SetSeedNodes(seeds []string) *MeshConfigBuilder {
	cb.config.SeedNodes = seeds
	return cb
}

// SetGrpcPort sets the port of the dkv gRPC API.
func (cb *MeshConfigBuilder) SetGrpcPort(port int) *MeshConfigBuilder {
	cb.config.GrpcPort = port
	return cb
}

// EnableFastTest optimizes cluster timing for rapid test execution.
func (cb *MeshConfigBuilder) EnableFastTest() *MeshConfigBuilder {
	cb.config.FastTest = true
	return cb
}

// SetReplicationFactor determines how many copies of each key are stored in the cluster.
func (cb *MeshConfigBuilder) SetReplicationFactor(n int) *MeshConfigBuilder {
	cb.config.ReplicationFactor = n
	return cb
}

// Build returns the final MeshConfig.
func (cb *MeshConfigBuilder) Build() MeshConfig {
	return cb.config
}

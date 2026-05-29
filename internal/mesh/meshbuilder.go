package mesh

import (
	"github.com/rosewrightdev/dkv/kv"
)

// MeshConfigBuilder provides an abstraction to configfer MeshConfig

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
func (mb *MeshConfigBuilder) SingleNode() *MeshConfigBuilder {
	mb.config.SingleNode = true
	return mb
}

// Distributed explicitly enables the distribution layer.
func (mb *MeshConfigBuilder) Distributed() *MeshConfigBuilder {
	mb.config.SingleNode = false
	return mb
}

// SetNodeID sets a unique identifier for this node in the cluster.
func (mb *MeshConfigBuilder) SetNodeID(id kv.NodeID) *MeshConfigBuilder {
	mb.config.NodeID = id
	return mb
}

// SetBindAddr sets the address memberlist will bind to for gossip (UDP/TCP).
func (mb *MeshConfigBuilder) SetBindAddr(addr string) *MeshConfigBuilder {
	mb.config.BindAddr = addr
	return mb
}

// SetBindPort sets the port memberlist will use for gossip.
func (mb *MeshConfigBuilder) SetBindPort(port int) *MeshConfigBuilder {
	mb.config.BindPort = port
	return mb
}

// SetAdvertiseAddr sets the address other nodes should use to reach this node.
func (mb *MeshConfigBuilder) SetAdvertiseAddr(addr string) *MeshConfigBuilder {
	mb.config.AdvertiseAddr = addr
	return mb
}

// SetSeedNodes provides a list of existing nodes to join upon startup.
func (mb *MeshConfigBuilder) SetSeedNodes(seeds []string) *MeshConfigBuilder {
	mb.config.SeedNodes = seeds
	return mb
}

// SetGrpcPort sets the port of the dkv gRPC API.
func (mb *MeshConfigBuilder) SetGrpcPort(port int) *MeshConfigBuilder {
	mb.config.GrpcPort = port
	return mb
}

// EnableFastTest optimizes cluster timing for rapid test execution.
func (mb *MeshConfigBuilder) EnableFastTest() *MeshConfigBuilder {
	mb.config.FastTest = true
	return mb
}

// SetReplicationFactor determines how many copies of each key are stored in the cluster.
func (mb *MeshConfigBuilder) SetReplicationFactor(n int) *MeshConfigBuilder {
	mb.config.ReplicationFactor = n
	return mb
}

// Build returns the final MeshConfig.
func (mb *MeshConfigBuilder) Build() MeshConfig {
	return mb.config
}

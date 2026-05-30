package mesh

import (
	"github.com/rosewrightdev/dkv/kv"
)

// ConfigBuilder provides an abstraction to configure Config

// ConfigBuilder provides a fluent API for configuring dkv's distribution layer.
type ConfigBuilder struct {
	config Config
}

// NewConfigBuilder initializes a new ConfigBuilder with production-ready defaults.
func NewConfigBuilder() *ConfigBuilder {
	return &ConfigBuilder{
		config: Config{
			SingleNode: false, // Distributed by default
			BindPort:   0,     // Auto-port by default
			GrpcPort:   0,     // Auto-port by default
		},
	}
}

// SingleNode explicitly disables the distribution layer.
func (mb *ConfigBuilder) SingleNode() *ConfigBuilder {
	mb.config.SingleNode = true
	return mb
}

// Distributed explicitly enables the distribution layer.
func (mb *ConfigBuilder) Distributed() *ConfigBuilder {
	mb.config.SingleNode = false
	return mb
}

// SetNodeID sets a unique identifier for this node in the cluster.
func (mb *ConfigBuilder) SetNodeID(id kv.NodeID) *ConfigBuilder {
	mb.config.NodeID = id
	return mb
}

// SetBindAddr sets the address memberlist will bind to for gossip (UDP/TCP).
func (mb *ConfigBuilder) SetBindAddr(addr string) *ConfigBuilder {
	mb.config.BindAddr = addr
	return mb
}

// SetBindPort sets the port memberlist will use for gossip.
func (mb *ConfigBuilder) SetBindPort(port int) *ConfigBuilder {
	mb.config.BindPort = port
	return mb
}

// SetAdvertiseAddr sets the address other nodes should use to reach this node.
func (mb *ConfigBuilder) SetAdvertiseAddr(addr string) *ConfigBuilder {
	mb.config.AdvertiseAddr = addr
	return mb
}

// SetSeedNodes provides a list of existing nodes to join upon startup.
func (mb *ConfigBuilder) SetSeedNodes(seeds []string) *ConfigBuilder {
	mb.config.SeedNodes = seeds
	return mb
}

// SetGrpcPort sets the port of the dkv gRPC API.
func (mb *ConfigBuilder) SetGrpcPort(port int) *ConfigBuilder {
	mb.config.GrpcPort = port
	return mb
}

// EnableFastTest optimizes cluster timing for rapid test execution.
func (mb *ConfigBuilder) EnableFastTest() *ConfigBuilder {
	mb.config.FastTest = true
	return mb
}

// SetReplicationFactor determines how many copies of each key are stored in the cluster.
func (mb *ConfigBuilder) SetReplicationFactor(n int) *ConfigBuilder {
	mb.config.ReplicationFactor = n
	return mb
}

// Build returns the final Config.
func (mb *ConfigBuilder) Build() Config {
	return mb.config
}

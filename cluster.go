package dkv

import (
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/hashicorp/memberlist"
)

// PeerAddress represents the network address (IP:Port) of a dkv node.
type PeerAddress string

// Cluster defines the interface for distributed node discovery and replication.
type Cluster interface {
	Broadcast(msg []byte)
	Members() []PeerAddress
	Owner(key Key) NodeID
	GetOwners(key Key, n int) []NodeID
	AddressForNode(nodeID NodeID) PeerAddress
	start() error
	stop() error
}

// ClusterConfig holds configuration for decentralized node discovery and membership.
type ClusterConfig struct {
	// SingleNode explicitly disables the distribution layer when set to true.
	// dkv is distributed by default.
	SingleNode bool
	// ReplicationFactor determines how many copies of each key are stored in the cluster.
	ReplicationFactor int
	// NodeID is a unique identifier for this node in the cluster.
	NodeID NodeID
	// BindAddr is the address memberlist will bind to for gossip (UDP/TCP).
	BindAddr string
	// BindPort is the port memberlist will use.
	BindPort int
	// AdvertiseAddr is the address other nodes should use to reach this node.
	AdvertiseAddr string
	// SeedNodes is a list of existing nodes to join upon startup.
	SeedNodes []string
	// GrpcPort is the port of the dkv gRPC API, shared with peers via metadata.
	GrpcPort int
	// FastTest optimizes internal intervals for rapid test execution.
	FastTest bool
}

// ClusterService manages the lifecycle of the node within a gossip-based cluster.
type ClusterService struct {
	config           ClusterConfig
	memberList       *memberlist.Memberlist
	broadcasts       *memberlist.TransmitLimitedQueue
	onMessage        func([]byte)
	getLocalState    func() []byte
	mergeRemoteState func([]byte)
	ring             *HashRing
}

// newClusterService initializes a new ClusterService instance.
func newClusterService(
	config ClusterConfig,
	onMessage func([]byte),
	getLocalState func() []byte,
	mergeRemoteState func([]byte),
) (*ClusterService, error) {
	ring := NewHashRing()
	cs := &ClusterService{
		config:           config,
		onMessage:        onMessage,
		getLocalState:    getLocalState,
		mergeRemoteState: mergeRemoteState,
		ring:             ring,
	}

	if onMessage == nil {
		panic("onMessage must be defined for cluster service")
	}
	if getLocalState == nil {
		panic("getLocalState must be defined for cluster service")
	}
	if mergeRemoteState == nil {
		panic("mergeRemoteState must be defined for cluster service")
	}

	mlConfig := memberlist.DefaultLocalConfig()
	if config.FastTest {
		mlConfig = memberlist.DefaultLANConfig()
		mlConfig.PushPullInterval = 500 * time.Millisecond
		mlConfig.GossipInterval = 20 * time.Millisecond
		mlConfig.ProbeInterval = 100 * time.Millisecond
		mlConfig.SuspicionMult = 2
	}
	mlConfig.LogOutput = io.Discard
	mlConfig.Delegate = cs
	mlConfig.Events = cs

	if config.NodeID != "" {
		mlConfig.Name = string(config.NodeID)
	}
	if config.BindAddr != "" {
		mlConfig.BindAddr = config.BindAddr
	} else {
		mlConfig.BindAddr = "127.0.0.1"
	}
	mlConfig.BindPort = config.BindPort
	if config.AdvertiseAddr != "" {
		mlConfig.AdvertiseAddr = config.AdvertiseAddr
	}

	mlConfig.Events = cs
	ml, err := memberlist.Create(mlConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create memberlist: %w", err)
	}

	cs.memberList = ml
	cs.broadcasts = &memberlist.TransmitLimitedQueue{
		NumNodes:       func() int { return ml.NumMembers() },
		RetransmitMult: 3,
	}

	return cs, nil
}

// Broadcast serializes and spreads a message across the cluster using epidemic gossip.
func (cs *ClusterService) Broadcast(msg []byte) {
	cs.broadcasts.QueueBroadcast(&broadcast{
		msg: msg,
	})
}

// Members returns the gRPC API addresses of all active peers discovered via gossip.
func (cs *ClusterService) Members() []PeerAddress {
	members := cs.memberList.Members()
	addrs := make([]PeerAddress, 0, len(members))
	for _, m := range members {
		if len(m.Meta) > 0 {
			addrs = append(addrs, PeerAddress(fmt.Sprintf("%s:%s", m.Addr.String(), string(m.Meta))))
		}
	}
	return addrs
}

// AddressForNode returns the gRPC address for a given node ID.
func (cs *ClusterService) AddressForNode(nodeID NodeID) PeerAddress {
	for _, m := range cs.memberList.Members() {
		if m.Name == string(nodeID) {
			if len(m.Meta) > 0 {
				return PeerAddress(fmt.Sprintf("%s:%s", m.Addr.String(), string(m.Meta)))
			}
			break
		}
	}
	return ""
}

// Owner returns the NodeID of the peer responsible for the given key.
func (cs *ClusterService) Owner(key Key) NodeID {
	return cs.ring.GetNode(key)
}

// GetOwners returns the N closest NodeIDs on the hash ring responsible for replicating the given key.
func (cs *ClusterService) GetOwners(key Key, n int) []NodeID {
	return cs.ring.GetOwners(key, n)
}

// NotifyJoin is called by memberlist when a new node joins.
func (cs *ClusterService) NotifyJoin(node *memberlist.Node) {
	slog.Info("Node joined cluster", "node", node.Name, "addr", node.Addr.String())
	cs.ring.AddNode(NodeID(node.Name))
}

// NotifyLeave is called by memberlist when a node leaves.
func (cs *ClusterService) NotifyLeave(node *memberlist.Node) {
	slog.Info("Node left cluster", "node", node.Name)
	cs.ring.RemoveNode(NodeID(node.Name))
}

// NotifyUpdate is called by memberlist when a node's metadata changes.
func (cs *ClusterService) NotifyUpdate(_ *memberlist.Node) {
	// Ring distribution depends only on node name, so update is a no-op.
}

func (cs *ClusterService) start() error {
	if len(cs.config.SeedNodes) > 0 {
		count, err := cs.memberList.Join(cs.config.SeedNodes)
		if err != nil {
			return fmt.Errorf("failed to join cluster: %w", err)
		}
		slog.Info("Joined cluster successfully", "seeds", len(cs.config.SeedNodes), "joined", count)
	}
	return nil
}

func (cs *ClusterService) stop() error {
	if cs.memberList == nil {
		return nil
	}

	slog.Info("Leaving cluster...")
	if err := cs.memberList.Leave(time.Second); err != nil {
		slog.Warn("Error during cluster leave", "error", err)
	}

	err := cs.memberList.Shutdown()
	cs.memberList = nil
	return err
}

// memberlist.Delegate implementation

// NodeMeta returns the metadata of the node, which includes the gRPC port.
func (cs *ClusterService) NodeMeta(_ int) []byte {
	return fmt.Appendf(nil, "%d", cs.config.GrpcPort)
}

// NotifyMsg is called when a user-space message is received.
func (cs *ClusterService) NotifyMsg(b []byte) {
	cs.onMessage(b)
}

// GetBroadcasts is called when memberlist needs messages to broadcast.
func (cs *ClusterService) GetBroadcasts(overhead, limit int) [][]byte {
	return cs.broadcasts.GetBroadcasts(overhead, limit)
}

// LocalState returns the local node state for anti-entropy.
func (cs *ClusterService) LocalState(_ bool) []byte {
	if cs.getLocalState != nil {
		return cs.getLocalState()
	}
	return nil
}

// MergeRemoteState merges incoming state from a peer.
func (cs *ClusterService) MergeRemoteState(buf []byte, _ bool) {
	cs.mergeRemoteState(buf)
}

// NopCluster is a non-functional implementation of the Cluster interface used when distribution is disabled.
type NopCluster struct{}

// Broadcast does nothing in a NopCluster.
func (n *NopCluster) Broadcast([]byte) {}

// Members returns nil as there are no members in a NopCluster.
func (n *NopCluster) Members() []PeerAddress { return nil }

// Owner returns an empty NodeID as there are no owners in a NopCluster.
func (n *NopCluster) Owner(Key) NodeID { return "" }

// GetOwners returns nil as there are no owners in a NopCluster.
func (n *NopCluster) GetOwners(Key, int) []NodeID { return nil }

// AddressForNode returns an empty string as there are no nodes in a NopCluster.
func (n *NopCluster) AddressForNode(NodeID) PeerAddress { return "" }
func (n *NopCluster) start() error                      { return nil }
func (n *NopCluster) stop() error                       { return nil }

type broadcast struct {
	msg []byte
}

func (b *broadcast) Invalidates(_ memberlist.Broadcast) bool { return false }
func (b *broadcast) Message() []byte                         { return b.msg }
func (b *broadcast) Finished()                               {}

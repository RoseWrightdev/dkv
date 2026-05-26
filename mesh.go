package dkv

import (
	"fmt"
	"io"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/hashicorp/memberlist"
)

// PeerAddress represents the network address (IP:Port) of a dkv node.
type PeerAddress string

// Mesher defines the interface for distributed node discovery and replication.
type Mesher interface {
	Broadcast(msg []byte)
	Members() []PeerAddress
	Owner(key Key) NodeID
	GetOwners(key Key, n int) []NodeID
	PutOwners(owners []NodeID)
	AddressForNode(nodeID NodeID) PeerAddress
	start() error
	stop() error
}

// MeshConfig holds configuration for decentralized node discovery and membership.
type MeshConfig struct {
	NodeID            NodeID
	BindAddr          string
	AdvertiseAddr     string
	SeedNodes         []string
	ReplicationFactor int
	BindPort          int
	GrpcPort          int
	SingleNode        bool
	FastTest          bool
}

// Mesh provides the implementation for L7 Routing and P2P communication between nodes.
type Mesh struct {
	memberList *memberlist.Memberlist
	broadcasts *memberlist.TransmitLimitedQueue
	gossip     Gossiper
	exchanger  StateExchanger
	ring       *HashRing
	config     MeshConfig
	stopping   atomic.Bool
}

// newMesh initializes a new Mesh instance.
func newMesh(gossip Gossiper, exchanger StateExchanger, config MeshConfig) (*Mesh, error) {
	ring := NewHashRing()
	m := &Mesh{
		gossip:    gossip,
		exchanger: exchanger,
		config:    config,
		ring:      ring,
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
	mlConfig.Delegate = m
	mlConfig.Events = m

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

	mlConfig.Events = m
	ml, err := memberlist.Create(mlConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create memberlist: %w", err)
	}

	m.memberList = ml
	m.broadcasts = &memberlist.TransmitLimitedQueue{
		NumNodes:       func() int { return ml.NumMembers() },
		RetransmitMult: 3,
	}

	return m, nil
}

// Broadcast serializes and spreads a message across the cluster using epidemic gossip.
func (m *Mesh) Broadcast(msg []byte) {
	m.broadcasts.QueueBroadcast(&broadcast{
		msg: msg,
	})
}

// Members returns the gRPC API addresses of all active peers discovered via gossip.
func (m *Mesh) Members() []PeerAddress {
	if m.stopping.Load() || m.memberList == nil {
		return nil
	}
	members := m.memberList.Members()
	addrs := make([]PeerAddress, 0, len(members))
	for _, member := range members {
		if len(member.Meta) > 0 {
			addrs = append(addrs, PeerAddress(fmt.Sprintf("%s:%s", member.Addr.String(), string(member.Meta))))
		}
	}
	return addrs
}

// AddressForNode returns the gRPC address for a given node ID.
func (m *Mesh) AddressForNode(nodeID NodeID) PeerAddress {
	if m.stopping.Load() || m.memberList == nil {
		return ""
	}
	for _, member := range m.memberList.Members() {
		if member.Name == string(nodeID) {
			if len(member.Meta) > 0 {
				return PeerAddress(fmt.Sprintf("%s:%s", member.Addr.String(), string(member.Meta)))
			}
			break
		}
	}
	return ""
}

// Owner returns the NodeID of the peer responsible for the given key.
func (m *Mesh) Owner(key Key) NodeID {
	return m.ring.GetNode(key)
}

// GetOwners returns the N closest NodeIDs on the hash ring responsible for replicating the given key.
func (m *Mesh) GetOwners(key Key, n int) []NodeID {
	return m.ring.GetOwners(key, n)
}

// PutOwners returns a slice of NodeIDs back to the ring's slice pool for recycling.
func (m *Mesh) PutOwners(owners []NodeID) {
	m.ring.PutOwners(owners)
}

// NotifyJoin is called by memberlist when a new node joins.
func (m *Mesh) NotifyJoin(node *memberlist.Node) {
	slog.Info("Node joined cluster", "node", node.Name, "addr", node.Addr.String())
	m.ring.AddNode(NodeID(node.Name))
}

// NotifyLeave is called by memberlist when a node leaves.
func (m *Mesh) NotifyLeave(node *memberlist.Node) {
	slog.Info("Node left cluster", "node", node.Name)
	m.ring.RemoveNode(NodeID(node.Name))
}

// NotifyUpdate is called by memberlist when a node's metadata changes.
func (m *Mesh) NotifyUpdate(_ *memberlist.Node) {
	// Ring distribution depends only on node name, so update is a no-op.
}

func (m *Mesh) start() error {
	if len(m.config.SeedNodes) > 0 {
		count, err := m.memberList.Join(m.config.SeedNodes)
		if err != nil {
			return fmt.Errorf("failed to join cluster: %w", err)
		}
		slog.Info("Joined cluster successfully", "seeds", len(m.config.SeedNodes), "joined", count)
	}
	return nil
}

func (m *Mesh) stop() error {
	m.stopping.Store(true)
	if m.memberList == nil {
		return nil
	}

	slog.Info("Leaving cluster...")
	if err := m.memberList.Leave(time.Second); err != nil {
		slog.Warn("Error during cluster leave", "error", err)
	}

	err := m.memberList.Shutdown()
	return err
}

// memberlist.Delegate implementation

// NodeMeta returns the metadata of the node, which includes the gRPC port.
func (m *Mesh) NodeMeta(_ int) []byte {
	return fmt.Appendf(nil, "%d", m.config.GrpcPort)
}

// NotifyMsg is called when a user-space message is received.
func (m *Mesh) NotifyMsg(b []byte) {
	m.gossip.OnGossip(b)
}

// GetBroadcasts is called when memberlist needs messages to broadcast.
func (m *Mesh) GetBroadcasts(overhead, limit int) [][]byte {
	return m.broadcasts.GetBroadcasts(overhead, limit)
}

// LocalState returns the local node state for anti-entropy.
func (m *Mesh) LocalState(_ bool) []byte {
	if m.stopping.Load() {
		return nil
	}
	return m.exchanger.ExportState()
}

// MergeRemoteState merges incoming state from a peer.
func (m *Mesh) MergeRemoteState(buf []byte, _ bool) {
	m.exchanger.ImportState(buf)
}

// NopMesh is a non-functional implementation of the Mesh interface used when distribution is disabled.
type NopMesh struct{}

// Broadcast does nothing in a NopMesh.
func (n *NopMesh) Broadcast([]byte) {}

// Members returns nil as there are no members in a NopMesh.
func (n *NopMesh) Members() []PeerAddress { return nil }

// Owner returns an empty NodeID as there are no owners in a NopMesh.
func (n *NopMesh) Owner(Key) NodeID { return "" }

// GetOwners returns nil as there are no owners in a NopMesh.
func (n *NopMesh) GetOwners(Key, int) []NodeID { return nil }

// PutOwners does nothing in a NopMesh.
func (n *NopMesh) PutOwners([]NodeID) {}

// AddressForNode returns an empty string as there are no nodes in a NopMesh.
func (n *NopMesh) AddressForNode(NodeID) PeerAddress { return "" }
func (n *NopMesh) start() error                      { return nil }
func (n *NopMesh) stop() error                       { return nil }

type broadcast struct {
	msg []byte
}

func (b *broadcast) Invalidates(_ memberlist.Broadcast) bool { return false }
func (b *broadcast) Message() []byte                         { return b.msg }
func (b *broadcast) Finished()                               {}

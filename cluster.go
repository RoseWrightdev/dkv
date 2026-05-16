package dkv

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/hashicorp/memberlist"
)

// PeerAddress represents the network address (IP:Port) of a DKV node.
type PeerAddress string

// Cluster defines the interface for distributed node discovery and replication.
type Cluster interface {
	Broadcast(msg []byte)
	Members() []PeerAddress
	start() error
	stop() error
}

// ClusterConfig holds configuration for decentralized node discovery and membership.
type ClusterConfig struct {
	// SingleNode explicitly disables the distribution layer when set to true.
	// DKV is distributed by default.
	SingleNode bool
	// NodeName is a unique identifier for this node in the cluster.
	NodeName string
	// BindAddr is the address memberlist will bind to for gossip (UDP/TCP).
	BindAddr string
	// BindPort is the port memberlist will use.
	BindPort int
	// AdvertiseAddr is the address other nodes should use to reach this node.
	AdvertiseAddr string
	// SeedNodes is a list of existing nodes to join upon startup.
	SeedNodes []string
	// GrpcPort is the port of the DKV gRPC API, shared with peers via metadata.
	GrpcPort int
}

// ClusterService manages the lifecycle of the node within a gossip-based cluster.
type ClusterService struct {
	config           ClusterConfig
	memberList       *memberlist.Memberlist
	broadcasts       *memberlist.TransmitLimitedQueue
	onMessage        func([]byte)
	getLocalState    func() []byte
	mergeRemoteState func([]byte)
}

// newClusterService initializes a new ClusterService instance.
func newClusterService(
	config ClusterConfig,
	onMessage func([]byte),
	getLocalState func() []byte,
	mergeRemoteState func([]byte),
) (*ClusterService, error) {
	cs := &ClusterService{
		config:           config,
		onMessage:        onMessage,
		getLocalState:    getLocalState,
		mergeRemoteState: mergeRemoteState,
	}

	mlConfig := memberlist.DefaultLocalConfig()
	mlConfig.Delegate = cs
	mlConfig.Events = cs

	if config.NodeName != "" {
		mlConfig.Name = config.NodeName
	}
	if config.BindAddr != "" {
		mlConfig.BindAddr = config.BindAddr
	}
	if config.BindPort != 0 {
		mlConfig.BindPort = config.BindPort
	}
	if config.AdvertiseAddr != "" {
		mlConfig.AdvertiseAddr = config.AdvertiseAddr
	}

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
	if cs.broadcasts == nil {
		return
	}
	cs.broadcasts.QueueBroadcast(&broadcast{
		msg: msg,
	})
}

// Members returns the gRPC API addresses of all active peers discovered via gossip.
func (cs *ClusterService) Members() []PeerAddress {
	if cs.memberList == nil {
		return nil
	}

	members := cs.memberList.Members()
	addrs := make([]PeerAddress, 0, len(members))
	for _, m := range members {
		if len(m.Meta) > 0 {
			addrs = append(addrs, PeerAddress(fmt.Sprintf("%s:%s", m.Addr.String(), string(m.Meta))))
		}
	}
	return addrs
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

func (cs *ClusterService) NodeMeta(limit int) []byte {
	return []byte(fmt.Sprintf("%d", cs.config.GrpcPort))
}

func (cs *ClusterService) NotifyMsg(b []byte) {
	if cs.onMessage != nil {
		cs.onMessage(b)
	}
}

func (cs *ClusterService) GetBroadcasts(overhead, limit int) [][]byte {
	return cs.broadcasts.GetBroadcasts(overhead, limit)
}

func (cs *ClusterService) LocalState(join bool) []byte {
	if cs.getLocalState != nil {
		return cs.getLocalState()
	}
	return nil
}

func (cs *ClusterService) MergeRemoteState(buf []byte, join bool) {
	if cs.mergeRemoteState != nil {
		cs.mergeRemoteState(buf)
	}
}

// memberlist.EventDelegate implementation

func (cs *ClusterService) NotifyJoin(node *memberlist.Node) {
	slog.Debug("Cluster member joined", "name", node.Name, "addr", node.Addr.String())
}

func (cs *ClusterService) NotifyLeave(node *memberlist.Node) {
	slog.Debug("Cluster member left", "name", node.Name, "addr", node.Addr.String())
}

func (cs *ClusterService) NotifyUpdate(node *memberlist.Node) {
	slog.Debug("Cluster member updated", "name", node.Name)
}

// NopCluster is a non-functional implementation of the Cluster interface used when distribution is disabled.
type NopCluster struct{}

func (n *NopCluster) Broadcast([]byte)       {}
func (n *NopCluster) Members() []PeerAddress { return nil }
func (n *NopCluster) start() error           { return nil }
func (n *NopCluster) stop() error            { return nil }

type broadcast struct {
	msg []byte
}

func (b *broadcast) Invalidates(other memberlist.Broadcast) bool { return false }
func (b *broadcast) Message() []byte                             { return b.msg }
func (b *broadcast) Finished()                                   {}

package dkv

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/hashicorp/memberlist"
)

// ClusterConfig holds configuration for the Gossip cluster.
type ClusterConfig struct {
	NodeName      string
	BindAddr      string
	BindPort      int
	AdvertiseAddr string
	SeedNodes     []string
	GrpcPort      int
}

// ClusterService manages node discovery and membership using memberlist.
type ClusterService struct {
	config     ClusterConfig
	memberList *memberlist.Memberlist
	broadcasts *memberlist.TransmitLimitedQueue
	onMessage  func([]byte)
}

func newClusterService(config ClusterConfig, onMessage func([]byte)) (*ClusterService, error) {
	cs := &ClusterService{
		config:    config,
		onMessage: onMessage,
	}

	mlConfig := memberlist.DefaultLocalConfig()
	mlConfig.Delegate = cs

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
		return nil, err
	}

	cs.memberList = ml
	cs.broadcasts = &memberlist.TransmitLimitedQueue{
		NumNodes:       func() int { return ml.NumMembers() },
		RetransmitMult: 3,
	}

	return cs, nil
}

// memberlist.Delegate implementation
func (cs *ClusterService) NodeMeta(limit int) []byte {
	return []byte(fmt.Sprintf("%d", cs.config.GrpcPort))
}

func (cs *ClusterService) NotifyMsg(b []byte) { cs.onMessage(b) }
func (cs *ClusterService) GetBroadcasts(overhead, limit int) [][]byte {
	return cs.broadcasts.GetBroadcasts(overhead, limit)
}
func (cs *ClusterService) LocalState(join bool) []byte            { return nil }
func (cs *ClusterService) MergeRemoteState(buf []byte, join bool) {}

// Broadcast sends a message to the cluster.
func (cs *ClusterService) Broadcast(msg []byte) {
	cs.broadcasts.QueueBroadcast(&broadcast{msg: msg})
}

type broadcast struct {
	msg []byte
}

func (b *broadcast) Invalidates(other memberlist.Broadcast) bool { return false }
func (b *broadcast) Message() []byte                             { return b.msg }
func (b *broadcast) Finished()                                   {}

func (cs *ClusterService) start() error {
	if len(cs.config.SeedNodes) > 0 {
		count, err := cs.memberList.Join(cs.config.SeedNodes)
		if err != nil {
			return err
		}
		slog.Info("Joined cluster", "seed_count", len(cs.config.SeedNodes), "joined_count", count)
	}
	return nil
}

func (cs *ClusterService) stop() error {
	if cs.memberList == nil {
		return nil
	}
	if err := cs.memberList.Leave(time.Second); err != nil {
		slog.Debug("Error leaving cluster", "error", err)
	}
	err := cs.memberList.Shutdown()
	cs.memberList = nil // Mark as stopped
	return err
}

// Members returns a list of current active members in the cluster.
func (cs *ClusterService) Members() []string {
	members := cs.memberList.Members()
	addrs := make([]string, 0, len(members))
	for _, m := range members {
		if len(m.Meta) > 0 {
			addrs = append(addrs, fmt.Sprintf("%s:%s", m.Addr.String(), string(m.Meta)))
		}
	}
	return addrs
}

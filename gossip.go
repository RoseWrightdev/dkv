package dkv

import (
	"log/slog"

	pb "github.com/rosewrightdev/dkv/api"
	"google.golang.org/protobuf/proto"
)

// Gossiper defines the interface for handling incoming gossip messages.
type Gossiper interface {
	OnGossip(msg []byte)
}

// Gossip manages the replication of messages received via gossip protocols.
type Gossip struct {
	pools  *pools
	writer StateWriter
}

// newGossip creates a new Gossip instance that handles incoming UDP replication messages.
func newGossip(pools *pools, writer StateWriter) *Gossip {
	return &Gossip{
		pools:  pools,
		writer: writer,
	}
}

// OnGossip processes raw incoming UDP gossip packets, unmarshals them into WAL entries, and applies the updates via the StateWriter.
func (g *Gossip) OnGossip(data []byte) {
	entry := g.pools.walEntries.Get().(*pb.WalEntry)
	defer g.pools.walEntries.Put(entry)
	entry.Reset()

	if err := proto.Unmarshal(data, entry); err != nil {
		slog.Error("Failed to unmarshal gossip message", "error", err)
		return
	}

	switch kv := entry.Entry.(type) {
	case *pb.WalEntry_Set:
		if err := g.writer.ApplySet(kv.Set); err != nil {
			slog.Error("Critical: Failed to apply gossip set", "error", err)
		}
	case *pb.WalEntry_Delete:
		if err := g.writer.ApplyDelete(kv.Delete); err != nil {
			slog.Error("Critical: Failed to apply gossip delete", "error", err)
		}
	}
	entry.Entry = nil
}

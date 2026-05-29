package gossip

import (
	"log/slog"
	"sync"

	pb "github.com/rosewrightdev/dkv/api"
	"google.golang.org/protobuf/proto"
)

var walEntriesPool sync.Pool = sync.Pool{
	New: func() any { return &pb.WalEntry{} },
}

// StateWriter defines the interface for applying sets and deletes to the state.
type StateWriter interface {
	ApplySet(req *pb.SetRequest) error
	ApplyDelete(req *pb.DeleteRequest) error
}

// Gossiper defines the interface for handling incoming gossip messages.
type Gossiper interface {
	OnGossip(msg []byte)
}

// Gossip manages the replication of messages received via gossip protocols.
type Gossip struct {
	writer StateWriter
}

// NewGossip creates a new Gossip instance that handles incoming UDP replication messages.
func NewGossip(writer StateWriter) *Gossip {
	return &Gossip{
		writer: writer,
	}
}

// OnGossip processes raw incoming UDP gossip packets, unmarshals them into WAL entries, and applies the updates via the StateWriter.
func (g *Gossip) OnGossip(data []byte) {
	entry := walEntriesPool.Get().(*pb.WalEntry)
	defer walEntriesPool.Put(entry)
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

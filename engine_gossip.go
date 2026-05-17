package dkv

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log/slog"

	pb "github.com/rosewrightdev/dkv/api"
	"google.golang.org/protobuf/proto"
)

func (eng *engine) onGossipMessage(data []byte) {
	entry := eng.pools.walEntries.Get().(*pb.WalEntry)
	defer eng.pools.walEntries.Put(entry)
	entry.Reset()

	if err := proto.Unmarshal(data, entry); err != nil {
		slog.Error("Failed to unmarshal gossip message", "error", err)
		return
	}

	switch kv := entry.Entry.(type) {
	case *pb.WalEntry_Set:
		if err := eng.applyGossipSet(kv.Set); err != nil {
			slog.Error("Critical: Failed to apply gossip set", "error", err)
		}
	case *pb.WalEntry_Delete:
		if err := eng.applyGossipDelete(kv.Delete); err != nil {
			slog.Error("Critical: Failed to apply gossip delete", "error", err)
		}
	}
	entry.Entry = nil
}

func (eng *engine) applyGossipSet(req *pb.SetRequest) error {
	eng.clock.Update(req.Timestamp)
	hash := hashFunc(req.Key)
	if !eng.isLocal(req.Key) {
		return nil // Ignore keys we are not responsible for
	}

	// LWW Conflict Resolution
	existing, ok := eng.hm.Load(req.Key, hash)
	if ok {
		if existing.Timestamp > req.Timestamp {
			return nil
		}
		if existing.Timestamp == req.Timestamp && existing.NodeID >= req.NodeId {
			return nil // Tie-break: existing node wins if NodeID is >= incoming
		}
	}

	// Apply update
	eng.hm.Store(req.Key, hash, Value{
		Data:      req.Value,
		Timestamp: req.Timestamp,
		NodeID:    req.NodeId,
		Tombstone: false,
	})

	if err := eng.wal.publish(req.Key, hash, req); err != nil {
		return fmt.Errorf("failed to persist gossip set to WAL: %w", err)
	}
	return nil
}

func (eng *engine) applyGossipDelete(req *pb.DeleteRequest) error {
	eng.clock.Update(req.Timestamp)
	hash := hashFunc(req.Key)
	if !eng.isLocal(req.Key) {
		return nil // Ignore keys we are not responsible for
	}

	existing, ok := eng.hm.Load(req.Key, hash)
	if ok {
		if existing.Timestamp > req.Timestamp {
			return nil
		}
		if existing.Timestamp == req.Timestamp && existing.NodeID >= req.NodeId {
			return nil
		}
	}

	eng.hm.Store(req.Key, hash, Value{
		Timestamp: req.Timestamp,
		NodeID:    req.NodeId,
		Tombstone: true,
	})
	if err := eng.wal.publish(req.Key, hash, req); err != nil {
		return fmt.Errorf("failed to persist gossip delete to WAL: %w", err)
	}
	return nil
}

func (eng *engine) getLocalState() []byte {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := eng.streamToEncoder(enc); err != nil {
		slog.Error("Failed to encode local state for cluster sync", "error", err)
		return nil
	}
	return buf.Bytes()
}

func (eng *engine) mergeRemoteState(buf []byte) {
	if len(buf) == 0 {
		return
	}
	reader := bytes.NewReader(buf)
	if err := eng.decodeFromReader(reader); err != nil {
		slog.Error("Failed to decode remote state from cluster sync", "error", err)
	}
}

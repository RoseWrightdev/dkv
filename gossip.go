package dkv

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"log/slog"
	"slices"

	pb "github.com/rosewrightdev/dkv/api"
	"google.golang.org/protobuf/proto"
)

type Gossiper interface {
	OnGossip(msg []byte)
}

type StateExchanger interface {
	ExportState() []byte
	ImportState(state []byte)
}

type StateWriter interface {
	ApplySet(req *pb.SetRequest) error
	ApplyDelete(req *pb.DeleteRequest) error
}

type Gossip struct {
	pools      *pools
	hm         *shardedMap
	wal        Waler
	clock      Clock
	mesh       Mesher
	meshConfig *MeshConfig
}

func newGossip(pools *pools, hm *shardedMap, wal Waler, clock Clock, mesh Mesher, meshConfig *MeshConfig) *Gossip {
	return &Gossip{pools, hm, wal, clock, mesh, meshConfig}
}

func (sip *Gossip) OnGossip(data []byte) {
	entry := sip.pools.walEntries.Get().(*pb.WalEntry)
	defer sip.pools.walEntries.Put(entry)
	entry.Reset()

	if err := proto.Unmarshal(data, entry); err != nil {
		slog.Error("Failed to unmarshal gossip message", "error", err)
		return
	}

	switch kv := entry.Entry.(type) {
	case *pb.WalEntry_Set:
		if err := sip.ApplySet(kv.Set); err != nil {
			slog.Error("Critical: Failed to apply gossip set", "error", err)
		}
	case *pb.WalEntry_Delete:
		if err := sip.ApplyDelete(kv.Delete); err != nil {
			slog.Error("Critical: Failed to apply gossip delete", "error", err)
		}
	}
	entry.Entry = nil
}

func (sip *Gossip) ApplySet(req *pb.SetRequest) error {
	sip.clock.Update(req.Timestamp)
	hash := hashFunc(req.Key)
	if !sip.isLocal(req.Key) {
		return nil // Ignore keys we are not responsible for
	}

	// LWW Conflict Resolution
	existing, ok := sip.hm.Load(req.Key, hash)
	if ok {
		if existing.Timestamp > req.Timestamp {
			return nil
		}
		if existing.Timestamp == req.Timestamp && existing.NodeID >= req.NodeId {
			return nil // Tie-break: existing node wins if NodeID is >= incoming
		}
	}

	// Apply update
	sip.hm.Store(req.Key, hash, Value{
		Data:      req.Value,
		Timestamp: req.Timestamp,
		NodeID:    req.NodeId,
		Tombstone: false,
	})

	if err := sip.wal.publish(req.Key, hash, req); err != nil {
		return fmt.Errorf("failed to persist gossip set to WAL: %w", err)
	}
	return nil
}

func (sip *Gossip) ApplyDelete(req *pb.DeleteRequest) error {
	sip.clock.Update(req.Timestamp)
	hash := hashFunc(req.Key)
	if !sip.isLocal(req.Key) {
		return nil // Ignore keys we are not responsible for
	}

	existing, ok := sip.hm.Load(req.Key, hash)
	if ok {
		if existing.Timestamp > req.Timestamp {
			return nil
		}
		if existing.Timestamp == req.Timestamp && existing.NodeID >= req.NodeId {
			return nil
		}
	}

	sip.hm.Store(req.Key, hash, Value{
		Timestamp: req.Timestamp,
		NodeID:    req.NodeId,
		Tombstone: true,
	})
	if err := sip.wal.publish(req.Key, hash, req); err != nil {
		return fmt.Errorf("failed to persist gossip delete to WAL: %w", err)
	}
	return nil
}

func (sip *Gossip) ExportState() []byte {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := sip.streamToEncoder(enc); err != nil {
		slog.Error("Failed to encode local state for cluster sync", "error", err)
		return nil
	}
	return buf.Bytes()
}

func (sip *Gossip) isLocal(key string) bool {
	if sip.meshConfig.SingleNode {
		return true
	}
	rf := sip.meshConfig.ReplicationFactor
	if rf <= 0 {
		rf = 1
	}

	// In a distributed cluster, we are responsible if we are one of the N owners
	owners := sip.mesh.GetOwners(key, rf)
	defer sip.mesh.PutOwners(owners)
	return slices.Contains(owners, sip.meshConfig.NodeID)
}

func (sip *Gossip) ImportState(buf []byte) {
	if len(buf) == 0 {
		return
	}
	reader := bytes.NewReader(buf)
	if err := sip.decodeFromReader(reader); err != nil {
		slog.Error("Failed to decode remote state from cluster sync", "error", err)
	}
}

func (sip *Gossip) streamToEncoder(enc *gob.Encoder) error {
	for i := range shardCount {
		shard := sip.hm[i]
		shard.mu.RLock()
		for b := range subBucketCount {
			for k, v := range shard.buckets[b] {
				entry := sip.pools.snapshotEntries.Get().(*snapshotEntry)
				entry.Key = k
				entry.Data = v.Data
				entry.Timestamp = v.Timestamp
				entry.Tombstone = v.Tombstone

				if err := enc.Encode(entry); err != nil {
					shard.mu.RUnlock()
					entry.Key = ""
					entry.Data = nil
					sip.pools.snapshotEntries.Put(entry)
					return err
				}
				entry.Key = ""
				entry.Data = nil
				sip.pools.snapshotEntries.Put(entry)
			}
		}
		shard.mu.RUnlock()
	}
	return nil
}

func (sip *Gossip) decodeFromReader(r io.Reader) error {
	dec := gob.NewDecoder(r)
	count := 0
	for {
		entry := sip.pools.snapshotEntries.Get().(*snapshotEntry)
		if err := dec.Decode(entry); err != nil {
			entry.Key = ""
			entry.Data = nil
			sip.pools.snapshotEntries.Put(entry)
			if err == io.EOF {
				break
			}
			return fmt.Errorf("failed to decode snapshot entry: %w", err)
		}

		if entry.Tombstone {
			// todo: refactor
			req := sip.pools.deleteRequests.Get().(*pb.DeleteRequest)
			req.Key = entry.Key
			req.Timestamp = entry.Timestamp
			err := sip.ApplyDelete(req)
			req.Reset()
			sip.pools.deleteRequests.Put(req)
			if err != nil {
				entry.Key = ""
				entry.Data = nil
				sip.pools.snapshotEntries.Put(entry)
				return err
			}
		} else {
			// todo: refactor
			req := sip.pools.setRequests.Get().(*pb.SetRequest)
			req.Key = entry.Key
			req.Value = entry.Data
			req.Timestamp = entry.Timestamp
			err := sip.ApplySet(req)
			req.Reset()
			sip.pools.setRequests.Put(req)
			if err != nil {
				entry.Key = ""
				entry.Data = nil
				sip.pools.snapshotEntries.Put(entry)
				return err
			}
		}

		entry.Key = ""
		entry.Data = nil
		sip.pools.snapshotEntries.Put(entry)
		count++
	}

	if count > 0 {
		slog.Info("Merged remote state from cluster member", "entries", count)
	}
	return nil
}

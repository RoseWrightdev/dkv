package writer

import (
	"fmt"
	"slices"

	pb "github.com/rosewrightdev/dkv/api"
	"github.com/rosewrightdev/dkv/clock"
	"github.com/rosewrightdev/dkv/hashmap"
	"github.com/rosewrightdev/dkv/kv"
	"github.com/rosewrightdev/dkv/mesh"
	"github.com/rosewrightdev/dkv/security"
	"github.com/rosewrightdev/dkv/wal"
)

// StateWriter defines the interface for applying sets and deletes to the state.
type StateWriter interface {
	ApplySet(req *pb.SetRequest) error
	ApplyDelete(req *pb.DeleteRequest) error
}

// StorageWriter handles applying mutations to the storage engine.
type StorageWriter struct {
	hm         *hashmap.ShardedMap
	wal        wal.Waler
	clock      clock.Clocker
	mesh       mesh.Mesher
	meshConfig *mesh.MeshConfig
}

// NewStorageWriter creates a StorageWriter instance to process and persist key-value mutations.
func NewStorageWriter(hm *hashmap.ShardedMap, wal wal.Waler, clock clock.Clocker, mesh mesh.Mesher, meshConfig *mesh.MeshConfig) *StorageWriter {
	return &StorageWriter{
		hm:         hm,
		wal:        wal,
		clock:      clock,
		mesh:       mesh,
		meshConfig: meshConfig,
	}
}

// SetMesh dynamically assigns a new Mesher interface implementation.
func (sw *StorageWriter) SetMesh(m mesh.Mesher) {
	sw.mesh = m
}

// ApplySet updates the in-memory store and publishes the update to the WAL after performing LWW conflict resolution and cluster ownership checks.
func (sw *StorageWriter) ApplySet(req *pb.SetRequest) error {
	sw.clock.Update(req.Timestamp)
	hash := security.HashFunc(req.Key)
	if !sw.isLocal(req.Key) {
		return nil // Ignore keys we are not responsible for
	}

	val := kv.Value{
		Data:      req.Value,
		Timestamp: req.Timestamp,
		NodeID:    req.NodeId,
		Tombstone: false,
	}

	if !sw.hm.StoreLWW(req.Key, hash, val) {
		return nil // Stale update ignored under LWW rules
	}

	if err := sw.wal.Publish(req.Key, hash, req); err != nil {
		return fmt.Errorf("failed to persist gossip set to WAL: %w", err)
	}
	return nil
}

// ApplyDelete marks a key as deleted (using a tombstone) in-memory and in the WAL after performing LWW conflict resolution and cluster ownership checks.
func (sw *StorageWriter) ApplyDelete(req *pb.DeleteRequest) error {
	sw.clock.Update(req.Timestamp)
	hash := security.HashFunc(req.Key)
	if !sw.isLocal(req.Key) {
		return nil // Ignore keys we are not responsible for
	}

	val := kv.Value{
		Timestamp: req.Timestamp,
		NodeID:    req.NodeId,
		Tombstone: true,
	}

	if !sw.hm.StoreLWW(req.Key, hash, val) {
		return nil // Stale delete ignored under LWW rules
	}

	if err := sw.wal.Publish(req.Key, hash, req); err != nil {
		return fmt.Errorf("failed to persist gossip delete to WAL: %w", err)
	}
	return nil
}

// isLocal checks if the current node is responsible for the given key based on cluster ring hash routing.
func (sw *StorageWriter) isLocal(key string) bool {
	if sw.meshConfig.SingleNode {
		return true
	}
	rf := sw.meshConfig.ReplicationFactor
	if rf <= 0 {
		rf = 1
	}

	// In a distributed cluster, we are responsible if we are one of the N owners
	owners := sw.mesh.GetOwners(kv.Key(key), rf)
	defer sw.mesh.PutOwners(owners)
	return slices.Contains(owners, sw.meshConfig.NodeID)
}

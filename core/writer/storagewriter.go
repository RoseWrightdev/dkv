// Package writer implements conflict-free storage mutations and writes to the local database.
package writer

import (
	"fmt"

	pb "github.com/rosewrightdev/oryx/api"
	"github.com/rosewrightdev/oryx/core/clock"
	"github.com/rosewrightdev/oryx/core/hashmap"
	"github.com/rosewrightdev/oryx/core/wal"
	"github.com/rosewrightdev/oryx/kv"
	"github.com/rosewrightdev/oryx/security"
)

// cloneBytes returns a private copy of b, or nil when b is nil so that an unset
// value stays distinguishable from an explicitly empty one.
func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}

// StateWriter defines the interface for applying sets and deletes to the state.
type StateWriter interface {
	ApplySet(req *pb.SetRequest) error
	ApplyDelete(req *pb.DeleteRequest) error
}

// StorageWriter handles applying mutations to the storage engine.
type StorageWriter struct {
	hm    *hashmap.ShardedMap
	wal   wal.Waler
	clock clock.Clocker
}

// NewStorageWriter creates a StorageWriter instance to process and persist key-value mutations.
func NewStorageWriter(hm *hashmap.ShardedMap, wal wal.Waler, clock clock.Clocker) *StorageWriter {
	return &StorageWriter{
		hm:    hm,
		wal:   wal,
		clock: clock,
	}
}

// ApplySet updates the in-memory store and publishes the update to the WAL after performing LWW conflict resolution.
//
// req.Value is copied before it is stored. Callers routinely hand us a slice
// they do not own for the lifetime of the record: engine.Set and the cluster
// gateway both lease req from a sync.Pool and Reset it as soon as this returns,
// and the RESP path points req.Value straight at a connection read buffer.
// Retaining any of those would let a later, unrelated write scribble over a
// live database value.
func (sw *StorageWriter) ApplySet(req *pb.SetRequest) error {
	sw.clock.Update(req.Timestamp)
	hash := security.HashFunc(req.Key)

	val := kv.Value{
		Data:      cloneBytes(req.Value),
		Timestamp: req.Timestamp,
		NodeID:    req.NodeId,
		Tombstone: false,
	}

	if !sw.hm.StoreLWW(req.Key, hash, val) {
		return nil // Stale update ignored under LWW rules
	}

	if err := sw.wal.Publish(req.Key, hash, req); err != nil {
		return fmt.Errorf("failed to persist set to WAL: %w", err)
	}
	return nil
}

// ApplyDelete marks a key as deleted (using a tombstone) in-memory and in the WAL after performing LWW conflict resolution.
func (sw *StorageWriter) ApplyDelete(req *pb.DeleteRequest) error {
	sw.clock.Update(req.Timestamp)
	hash := security.HashFunc(req.Key)

	val := kv.Value{
		Timestamp: req.Timestamp,
		NodeID:    req.NodeId,
		Tombstone: true,
	}

	if !sw.hm.StoreLWW(req.Key, hash, val) {
		return nil // Stale delete ignored under LWW rules
	}

	if err := sw.wal.Publish(req.Key, hash, req); err != nil {
		return fmt.Errorf("failed to persist delete to WAL: %w", err)
	}
	return nil
}

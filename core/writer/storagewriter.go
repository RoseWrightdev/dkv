// Package writer implements conflict-free storage mutations and writes to the local database.
package writer

import (
	"fmt"

	pb "github.com/rosewrightdev/dkv/api"
	"github.com/rosewrightdev/dkv/core/clock"
	"github.com/rosewrightdev/dkv/core/hashmap"
	"github.com/rosewrightdev/dkv/core/wal"
	"github.com/rosewrightdev/dkv/kv"
	"github.com/rosewrightdev/dkv/security"
)

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
func (sw *StorageWriter) ApplySet(req *pb.SetRequest) error {
	sw.clock.Update(req.Timestamp)
	hash := security.HashFunc(req.Key)

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

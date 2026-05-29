package trans

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"log/slog"
	"sync"

	pb "github.com/rosewrightdev/dkv/api"
	"github.com/rosewrightdev/dkv/internal/hashmap"
	"github.com/rosewrightdev/dkv/kv"
	"github.com/rosewrightdev/dkv/internal/snap"
)

var (
	snapshotEntries = sync.Pool{
		New: func() any { return &snap.SnapshotEntry{} },
	}
	setRequests = sync.Pool{
		New: func() any { return &pb.SetRequest{} },
	}
	deleteRequests = sync.Pool{
		New: func() any { return &pb.DeleteRequest{} },
	}
)

// StateWriter defines the interface for applying sets and deletes to the state.
type StateWriter interface {
	ApplySet(req *pb.SetRequest) error
	ApplyDelete(req *pb.DeleteRequest) error
}

// StateExchanger defines the interface for exporting and importing cluster state.
type StateExchanger interface {
	ExportState() []byte
	ImportState(state []byte)
}

// StateTransfer coordinates the exchange of local and remote database state.
type StateTransfer struct {
	hm     *hashmap.ShardedMap
	writer StateWriter
}

// NewStateTransfer creates a StateTransfer instance to handle state import/export across cluster nodes.
func NewStateTransfer(hm *hashmap.ShardedMap, writer StateWriter) *StateTransfer {
	return &StateTransfer{
		hm:     hm,
		writer: writer,
	}
}

// ExportState serializes the entire shardedMap into a byte slice to support cluster-wide state synchronization.
func (st *StateTransfer) ExportState() []byte {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := st.StreamToEncoder(enc); err != nil {
		slog.Error("Failed to encode local state for cluster sync", "error", err)
		return nil
	}
	return buf.Bytes()
}

// ImportState accepts a raw byte slice, decodes it, and applies all received keys and values to the storage engine.
func (st *StateTransfer) ImportState(buf []byte) {
	if len(buf) == 0 {
		return
	}
	reader := bytes.NewReader(buf)
	if err := st.DecodeFromReader(reader); err != nil {
		slog.Error("Failed to decode remote state from cluster sync", "error", err)
	}
}

// StreamToEncoder serializes all key-value entries across all shards into a gob Encoder.
func (st *StateTransfer) StreamToEncoder(enc *gob.Encoder) error {
	var err error
	st.hm.Range(func(k kv.Key, v kv.Value) bool {
		entry := snapshotEntries.Get().(*snap.SnapshotEntry)
		entry.Key = k
		entry.Data = v.Data
		entry.Timestamp = v.Timestamp
		entry.Tombstone = v.Tombstone

		if encErr := enc.Encode(entry); encErr != nil {
			err = encErr
			entry.Key = ""
			entry.Data = nil
			entry.Timestamp = 0
			entry.Tombstone = false
			snapshotEntries.Put(entry)
			return false // stop iteration
		}
		entry.Key = ""
		entry.Data = nil
		entry.Timestamp = 0
		entry.Tombstone = false
		snapshotEntries.Put(entry)
		return true // continue iteration
	})
	return err
}

// DecodeFromReader reads snapshot entries from a Reader and writes them to the underlying storage engine.
func (st *StateTransfer) DecodeFromReader(r io.Reader) error {
	dec := gob.NewDecoder(r)
	count := 0
	for {
		entry := snapshotEntries.Get().(*snap.SnapshotEntry)
		if err := dec.Decode(entry); err != nil {
			entry.Key = ""
			entry.Data = nil
			entry.Timestamp = 0
			entry.Tombstone = false
			snapshotEntries.Put(entry)
			if err == io.EOF {
				break
			}
			return fmt.Errorf("failed to decode snapshot entry: %w", err)
		}

		if entry.Tombstone {
			req := deleteRequests.Get().(*pb.DeleteRequest)
			req.Key = entry.Key
			req.Timestamp = entry.Timestamp
			err := st.writer.ApplyDelete(req)
			req.Reset()
			deleteRequests.Put(req)
			if err != nil {
				entry.Key = ""
				entry.Data = nil
				entry.Timestamp = 0
				entry.Tombstone = false
				snapshotEntries.Put(entry)
				return err
			}
		} else {
			req := setRequests.Get().(*pb.SetRequest)
			req.Key = entry.Key
			req.Value = entry.Data
			req.Timestamp = entry.Timestamp
			err := st.writer.ApplySet(req)
			req.Reset()
			setRequests.Put(req)
			if err != nil {
				entry.Key = ""
				entry.Data = nil
				entry.Timestamp = 0
				entry.Tombstone = false
				snapshotEntries.Put(entry)
				return err
			}
		}

		entry.Key = ""
		entry.Data = nil
		entry.Timestamp = 0
		entry.Tombstone = false
		snapshotEntries.Put(entry)
		count++
	}

	if count > 0 {
		slog.Info("Merged remote state from cluster member", "entries", count)
	}
	return nil
}

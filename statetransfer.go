package dkv

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"log/slog"

	pb "github.com/rosewrightdev/dkv/api"
)

type StateExchanger interface {
	ExportState() []byte
	ImportState(state []byte)
}

type StateTransfer struct {
	pools  *pools
	hm     *shardedMap
	writer StateWriter
}

// newStateTransfer creates a StateTransfer instance to handle state import/export across cluster nodes.
func newStateTransfer(pools *pools, hm *shardedMap, writer StateWriter) *StateTransfer {
	return &StateTransfer{
		pools:  pools,
		hm:     hm,
		writer: writer,
	}
}

// ExportState serializes the entire shardedMap into a byte slice to support cluster-wide state synchronization.
func (st *StateTransfer) ExportState() []byte {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := st.streamToEncoder(enc); err != nil {
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
	if err := st.decodeFromReader(reader); err != nil {
		slog.Error("Failed to decode remote state from cluster sync", "error", err)
	}
}

// streamToEncoder serializes all key-value entries across all shards into a gob Encoder.
func (st *StateTransfer) streamToEncoder(enc *gob.Encoder) error {
	for i := range shardCount {
		shard := st.hm[i]
		shard.mu.RLock()
		for b := range subBucketCount {
			for k, v := range shard.buckets[b] {
				entry := st.pools.snapshotEntries.Get().(*snapshotEntry)
				entry.Key = k
				entry.Data = v.Data
				entry.Timestamp = v.Timestamp
				entry.Tombstone = v.Tombstone

				if err := enc.Encode(entry); err != nil {
					shard.mu.RUnlock()
					entry.Key = ""
					entry.Data = nil
					st.pools.snapshotEntries.Put(entry)
					return err
				}
				entry.Key = ""
				entry.Data = nil
				st.pools.snapshotEntries.Put(entry)
			}
		}
		shard.mu.RUnlock()
	}
	return nil
}

// decodeFromReader reads snapshot entries from a Reader and writes them to the underlying storage engine.
func (st *StateTransfer) decodeFromReader(r io.Reader) error {
	dec := gob.NewDecoder(r)
	count := 0
	for {
		entry := st.pools.snapshotEntries.Get().(*snapshotEntry)
		if err := dec.Decode(entry); err != nil {
			entry.Key = ""
			entry.Data = nil
			st.pools.snapshotEntries.Put(entry)
			if err == io.EOF {
				break
			}
			return fmt.Errorf("failed to decode snapshot entry: %w", err)
		}

		if entry.Tombstone {
			// todo: refactor
			req := st.pools.deleteRequests.Get().(*pb.DeleteRequest)
			req.Key = entry.Key
			req.Timestamp = entry.Timestamp
			err := st.writer.ApplyDelete(req)
			req.Reset()
			st.pools.deleteRequests.Put(req)
			if err != nil {
				entry.Key = ""
				entry.Data = nil
				st.pools.snapshotEntries.Put(entry)
				return err
			}
		} else {
			// todo: refactor
			req := st.pools.setRequests.Get().(*pb.SetRequest)
			req.Key = entry.Key
			req.Value = entry.Data
			req.Timestamp = entry.Timestamp
			err := st.writer.ApplySet(req)
			req.Reset()
			st.pools.setRequests.Put(req)
			if err != nil {
				entry.Key = ""
				entry.Data = nil
				st.pools.snapshotEntries.Put(entry)
				return err
			}
		}

		entry.Key = ""
		entry.Data = nil
		st.pools.snapshotEntries.Put(entry)
		count++
	}

	if count > 0 {
		slog.Info("Merged remote state from cluster member", "entries", count)
	}
	return nil
}

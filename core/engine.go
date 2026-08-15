// Package core provides a standalone, high-performance embedded key-value storage engine.
package core

import (
	"encoding/gob"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"time"

	pb "github.com/rosewrightdev/oryx/api"
	"github.com/rosewrightdev/oryx/core/clock"
	"github.com/rosewrightdev/oryx/core/evict"
	"github.com/rosewrightdev/oryx/core/hashmap"
	"github.com/rosewrightdev/oryx/core/snap"
	"github.com/rosewrightdev/oryx/core/wal"
	"github.com/rosewrightdev/oryx/core/writer"
	"github.com/rosewrightdev/oryx/kv"
	"github.com/rosewrightdev/oryx/security"
)

// Engine defines the standalone storage engine interface.
type Engine interface {
	Get(key kv.Key) ([]byte, bool)
	Set(key kv.Key, value []byte) error
	Delete(key kv.Key) (bool, error)
	Start()
	Stop()
	ApplySet(req *pb.SetRequest) error
	ApplyDelete(req *pb.DeleteRequest) error
	HM() *hashmap.ShardedMap
	Wal() wal.Waler
	Clock() clock.Clocker
	Writer() *writer.StorageWriter
	Snp() *snap.Snapshotter
	Evt() evict.Evictor
	Occupancy() float64
}

// Config specifies initialization parameters for the core storage engine.
type Config struct {
	Evt           evict.Evictor
	Clock         clock.Clocker
	WalPath       string
	SnpPath       string
	WalInterval   time.Duration
	SnpInterval   time.Duration
	WalSegments   int
	WalBufferSize uint32
	NodeID        kv.NodeID
}

type engine struct {
	clock     clock.Clocker
	wal       wal.Waler
	evt       evict.Evictor
	hm        *hashmap.ShardedMap
	snp       *snap.Snapshotter
	sw        *writer.StorageWriter
	pools     *pools
	nodeID    kv.NodeID
	startOnce sync.Once
	stopOnce  sync.Once
}

type pools struct {
	setRequests     sync.Pool
	deleteRequests  sync.Pool
	snapshotEntries sync.Pool
}

func newPools() *pools {
	return &pools{
		setRequests: sync.Pool{
			New: func() any { return &pb.SetRequest{} },
		},
		deleteRequests: sync.Pool{
			New: func() any { return &pb.DeleteRequest{} },
		},
		snapshotEntries: sync.Pool{
			New: func() any { return &snap.SnapshotEntry{} },
		},
	}
}

// NewEngine creates and initializes a standalone core storage engine.
func NewEngine(config Config) (Engine, error) {
	w, err := wal.NewWal(config.WalPath, config.WalInterval, config.WalBufferSize, config.WalSegments)
	if err != nil {
		return nil, err
	}

	eng := &engine{
		hm:     hashmap.NewShardedMap(),
		wal:    w,
		clock:  config.Clock,
		evt:    config.Evt,
		nodeID: config.NodeID,
		pools:  newPools(),
	}

	if err := eng.recover(config.SnpPath); err != nil {
		slog.Error("Failed to recover database state", "error", err)
	}

	eng.sw = writer.NewStorageWriter(eng.hm, eng.wal, eng.clock)

	stateTransferEncoder := func(enc *gob.Encoder) error {
		var encodeErr error
		eng.hm.Range(func(k kv.Key, v kv.Value) bool {
			snapEntry := eng.pools.snapshotEntries.Get().(*snap.SnapshotEntry)
			snapEntry.Key = k
			snapEntry.Data = v.Data
			snapEntry.Timestamp = v.Timestamp
			snapEntry.NodeID = kv.NodeID(v.NodeID)
			snapEntry.Tombstone = v.Tombstone

			if err := enc.Encode(snapEntry); err != nil {
				encodeErr = fmt.Errorf("failed to encode snapshot entry: %w", err)
				snapEntry.Key = ""
				snapEntry.Data = nil
				snapEntry.NodeID = ""
				eng.pools.snapshotEntries.Put(snapEntry)
				return false
			}
			snapEntry.Key = ""
			snapEntry.Data = nil
			snapEntry.NodeID = ""
			eng.pools.snapshotEntries.Put(snapEntry)
			return true
		})
		return encodeErr
	}

	snp, err := snap.NewSnapshotter(config.SnpPath, config.SnpInterval, w, stateTransferEncoder)
	if err != nil {
		return nil, err
	}
	eng.snp = snp

	if eng.evt != nil {
		eng.evt.SetEvictCallback(eng.Evict)
	}

	return eng, nil
}

func (eng *engine) Start() {
	eng.startOnce.Do(func() {
		if eng.snp != nil {
			eng.snp.Start()
		}
		if eng.wal != nil {
			eng.wal.Start()
		}
		if eng.evt != nil {
			eng.evt.Start()
		}
	})
}

func (eng *engine) Stop() {
	eng.stopOnce.Do(func() {
		if eng.snp != nil {
			eng.snp.Stop()
		}
		if eng.wal != nil {
			eng.wal.Stop()
		}
		if eng.evt != nil {
			eng.evt.Stop()
		}
	})
}

func (eng *engine) Get(key kv.Key) ([]byte, bool) {
	hash := kv.HashKey(security.HashFunc(key))
	iv, ok := eng.hm.Load(key, hash)
	if ok && !iv.Tombstone {
		if eng.evt != nil {
			eng.evt.Publish(key, hash)
		}
		return iv.Data, true
	}
	return nil, false
}

func (eng *engine) Set(key kv.Key, value []byte) error {
	hash := security.HashFunc(key)
	if eng.evt != nil {
		eng.evt.Publish(key, hash)
	}

	ts := eng.clock.Now()

	req := eng.pools.setRequests.Get().(*pb.SetRequest)
	req.Key = key
	req.Value = value
	req.Timestamp = ts
	req.NodeId = string(eng.nodeID)

	err := eng.sw.ApplySet(req)
	req.Reset()
	eng.pools.setRequests.Put(req)
	return err
}

func (eng *engine) Delete(key kv.Key) (bool, error) {
	hash := security.HashFunc(key)
	hk := kv.HashKey(hash)
	iv, ok := eng.hm.Load(key, hk)
	if !ok || iv.Tombstone {
		return false, nil
	}

	if eng.evt != nil {
		eng.evt.PublishDelete(key, hash)
	}

	ts := eng.clock.Now()

	req := eng.pools.deleteRequests.Get().(*pb.DeleteRequest)
	req.Key = key
	req.Timestamp = ts
	req.NodeId = string(eng.nodeID)

	err := eng.sw.ApplyDelete(req)
	req.Reset()
	eng.pools.deleteRequests.Put(req)
	return true, err
}

func (eng *engine) Evict(key kv.Key, reason evict.Reason) error {
	hash := security.HashFunc(key)
	if reason == evict.ReasonCapacity {
		eng.hm.Delete(key, hash)
		return nil
	}

	ts := eng.clock.Now()
	req := eng.pools.deleteRequests.Get().(*pb.DeleteRequest)
	req.Key = key
	req.Timestamp = ts
	req.NodeId = string(eng.nodeID)

	eng.hm.Store(key, hash, kv.Value{
		Timestamp: ts,
		NodeID:    string(eng.nodeID),
		Tombstone: true,
	})
	err := eng.wal.Publish(key, hash, req)
	req.Reset()
	eng.pools.deleteRequests.Put(req)
	return err
}

func (eng *engine) ApplySet(req *pb.SetRequest) error {
	return eng.sw.ApplySet(req)
}

func (eng *engine) ApplyDelete(req *pb.DeleteRequest) error {
	return eng.sw.ApplyDelete(req)
}

func (eng *engine) HM() *hashmap.ShardedMap {
	return eng.hm
}

func (eng *engine) Wal() wal.Waler {
	return eng.wal
}

func (eng *engine) Clock() clock.Clocker {
	return eng.clock
}

func (eng *engine) Writer() *writer.StorageWriter {
	return eng.sw
}

func (eng *engine) Snp() *snap.Snapshotter {
	return eng.snp
}

func (eng *engine) Evt() evict.Evictor {
	return eng.evt
}

func (eng *engine) Occupancy() float64 {
	if occupier, ok := eng.evt.(interface{ Occupancy() float64 }); ok {
		return occupier.Occupancy()
	}
	return 0.0
}

func (eng *engine) recover(snpPath string) error {
	if info, err := os.Stat(snpPath); err == nil && info.Size() > 0 {
		file, err := os.Open(snpPath)
		if err != nil {
			return err
		}
		defer func() {
			_ = file.Close()
		}()

		dec := gob.NewDecoder(file)
		count := 0
		for {
			entry := eng.pools.snapshotEntries.Get().(*snap.SnapshotEntry)
			if err := dec.Decode(entry); err != nil {
				entry.Key = ""
				entry.Data = nil
				eng.pools.snapshotEntries.Put(entry)
				if err == io.EOF {
					break
				}
				return err
			}
			eng.hm.Store(entry.Key, security.HashFunc(entry.Key), kv.Value{
				Data:      entry.Data,
				Timestamp: entry.Timestamp,
				NodeID:    string(entry.NodeID),
				Tombstone: entry.Tombstone,
			})
			entry.Key = ""
			entry.Data = nil
			entry.NodeID = ""
			eng.pools.snapshotEntries.Put(entry)
			count++
		}
		slog.Info("Loaded state from snapshot", "path", snpPath, "keys", count)
	}

	updates, err := eng.wal.Replay()
	if err != nil {
		return err
	}
	for k, v := range updates {
		h := security.HashFunc(k)
		eng.hm.StoreLWW(k, h, v)
	}
	if len(updates) > 0 {
		slog.Info("Replayed updates from WAL", "count", len(updates))
	}

	return nil
}

package dkv

import (
	"encoding/gob"
	"log/slog"
	"os"
	"sync"
	"time"

	pb "github.com/rosewrightdev/dkv/api"
	"google.golang.org/protobuf/proto"
)

type Engine interface {
	Get(key Key) (Value, bool)
	Set(key Key, value Value) error
	Delete(key Key) error
	Start()
	Stop()
	Snapshot() error
	SyncPull(knownDigests map[int32]uint64) ([]*pb.SetRequest, []*pb.DeleteRequest, error)
	SyncPush(sets []*pb.SetRequest, deletes []*pb.DeleteRequest) error
}

type engine struct {
	hm                *shardedMap
	wal               Waler
	sss               *SnapShotService
	evictionService   Evictor
	setRequestPool    sync.Pool
	deleteRequestPool sync.Pool
	snapshotEntryPool sync.Pool
	clock             Clock
	cluster           *ClusterService
	entropy           *AntiEntropyService
}

type EngineConfig struct {
	walPath         string
	sssPath         string
	walSyncInterval time.Duration
	sssInterval     time.Duration
	walBufferSize   uint32
	walSegments     int
	evictionService Evictor
	clock           Clock
	clusterConfig   ClusterConfig
	syncInterval    time.Duration
}

// snapshotEntry is used for streaming serialization
type snapshotEntry struct {
	Key       Key
	Value     Value
	Timestamp int64
	IsDeleted bool
}

func newEngine(config EngineConfig) (Engine, error) {
	wal, err := newWal(config.walPath, config.walSyncInterval, config.walBufferSize, config.walSegments)
	if err != nil {
		return nil, err
	}

	eng := &engine{
		hm:    newShardedMap(),
		wal:   wal,
		clock: config.clock,
	}

	eng.setRequestPool = sync.Pool{
		New: func() any {
			return &pb.SetRequest{}
		},
	}
	eng.deleteRequestPool = sync.Pool{
		New: func() any {
			return &pb.DeleteRequest{}
		},
	}
	eng.snapshotEntryPool = sync.Pool{
		New: func() any {
			return &snapshotEntry{}
		},
	}

	if err := eng.recover(config.sssPath); err != nil {
		slog.Error("Failed to recover database state", "error", err)
	}

	sss, err := newSnapshotService(config.sssPath, config.sssInterval, wal, eng.streamToEncoder)
	if err != nil {
		return nil, err
	}
	eng.sss = sss
	eng.evictionService = config.evictionService
	eng.evictionService.SetEvictCallback(eng.Evict)

	if config.clusterConfig.BindPort != 0 {
		cs, err := newClusterService(config.clusterConfig, eng.onGossipMessage)
		if err != nil {
			return nil, err
		}
		eng.cluster = cs
	}

	eng.entropy = newAntiEntropyService(eng, config.syncInterval)

	return eng, nil
}

func (eng *engine) Start() {
	eng.sss.start()
	eng.wal.start()
	eng.evictionService.start()
	if eng.cluster != nil {
		if err := eng.cluster.start(); err != nil {
			slog.Error("Failed to start cluster service", "error", err)
		}
	}
	eng.entropy.start()
}

func (eng *engine) Stop() {
	eng.entropy.stop()
	if eng.cluster != nil {
		if err := eng.cluster.stop(); err != nil {
			slog.Error("Failed to stop cluster service", "error", err)
		}
	}
	eng.sss.stop()
	eng.wal.stop()
	eng.evictionService.stop()
}

func (eng *engine) Get(key Key) (Value, bool) {
	hash := hashFunc(key)
	eng.evictionService.publish(key, hash)
	iv, ok := eng.hm.Load(key, hash)
	if !ok || iv.IsDeleted {
		return nil, false
	}
	return iv.Data, true
}

func (eng *engine) Set(key Key, value Value) error {
	hash := hashFunc(key)
	eng.evictionService.publish(key, hash)
	
	ts := eng.clock.Now()
	
	req := eng.setRequestPool.Get().(*pb.SetRequest)
	req.Key = key
	req.Value = value
	req.Timestamp = ts

	err := eng.wal.publish(key, hash, req)

	if err != nil {
		return err
	}

	eng.hm.Store(key, hash, internalValue{
		Data:      value,
		Timestamp: ts,
		IsDeleted: false,
	})

	if eng.cluster != nil {
		entry := pb.WalEntry{Entry: &pb.WalEntry_Set{Set: req}}
		if data, err := proto.Marshal(&entry); err == nil {
			eng.cluster.Broadcast(data)
		}
	}
	
	eng.setRequestPool.Put(req)
	return nil
}

func (eng *engine) Delete(key Key) error {
	hash := hashFunc(key)
	eng.evictionService.publishDelete(key, hash)
	
	ts := eng.clock.Now()
	
	req := eng.deleteRequestPool.Get().(*pb.DeleteRequest)
	req.Key = key
	req.Timestamp = ts

	err := eng.wal.publish(key, hash, req)

	if err != nil {
		return err
	}
	eng.hm.Store(key, hash, internalValue{
		Timestamp: ts,
		IsDeleted: true,
	})

	if eng.cluster != nil {
		entry := pb.WalEntry{Entry: &pb.WalEntry_Delete{Delete: req}}
		if data, err := proto.Marshal(&entry); err == nil {
			eng.cluster.Broadcast(data)
		}
	}
	
	req.Reset()
	eng.deleteRequestPool.Put(req)

	return nil
}

func (eng *engine) Evict(key Key) error {
	hash := hashFunc(key)
	
	ts := eng.clock.Now()
	
	req := eng.deleteRequestPool.Get().(*pb.DeleteRequest)
	req.Key = key
	req.Timestamp = ts

	err := eng.wal.publish(key, hash, req)

	if err != nil {
		return err
	}
	eng.hm.Store(key, hash, internalValue{
		Timestamp: ts,
		IsDeleted: true,
	})

	if eng.cluster != nil {
		entry := pb.WalEntry{Entry: &pb.WalEntry_Delete{Delete: req}}
		if data, err := proto.Marshal(&entry); err == nil {
			eng.cluster.Broadcast(data)
		}
	}
	
	req.Reset()
	eng.deleteRequestPool.Put(req)

	return nil
}

func (eng *engine) Snapshot() error {
	return eng.sss.create()
}

func (eng *engine) streamToEncoder(enc *gob.Encoder) error {
	var err error
	eng.hm.Range(func(k, v any) bool {
		iv := v.(internalValue)
		entry := eng.snapshotEntryPool.Get().(*snapshotEntry)
		entry.Key = k.(Key)
		entry.Value = iv.Data
		entry.Timestamp = iv.Timestamp
		entry.IsDeleted = iv.IsDeleted
		
		if e := enc.Encode(entry); e != nil {
			err = e
			entry.Key = ""
			entry.Value = nil
			eng.snapshotEntryPool.Put(entry)
			return false
		}
		entry.Key = ""
		entry.Value = nil
		eng.snapshotEntryPool.Put(entry)
		return true
	})
	return err
}

func (eng *engine) recover(sssPath string) error {
	if info, err := os.Stat(sssPath); err == nil && info.Size() > 0 {
		file, err := os.Open(sssPath)
		if err != nil {
			return err
		}
		defer file.Close()

		dec := gob.NewDecoder(file)
		count := 0
		for {
			var entry snapshotEntry
			if err := dec.Decode(&entry); err != nil {
				break // End of file or error
			}
			eng.hm.Store(entry.Key, hashFunc(entry.Key), internalValue{
				Data:      entry.Value,
				Timestamp: entry.Timestamp,
				IsDeleted: entry.IsDeleted,
			})
			count++
		}
		slog.Info("Loaded state from snapshot", "path", sssPath, "keys", count)
	}

	updates, err := eng.wal.replay()
	if err != nil {
		return err
	}
	for k, v := range updates {
		h := hashFunc(k)
		eng.hm.Store(k, h, v)
	}
	if len(updates) > 0 {
		slog.Info("Replayed updates from WAL", "count", len(updates))
	}

	return nil
}

func (eng *engine) onGossipMessage(data []byte) {
	var entry pb.WalEntry
	if err := proto.Unmarshal(data, &entry); err != nil {
		slog.Error("Failed to unmarshal gossip message", "error", err)
		return
	}

	switch kv := entry.Entry.(type) {
	case *pb.WalEntry_Set:
		eng.applyGossipSet(kv.Set)
	case *pb.WalEntry_Delete:
		eng.applyGossipDelete(kv.Delete)
	}
}

func (eng *engine) applyGossipSet(req *pb.SetRequest) {
	hash := hashFunc(req.Key)

	// LWW Conflict Resolution
	existing, ok := eng.hm.Load(req.Key, hash)
	if ok && existing.Timestamp >= req.Timestamp {
		return // Ignore older or equal update
	}

	// Apply update
	eng.hm.Store(req.Key, hash, internalValue{
		Data:      req.Value,
		Timestamp: req.Timestamp,
		IsDeleted: false,
	})

	// Also write to WAL for persistence
	_ = eng.wal.publish(req.Key, hash, req)
}

func (eng *engine) applyGossipDelete(req *pb.DeleteRequest) {
	hash := hashFunc(req.Key)

	existing, ok := eng.hm.Load(req.Key, hash)
	if ok && existing.Timestamp >= req.Timestamp {
		return
	}

	eng.hm.Store(req.Key, hash, internalValue{
		Timestamp: req.Timestamp,
		IsDeleted: true,
	})
	_ = eng.wal.publish(req.Key, hash, req)
}

func (eng *engine) SyncPull(knownDigests map[int32]uint64) ([]*pb.SetRequest, []*pb.DeleteRequest, error) {
	localDigests := eng.hm.Digests()
	var sets []*pb.SetRequest
	var deletes []*pb.DeleteRequest

	for shardID, localHash := range localDigests {
		remoteHash, ok := knownDigests[shardID]
		if !ok || localHash != remoteHash {
			// Shard mismatch, collect all entries for this shard
			// In a more advanced version, we would use Merkle trees here
			shard := eng.hm[shardID]
			shard.mu.RLock()
			for k, v := range shard.m {
				if v.IsDeleted {
					deletes = append(deletes, &pb.DeleteRequest{
						Key:       k,
						Timestamp: v.Timestamp,
					})
				} else {
					sets = append(sets, &pb.SetRequest{
						Key:       k,
						Value:     v.Data,
						Timestamp: v.Timestamp,
					})
				}
			}
			shard.mu.RUnlock()
		}
	}
	return sets, deletes, nil
}

func (eng *engine) SyncPush(sets []*pb.SetRequest, deletes []*pb.DeleteRequest) error {
	for _, s := range sets {
		eng.applyGossipSet(s)
	}
	for _, d := range deletes {
		eng.applyGossipDelete(d)
	}
	return nil
}

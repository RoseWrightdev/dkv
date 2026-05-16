package dkv

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"time"

	pb "github.com/rosewrightdev/dkv/api"
	"google.golang.org/protobuf/proto"
)

type Engine interface {
	Get(key Key) ([]byte, bool)
	Set(key Key, value []byte) error
	Delete(key Key) error
	Start()
	Stop()
	Snapshot() error
	SyncPull(knownDigests map[ShardID]ShardDigest) ([]*pb.SetRequest, []*pb.DeleteRequest, error)
	SyncPush(sets []*pb.SetRequest, deletes []*pb.DeleteRequest) error
}

type engine struct {
	hm              *shardedMap
	wal             Waler
	sss             *SnapShotService
	evictionService Evictor
	pools           *resourcePools
	clock           Clock
	cluster         Cluster
	clusterConfig   ClusterConfig
	entropy         *AntiEntropyService
	startOnce       sync.Once
	stopOnce        sync.Once
}

type resourcePools struct {
	setRequests       sync.Pool
	deleteRequests    sync.Pool
	snapshotEntries   sync.Pool
	walEntries        sync.Pool
	walSetWrappers    sync.Pool
	walDeleteWrappers sync.Pool
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
	gossipInterval  time.Duration
}

// snapshotEntry is used for streaming serialization
type snapshotEntry struct {
	Key       Key
	Data      []byte
	Timestamp int64
	Tombstone bool
}

func newEngine(config EngineConfig) (Engine, error) {
	wal, err := newWal(config.walPath, config.walSyncInterval, config.walBufferSize, config.walSegments)
	if err != nil {
		return nil, err
	}

	eng := &engine{
		hm:            newShardedMap(),
		wal:           wal,
		clock:         config.clock,
		clusterConfig: config.clusterConfig,
		pools: &resourcePools{
			setRequests: sync.Pool{
				New: func() any { return &pb.SetRequest{} },
			},
			deleteRequests: sync.Pool{
				New: func() any { return &pb.DeleteRequest{} },
			},
			snapshotEntries: sync.Pool{
				New: func() any { return &snapshotEntry{} },
			},
			walEntries: sync.Pool{
				New: func() any { return &pb.WalEntry{} },
			},
			walSetWrappers: sync.Pool{
				New: func() any { return &pb.WalEntry_Set{} },
			},
			walDeleteWrappers: sync.Pool{
				New: func() any { return &pb.WalEntry_Delete{} },
			},
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

	eng.cluster = &NopCluster{}
	if !config.clusterConfig.SingleNode {
		cs, err := newClusterService(
			config.clusterConfig,
			eng.onGossipMessage,
			eng.getLocalState,
			eng.mergeRemoteState,
		)
		if err != nil {
			return nil, err
		}
		eng.cluster = cs
	}

	if !config.clusterConfig.SingleNode {
		eng.entropy = newAntiEntropyService(eng.cluster, eng, config.gossipInterval)
	}

	return eng, nil
}

func (eng *engine) Start() {
	eng.startOnce.Do(func() {
		eng.sss.start()
		eng.wal.start()
		eng.evictionService.start()
		if err := eng.cluster.start(); err != nil {
			panic(fmt.Sprintf("failed to start cluster service: %v", err))
		}
		if eng.entropy != nil {
			eng.entropy.start()
		}
	})
}

func (eng *engine) Stop() {
	eng.stopOnce.Do(func() {
		if eng.entropy != nil {
			eng.entropy.stop()
		}
		eng.sss.stop()
		eng.wal.stop()
		eng.evictionService.stop()
		if err := eng.cluster.stop(); err != nil {
			panic(fmt.Sprintf("failed to stop cluster service: %v", err))
		}
	})
}

func (eng *engine) Get(key Key) ([]byte, bool) {
	hash := hashKey(hashFunc(key))
	iv, ok := eng.hm.Load(key, hash)
	if !ok || iv.Tombstone {
		return nil, false
	}
	eng.evictionService.publish(key, hash)
	return iv.Data, true
}

func (eng *engine) Set(key Key, value []byte) error {
	hash := hashFunc(key)
	eng.evictionService.publish(key, hash)

	ts := eng.clock.Now()

	req := eng.pools.setRequests.Get().(*pb.SetRequest)
	req.Key = key
	req.Value = value
	req.Timestamp = ts

	err := eng.wal.publish(key, hash, req)

	if err != nil {
		return err
	}

	eng.hm.Store(key, hash, Value{
		Data:      value,
		Timestamp: ts,
		Tombstone: false,
	})

	if !eng.clusterConfig.SingleNode {
		entry := eng.pools.walEntries.Get().(*pb.WalEntry)
		wrapper := eng.pools.walSetWrappers.Get().(*pb.WalEntry_Set)
		wrapper.Set = req
		entry.Entry = wrapper
		if data, err := proto.Marshal(entry); err == nil {
			eng.cluster.Broadcast(data)
		}
		entry.Entry = nil
		wrapper.Set = nil
		eng.pools.walSetWrappers.Put(wrapper)
		eng.pools.walEntries.Put(entry)
	}

	eng.pools.setRequests.Put(req)
	return nil
}

func (eng *engine) Delete(key Key) error {
	hash := hashFunc(key)
	eng.evictionService.publishDelete(key, hash)

	ts := eng.clock.Now()

	req := eng.pools.deleteRequests.Get().(*pb.DeleteRequest)
	req.Key = key
	req.Timestamp = ts

	err := eng.wal.publish(key, hash, req)

	if err != nil {
		return err
	}
	eng.hm.Store(key, hash, Value{
		Timestamp: ts,
		Tombstone: true,
	})

	if !eng.clusterConfig.SingleNode {
		entry := eng.pools.walEntries.Get().(*pb.WalEntry)
		wrapper := eng.pools.walDeleteWrappers.Get().(*pb.WalEntry_Delete)
		wrapper.Delete = req
		entry.Entry = wrapper
		if data, err := proto.Marshal(entry); err == nil {
			eng.cluster.Broadcast(data)
		}
		entry.Entry = nil
		wrapper.Delete = nil
		eng.pools.walDeleteWrappers.Put(wrapper)
		eng.pools.walEntries.Put(entry)
	}

	req.Reset()
	eng.pools.deleteRequests.Put(req)

	return nil
}

func (eng *engine) Evict(key Key) error {
	hash := hashFunc(key)

	ts := eng.clock.Now()

	req := eng.pools.deleteRequests.Get().(*pb.DeleteRequest)
	req.Key = key
	req.Timestamp = ts

	err := eng.wal.publish(key, hash, req)

	if err != nil {
		return err
	}
	eng.hm.Store(key, hash, Value{
		Timestamp: ts,
		Tombstone: true,
	})

	if !eng.clusterConfig.SingleNode {
		entry := eng.pools.walEntries.Get().(*pb.WalEntry)
		wrapper := eng.pools.walDeleteWrappers.Get().(*pb.WalEntry_Delete)
		wrapper.Delete = req
		entry.Entry = wrapper
		if data, err := proto.Marshal(entry); err == nil {
			eng.cluster.Broadcast(data)
		}
		entry.Entry = nil
		wrapper.Delete = nil
		eng.pools.walDeleteWrappers.Put(wrapper)
		eng.pools.walEntries.Put(entry)
	}

	req.Reset()
	eng.pools.deleteRequests.Put(req)

	return nil
}

func (eng *engine) Snapshot() error {
	return eng.sss.create()
}

func (eng *engine) streamToEncoder(enc *gob.Encoder) error {
	for i := range shardCount {
		shard := eng.hm[i]
		shard.mu.RLock()
		for k, v := range shard.m {
			entry := eng.pools.snapshotEntries.Get().(*snapshotEntry)
			entry.Key = k
			entry.Data = v.Data
			entry.Timestamp = v.Timestamp
			entry.Tombstone = v.Tombstone

			if err := enc.Encode(entry); err != nil {
				shard.mu.RUnlock()
				entry.Key = ""
				entry.Data = nil
				eng.pools.snapshotEntries.Put(entry)
				return err
			}
			entry.Key = ""
			entry.Data = nil
			eng.pools.snapshotEntries.Put(entry)
		}
		shard.mu.RUnlock()
	}
	return nil
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
			entry := eng.pools.snapshotEntries.Get().(*snapshotEntry)
			if err := dec.Decode(entry); err != nil {
				entry.Key = ""
				entry.Data = nil
				eng.pools.snapshotEntries.Put(entry)
				if err == io.EOF {
					break
				}
				return err
			}
			eng.hm.Store(entry.Key, hashFunc(entry.Key), Value{
				Data:      entry.Data,
				Timestamp: entry.Timestamp,
				Tombstone: entry.Tombstone,
			})
			entry.Key = ""
			entry.Data = nil
			eng.pools.snapshotEntries.Put(entry)
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
		if err := eng.applyGossipSet(kv.Set); err != nil {
			slog.Error("Critical: Failed to apply gossip set", "error", err)
		}
	case *pb.WalEntry_Delete:
		if err := eng.applyGossipDelete(kv.Delete); err != nil {
			slog.Error("Critical: Failed to apply gossip delete", "error", err)
		}
	}
}

func (eng *engine) applyGossipSet(req *pb.SetRequest) error {
	eng.clock.Update(req.Timestamp)
	hash := hashFunc(req.Key)

	// LWW Conflict Resolution
	existing, ok := eng.hm.Load(req.Key, hash)
	if ok && existing.Timestamp >= req.Timestamp {
		return nil // Ignore older or equal update
	}

	// Apply update
	eng.hm.Store(req.Key, hash, Value{
		Data:      req.Value,
		Timestamp: req.Timestamp,
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

	existing, ok := eng.hm.Load(req.Key, hash)
	if ok && existing.Timestamp >= req.Timestamp {
		return nil
	}

	eng.hm.Store(req.Key, hash, Value{
		Timestamp: req.Timestamp,
		Tombstone: true,
	})
	if err := eng.wal.publish(req.Key, hash, req); err != nil {
		return fmt.Errorf("failed to persist gossip delete to WAL: %w", err)
	}
	return nil
}

func (eng *engine) SyncPull(knownDigests map[ShardID]ShardDigest) ([]*pb.SetRequest, []*pb.DeleteRequest, error) {
	localDigests := eng.hm.Digests()
	var sets []*pb.SetRequest
	var deletes []*pb.DeleteRequest

	for shardID, localHash := range localDigests {
		remoteHash, ok := knownDigests[shardID]
		if !ok || localHash != remoteHash {
			// Shard mismatch, collect all entries for this shard
			// In a more advanced version, we would use Merkle trees here
			shard := eng.hm[int(shardID)]
			shard.mu.RLock()
			for k, v := range shard.m {
				if v.Tombstone {
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

func (eng *engine) Digests() map[ShardID]ShardDigest {
	return eng.hm.Digests()
}

func (eng *engine) SyncPush(sets []*pb.SetRequest, deletes []*pb.DeleteRequest) error {
	for _, s := range sets {
		if err := eng.applyGossipSet(s); err != nil {
			return err
		}
	}
	for _, d := range deletes {
		if err := eng.applyGossipDelete(d); err != nil {
			return err
		}
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

func (eng *engine) decodeFromReader(r io.Reader) error {
	dec := gob.NewDecoder(r)
	count := 0
	for {
		entry := eng.pools.snapshotEntries.Get().(*snapshotEntry)
		if err := dec.Decode(entry); err != nil {
			entry.Key = ""
			entry.Data = nil
			eng.pools.snapshotEntries.Put(entry)
			if err == io.EOF {
				break
			}
			return fmt.Errorf("failed to decode snapshot entry: %w", err)
		}

		if entry.Tombstone {
			req := eng.pools.deleteRequests.Get().(*pb.DeleteRequest)
			req.Key = entry.Key
			req.Timestamp = entry.Timestamp
			err := eng.applyGossipDelete(req)
			req.Reset()
			eng.pools.deleteRequests.Put(req)
			if err != nil {
				entry.Key = ""
				entry.Data = nil
				eng.pools.snapshotEntries.Put(entry)
				return err
			}
		} else {
			req := eng.pools.setRequests.Get().(*pb.SetRequest)
			req.Key = entry.Key
			req.Value = entry.Data
			req.Timestamp = entry.Timestamp
			err := eng.applyGossipSet(req)
			req.Reset()
			eng.pools.setRequests.Put(req)
			if err != nil {
				entry.Key = ""
				entry.Data = nil
				eng.pools.snapshotEntries.Put(entry)
				return err
			}
		}

		entry.Key = ""
		entry.Data = nil
		eng.pools.snapshotEntries.Put(entry)
		count++
	}

	if count > 0 {
		slog.Info("Merged remote state from cluster member", "entries", count)
	}
	return nil
}

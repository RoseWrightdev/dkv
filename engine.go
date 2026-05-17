package dkv

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"log/slog"
	"os"
	"slices"
	"sync"
	"time"

	pb "github.com/rosewrightdev/dkv/api"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/proto"
)

type Engine interface {
	Get(key Key) ([]byte, bool)
	Set(key Key, value []byte) error
	Delete(key Key) error
	Owner(key Key) NodeID
	Start()
	Stop()
	Snapshot() error
	RootDigest() RootDigest
	FillShardDigests(dst map[ShardID]Digest)
	FillDigests(dst map[ShardID]ShardDigest)
	SyncPull(requesterID NodeID, root RootDigest, shards map[ShardID]Digest, buckets map[ShardID]ShardDigest) ([]*pb.SetRequest, []*pb.DeleteRequest, error)
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
	creds           credentials.TransportCredentials
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
	pullRequests      sync.Pool
	shardDigests      sync.Pool
	shardMaps         sync.Pool
	bucketMaps        sync.Pool
}

type EngineConfig struct {
	walPath              string
	sssPath              string
	walSyncInterval      time.Duration
	sssInterval          time.Duration
	walBufferSize        uint32
	walSegments          int
	evictionService      Evictor
	clock                Clock
	clusterConfig        ClusterConfig
	gossipInterval       time.Duration
	transportCredentials credentials.TransportCredentials
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

	// todo: refactor toplevel engine pool
	eng := &engine{
		hm:            newShardedMap(),
		wal:           wal,
		clock:         config.clock,
		clusterConfig: config.clusterConfig,
		creds:         config.transportCredentials,
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
			pullRequests: sync.Pool{
				New: func() any {
					return &pb.PullRequest{
						ShardDigests: make(map[uint32]uint64, shardCount),
						SubDigests:   make(map[uint32]*pb.ShardDigests, shardCount),
					}
				},
			},
			shardDigests: sync.Pool{
				New: func() any { return &pb.ShardDigests{} },
			},
			shardMaps: sync.Pool{
				New: func() any { return make(map[ShardID]Digest) },
			},
			bucketMaps: sync.Pool{
				New: func() any {
					m := make(map[ShardID]ShardDigest, shardCount)
					for i := range shardCount {
						m[ShardID(i)] = make([]Digest, subBucketCount)
					}
					return m
				},
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
		eng.entropy = newAntiEntropyService(config.clusterConfig.NodeID, eng.cluster, eng, eng.pools, config.gossipInterval, config.transportCredentials)
	}

	return eng, nil
}

// Start initializes background services.
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

// Stop gracefully shuts down the engine and its background services.
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

func (eng *engine) isLocal(key string) bool {
	if eng.clusterConfig.SingleNode {
		return true
	}
	rf := eng.clusterConfig.ReplicationFactor
	if rf <= 0 {
		rf = 1
	}

	// In a distributed cluster, we are responsible if we are one of the N owners
	owners := eng.cluster.GetOwners(key, rf)
	return slices.Contains(owners, eng.clusterConfig.NodeID)
}

func (eng *engine) Owner(key Key) NodeID {
	if eng.clusterConfig.SingleNode {
		return eng.clusterConfig.NodeID
	}
	return eng.cluster.Owner(key)
}

// Get retrieves the value associated with a key from the sharded map.
func (eng *engine) Get(key Key) ([]byte, bool) {
	hash := hashKey(hashFunc(key))
	iv, ok := eng.hm.Load(key, hash)
	if ok && !iv.Tombstone {
		eng.evictionService.publish(key, hash)
		return iv.Data, true
	}

	// Gateway Proxy: If we don't have it locally, fetch it from an owner
	if !eng.clusterConfig.SingleNode {
		rf := eng.clusterConfig.ReplicationFactor
		if rf <= 0 {
			rf = 1
		}
		owners := eng.cluster.GetOwners(key, rf)

		for _, owner := range owners {
			if owner == eng.clusterConfig.NodeID {
				continue // We already checked local storage
			}

			addr := eng.cluster.AddressForNode(owner)
			if addr == "" {
				continue // Try next owner
			}

			// Proxy the read
			client, err := NewClient(addr, 1*time.Second, eng.creds)
			if err != nil {
				continue // Try next owner
			}
			val, ok, err := client.Get(key)
			client.Close()
			if err != nil || !ok {
				continue
			}
			return val, true
		}
	}

	return nil, false
}

// Set persists a key-value pair to the WAL and updates the sharded map.
func (eng *engine) Set(key Key, value []byte) error {
	hash := hashFunc(key)
	eng.evictionService.publish(key, hash)

	ts := eng.clock.Now()

	req := eng.pools.setRequests.Get().(*pb.SetRequest)
	req.Key = key
	req.Value = value
	req.Timestamp = ts
	req.NodeId = string(eng.clusterConfig.NodeID)

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

// Delete marks a key as deleted by publishing a tombstone to the WAL.
func (eng *engine) Delete(key Key) error {
	hash := hashFunc(key)
	eng.evictionService.publishDelete(key, hash)

	ts := eng.clock.Now()

	req := eng.pools.deleteRequests.Get().(*pb.DeleteRequest)
	req.Key = key
	req.Timestamp = ts
	req.NodeId = string(eng.clusterConfig.NodeID)

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

// Snapshot triggers an immediate persistence of the current state to disk.
func (eng *engine) Snapshot() error {
	return eng.sss.create()
}

func (eng *engine) streamToEncoder(enc *gob.Encoder) error {
	for i := range shardCount {
		shard := eng.hm[i]
		shard.mu.RLock()
		for b := range subBucketCount {
			for k, v := range shard.buckets[b] {
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

func (eng *engine) RootDigest() RootDigest {
	return eng.hm.RootDigest()
}

func (eng *engine) FillShardDigests(dst map[ShardID]Digest) {
	eng.hm.FillShardDigests(dst)
}

func (eng *engine) FillDigests(dst map[ShardID]ShardDigest) {
	eng.hm.FillDigests(dst)
}

// SyncPull performs a hierarchical anti-entropy reconciliation against a remote node's state.
// It uses a 3-level Merkle-style comparison tree to pinpoint divergence with minimal bandwidth:
//
// 1. Root Level: Single global hash check (O(1)).
//
// 2. Shard Level: 128 intermediate shard hashes.
//
// 3. Bucket Level: 64 sub-bucket hashes per mismatched shard.
//
// It returns only the specific records (Sets/Deletes) needed to bring the remote node into sync.
func (eng *engine) SyncPull(requesterID NodeID, root RootDigest, shards map[ShardID]Digest, buckets map[ShardID]ShardDigest) ([]*pb.SetRequest, []*pb.DeleteRequest, error) {
	// Level 1: Global check. If the root hash matches, the entire database is identical.
	if root == eng.RootDigest() {
		return nil, nil, nil
	}

	localShardDigests := eng.pools.shardMaps.Get().(map[ShardID]Digest)
	localBuckets := eng.pools.bucketMaps.Get().(map[ShardID]ShardDigest)
	defer func() {
		for k := range localShardDigests {
			delete(localShardDigests, k)
		}
		for k := range localBuckets {
			delete(localBuckets, k)
		}
		eng.pools.shardMaps.Put(localShardDigests)
		eng.pools.bucketMaps.Put(localBuckets)
	}()

	eng.hm.FillShardDigests(localShardDigests)
	eng.hm.FillDigests(localBuckets)

	var sets []*pb.SetRequest
	var deletes []*pb.DeleteRequest

	for shardID, localShardHash := range localShardDigests {
		remoteShardHash, hasShard := shards[shardID]

		// Level 2: Shard check
		if hasShard && localShardHash == remoteShardHash {
			continue
		}

		remoteBuckets, hasBuckets := buckets[shardID]
		localBucketHashes := localBuckets[shardID]

		// Level 3: Determine which sub-buckets need syncing using a bitmask. Each bit
		// corresponds to a sub-bucket index (0-63). To fit perfectly in a cache line.
		var mismatchMask uint64
		if !hasBuckets || len(remoteBuckets) != len(localBucketHashes) {
			// If the remote node is missing the bucket hashes entirely,
			// mark all 64 bits for synchronization.
			mismatchMask = ^uint64(0)
		} else {
			// Compare each sub-bucket hash and set the corresponding bit if they differ.
			for i := range subBucketCount {
				if localBucketHashes[i] != remoteBuckets[i] {
					mismatchMask |= (1 << i)
				}
			}
		}

		if mismatchMask > 0 {
			shard := eng.hm[int(shardID)]
			shard.mu.RLock()
			for b := range subBucketCount {
				if (mismatchMask & (1 << b)) != 0 {
					for k, v := range shard.buckets[b] {
						// Filter: Only send keys the requester is responsible for
						if !eng.clusterConfig.SingleNode {
							isResponsible := false
							if slices.Contains(eng.cluster.GetOwners(Key(k), eng.clusterConfig.ReplicationFactor), requesterID) {
									isResponsible = true
								}
							if !isResponsible {
								continue
							}
						}

						if v.Tombstone {
							req := eng.pools.deleteRequests.Get().(*pb.DeleteRequest)
							req.Key = k
							req.Timestamp = v.Timestamp
							req.NodeId = v.NodeID
							deletes = append(deletes, req)
						} else {
							req := eng.pools.setRequests.Get().(*pb.SetRequest)
							req.Key = k
							req.Value = v.Data
							req.Timestamp = v.Timestamp
							req.NodeId = v.NodeID
							sets = append(sets, req)
						}
					}
				}
			}
			shard.mu.RUnlock()
		}
	}
	return sets, deletes, nil
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

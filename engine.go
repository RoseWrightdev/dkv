package dkv

import (
	"fmt"
	"log/slog"
	"sync"
	"time"

	pb "github.com/rosewrightdev/dkv/api"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/proto"
)

// Engine defines the core storage and replication engine interface of the dkv node.
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

	clientMu sync.RWMutex
	clients  map[string]*Client
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

// EngineConfig specifies the parameters required to initialize and run a dkv Engine.
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
		clients:       make(map[string]*Client),
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

		// Close all cached clients
		eng.clientMu.Lock()
		for _, client := range eng.clients {
			_ = client.Close()
		}
		eng.clients = nil
		eng.clientMu.Unlock()
	})
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

func (eng *engine) Evict(key Key, reason EvictReason) error {
	hash := hashFunc(key)

	if reason == EvictReasonCapacity {
		eng.hm.Delete(key, hash)
		return nil
	}

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

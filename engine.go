package dkv

import (
	"encoding/gob"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"time"

	pb "github.com/rosewrightdev/dkv/api"
	"github.com/rosewrightdev/dkv/entropy"
	"github.com/rosewrightdev/dkv/evict"
	"github.com/rosewrightdev/dkv/gateway"
	"github.com/rosewrightdev/dkv/gossip"
	"github.com/rosewrightdev/dkv/hashmap"
	"github.com/rosewrightdev/dkv/kv"
	"github.com/rosewrightdev/dkv/mesh"
	"github.com/rosewrightdev/dkv/security"
	"github.com/rosewrightdev/dkv/snap"
	"github.com/rosewrightdev/dkv/trans"
	"github.com/rosewrightdev/dkv/wal"
	"github.com/rosewrightdev/dkv/writer"
	"google.golang.org/grpc/credentials"
)

// Engine defines the core storage and replication engine interface of the dkv node.
type Engine interface {
	Get(key kv.Key) ([]byte, bool)
	Set(key kv.Key, value []byte) error
	Delete(key kv.Key) error
	Owner(key kv.Key) kv.NodeID
	NodeID() kv.NodeID
	Start()
	Stop()
	SyncPull(pullConfig *entropy.PullConfig) ([]*pb.SetRequest, []*pb.DeleteRequest, error)
	SyncPush(sets []*pb.SetRequest, deletes []*pb.DeleteRequest) error
	Addr() string
	GossipAddr() string
}

type engine struct {
	creds      credentials.TransportCredentials
	clock      Clock
	wal        wal.Waler
	mesh       mesh.Mesher
	evt        evict.Evictor
	gw         *gateway.Gateway
	syncer     *entropy.Syncer
	pools      *pools
	hm         *hashmap.ShardedMap
	snp        *snap.Snapshotter
	sw         *writer.StorageWriter
	meshConfig mesh.MeshConfig
	startOnce  sync.Once
	stopOnce   sync.Once
}

// EngineConfig specifies the parameters required to initialize and run a dkv Engine.
type EngineConfig struct {
	evt            evict.Evictor
	clock          Clock
	creds          credentials.TransportCredentials
	walPath        string
	snpPath        string
	meshConfig     mesh.MeshConfig
	walInterval    time.Duration
	snpInterval    time.Duration
	walSegments    int
	gossipInterval time.Duration
	walBufferSize  uint32
}

func newEngine(config EngineConfig) (Engine, error) {
	w, err := wal.NewWal(config.walPath, config.walInterval, config.walBufferSize, config.walSegments)
	if err != nil {
		return nil, err
	}

	eng := &engine{
		hm:         hashmap.NewShardedMap(),
		wal:        w,
		clock:      config.clock,
		meshConfig: config.meshConfig,
		creds:      config.creds,
		pools:      newPools(),
	}

	if err := eng.recover(config.snpPath); err != nil {
		slog.Error("Failed to recover database state", "error", err)
	}

	swWriter := writer.NewStorageWriter(eng.hm, eng.wal, eng.clock, eng.mesh, &eng.meshConfig)
	eng.sw = swWriter

	stateTransfer := trans.NewStateTransfer(eng.hm, swWriter)

	snp, err := snap.NewSnapshotter(config.snpPath, config.snpInterval, w, stateTransfer.StreamToEncoder)
	if err != nil {
		return nil, err
	}
	eng.snp = snp
	eng.evt = config.evt
	eng.evt.SetEvictCallback(eng.Evict)

	gossipService := gossip.NewGossip(swWriter)

	eng.mesh = &mesh.NopMesh{}
	if !config.meshConfig.SingleNode {
		meshObj, err := mesh.NewMesh(
			gossipService,
			stateTransfer,
			config.meshConfig,
		)
		if err != nil {
			return nil, err
		}
		eng.mesh = meshObj
	}
	swWriter.SetMesh(eng.mesh)

	eng.gw = gateway.NewGateway(eng.mesh, &eng.meshConfig, config.creds)
	eng.gw.SetStateWriter(swWriter)

	if !config.meshConfig.SingleNode {
		eng.syncer = entropy.NewSyncer(&entropy.SyncerConfig{
			NodeID:     config.meshConfig.NodeID,
			Writer:     eng.sw,
			Mesh:       eng.mesh,
			MeshConfig: &eng.meshConfig,
			Hm:         eng.hm,
			Interval:   config.gossipInterval,
			Creds:      config.creds,
			Cc:         eng.gw.Cc(),
		})
	}

	return eng, nil
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

// Start initializes background services.
func (eng *engine) Start() {
	eng.startOnce.Do(func() {
		eng.snp.Start()
		eng.wal.Start()
		eng.evt.Start()
		if err := eng.mesh.Start(); err != nil {
			panic(fmt.Sprintf("failed to start cluster service: %v", err))
		}
		if eng.syncer != nil {
			eng.syncer.Start()
		}
	})
}

// Stop gracefully shuts down the engine and its background services.
func (eng *engine) Stop() {
	eng.stopOnce.Do(func() {
		if eng.syncer != nil {
			eng.syncer.Stop()
		}
		eng.snp.Stop()
		eng.wal.Stop()
		eng.evt.Stop()
		if err := eng.mesh.Stop(); err != nil {
			panic(fmt.Sprintf("failed to stop cluster service: %v", err))
		}
		eng.gw.Close()
	})
}

// Get retrieves the value associated with a key from the sharded map.
func (eng *engine) Get(key kv.Key) ([]byte, bool) {
	hash := kv.HashKey(security.HashFunc(key))
	iv, ok := eng.hm.Load(key, hash)
	if ok && !iv.Tombstone {
		eng.evt.Publish(key, hash)
		return iv.Data, true
	} else if ok && iv.Tombstone {
		// We have a local tombstone. Do not proxy the read as the key is known to be deleted.
		return nil, false
	}

	// Gateway Proxy: If we don't have it locally, fetch it from an owner
	if !eng.meshConfig.SingleNode {
		return eng.gw.Get(key)
	}

	return nil, false
}

// Set persists a key-value pair to the WAL and updates the sharded map.
func (eng *engine) Set(key kv.Key, value []byte) error {
	hash := security.HashFunc(key)
	eng.evt.Publish(key, hash)

	ts := eng.clock.Now()

	if eng.meshConfig.SingleNode {
		// Hot path: bypass ApplySet to avoid redundant clock.Update, double HashFunc, and
		// the always-true isLocal() ring check — restoring the pre-refactor baseline perf.
		req := eng.pools.setRequests.Get().(*pb.SetRequest)
		req.Key = key
		req.Value = value
		req.Timestamp = ts
		req.NodeId = string(eng.meshConfig.NodeID)

		eng.hm.Store(key, hash, kv.Value{
			Data:      value,
			Timestamp: ts,
			NodeID:    string(eng.meshConfig.NodeID),
			Tombstone: false,
		})
		err := eng.wal.Publish(key, hash, req)
		req.Reset()
		eng.pools.setRequests.Put(req)
		return err
	}

	return eng.gw.Set(key, value, ts)
}

// Delete marks a key as deleted by publishing a tombstone to the WAL.
func (eng *engine) Delete(key kv.Key) error {
	hash := security.HashFunc(key)
	eng.evt.PublishDelete(key, hash)

	ts := eng.clock.Now()

	if eng.meshConfig.SingleNode {
		// Hot path: same bypass rationale as Set — skip ApplyDelete overhead.
		req := eng.pools.deleteRequests.Get().(*pb.DeleteRequest)
		req.Key = key
		req.Timestamp = ts
		req.NodeId = string(eng.meshConfig.NodeID)

		eng.hm.Store(key, hash, kv.Value{
			Timestamp: ts,
			NodeID:    string(eng.meshConfig.NodeID),
			Tombstone: true,
		})
		err := eng.wal.Publish(key, hash, req)
		req.Reset()
		eng.pools.deleteRequests.Put(req)
		return err
	}

	return eng.gw.Delete(key, ts)
}

func (eng *engine) Evict(key kv.Key, reason evict.EvictReason) error {
	hash := security.HashFunc(key)

	if reason == evict.EvictReasonCapacity {
		eng.hm.Delete(key, hash)
		return nil
	}

	ts := eng.clock.Now()

	if eng.meshConfig.SingleNode {
		// Hot path: same bypass rationale as Set/Delete.
		req := eng.pools.deleteRequests.Get().(*pb.DeleteRequest)
		req.Key = key
		req.Timestamp = ts
		req.NodeId = string(eng.meshConfig.NodeID)

		eng.hm.Store(key, hash, kv.Value{
			Timestamp: ts,
			NodeID:    string(eng.meshConfig.NodeID),
			Tombstone: true,
		})
		err := eng.wal.Publish(key, hash, req)
		req.Reset()
		eng.pools.deleteRequests.Put(req)
		return err
	}

	return eng.gw.Delete(key, ts)
}

func (eng *engine) recover(snpPath string) error {
	if info, err := os.Stat(snpPath); err == nil && info.Size() > 0 {
		// #nosec G304
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
				Tombstone: entry.Tombstone,
			})
			entry.Key = ""
			entry.Data = nil
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
		eng.hm.Store(k, h, v)
	}
	if len(updates) > 0 {
		slog.Info("Replayed updates from WAL", "count", len(updates))
	}

	return nil
}

func (eng *engine) SyncPull(pullConfig *entropy.PullConfig) ([]*pb.SetRequest, []*pb.DeleteRequest, error) {
	return eng.syncer.Pull(pullConfig)
}

func (eng *engine) SyncPush(sets []*pb.SetRequest, deletes []*pb.DeleteRequest) error {
	return eng.syncer.Push(sets, deletes)
}

func (eng *engine) Owner(key kv.Key) kv.NodeID {
	if eng.meshConfig.SingleNode {
		return eng.meshConfig.NodeID
	}
	return eng.mesh.Owner(key)
}

func (eng *engine) NodeID() kv.NodeID {
	return eng.meshConfig.NodeID
}

func (eng *engine) Addr() string {
	addr := eng.meshConfig.BindAddr
	if addr == "" {
		panic("dkv: bind address not configured")
	}
	return fmt.Sprintf("%s:%d", addr, eng.meshConfig.GrpcPort)
}

func (eng *engine) GossipAddr() string {
	addr := eng.meshConfig.BindAddr
	if addr == "" {
		panic("dkv: bind address not configured")
	}
	return fmt.Sprintf("%s:%d", addr, eng.meshConfig.BindPort)
}

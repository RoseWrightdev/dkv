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
	SyncPull(pullConfig *PullConfig) ([]*pb.SetRequest, []*pb.DeleteRequest, error)
	SyncPush(sets []*pb.SetRequest, deletes []*pb.DeleteRequest) error
	Addr() string
}

type engine struct {
	creds      credentials.TransportCredentials
	clock      Clock
	wal        Waler
	mesh       Mesher
	evt        Evictor
	cc         *ClientCache
	syncer     *Syncer
	pools      *pools
	hm         *shardedMap
	snp        *Snapshotter
	sip        *Gossip
	meshConfig MeshConfig
	startOnce  sync.Once
	stopOnce   sync.Once
}

// EngineConfig specifies the parameters required to initialize and run a dkv Engine.
type EngineConfig struct {
	evt            Evictor
	clock          Clock
	creds          credentials.TransportCredentials
	walPath        string
	snpPath        string
	meshConfig     MeshConfig
	walInterval    time.Duration
	snpInterval    time.Duration
	walSegments    int
	gossipInterval time.Duration
	walBufferSize  uint32
}

func newEngine(config EngineConfig) (Engine, error) {
	wal, err := newWal(config.walPath, config.walInterval, config.walBufferSize, config.walSegments)
	if err != nil {
		return nil, err
	}

	// todo: refactor toplevel engine pool
	eng := &engine{
		hm:         newShardedMap(),
		wal:        wal,
		clock:      config.clock,
		meshConfig: config.meshConfig,
		creds:      config.creds,
		cc:         newClientCache(config.creds),
		pools:      newPools(),
	}

	if err := eng.recover(config.snpPath); err != nil {
		slog.Error("Failed to recover database state", "error", err)
	}

	gossip := newGossip(eng.pools, eng.hm, eng.wal, eng.clock, eng.mesh, &eng.meshConfig)
	eng.sip = gossip

	snp, err := newSnapshotter(config.snpPath, config.snpInterval, wal, gossip.streamToEncoder)
	if err != nil {
		return nil, err
	}
	eng.snp = snp
	eng.evt = config.evt
	eng.evt.SetEvictCallback(eng.Evict)

	eng.mesh = &NopMesh{}
	if !config.meshConfig.SingleNode {
		mesh, err := newMesh(
			gossip,
			config.meshConfig,
		)
		if err != nil {
			return nil, err
		}
		eng.mesh = mesh
	}
	gossip.mesh = eng.mesh

	if !config.meshConfig.SingleNode {
		eng.syncer = newSyncer(&SyncerConfig{
			nodeID:     config.meshConfig.NodeID,
			gossip:     eng.sip,
			mesh:       eng.mesh,
			meshConfig: &eng.meshConfig,
			hm:         eng.hm,
			pools:      eng.pools,
			interval:   config.gossipInterval,
			creds:      config.creds,
		})
	}

	return eng, nil
}

// Start initializes background services.
func (eng *engine) Start() {
	eng.startOnce.Do(func() {
		eng.snp.start()
		eng.wal.start()
		eng.evt.start()
		if err := eng.mesh.start(); err != nil {
			panic(fmt.Sprintf("failed to start cluster service: %v", err))
		}
		if eng.syncer != nil {
			eng.syncer.start()
		}
	})
}

// Stop gracefully shuts down the engine and its background services.
func (eng *engine) Stop() {
	eng.stopOnce.Do(func() {
		if eng.syncer != nil {
			eng.syncer.stop()
		}
		eng.snp.stop()
		eng.wal.stop()
		eng.evt.stop()
		if err := eng.mesh.stop(); err != nil {
			panic(fmt.Sprintf("failed to stop cluster service: %v", err))
		}
		eng.cc.close()
	})
}

// Get retrieves the value associated with a key from the sharded map.
func (eng *engine) Get(key Key) ([]byte, bool) {
	hash := hashKey(hashFunc(key))
	iv, ok := eng.hm.Load(key, hash)
	if ok && !iv.Tombstone {
		eng.evt.publish(key, hash)
		return iv.Data, true
	} else if ok && iv.Tombstone {
		// We have a local tombstone. Do not proxy the read as the key is known to be deleted.
		return nil, false
	}

	// Gateway Proxy: If we don't have it locally, fetch it from an owner
	if !eng.meshConfig.SingleNode {
		// todo: refactor
		rf := eng.meshConfig.ReplicationFactor
		if rf <= 0 {
			rf = 1
		}
		owners := eng.mesh.GetOwners(key, rf)
		defer eng.mesh.PutOwners(owners)

		for _, owner := range owners {
			if owner == eng.meshConfig.NodeID {
				continue // We already checked local storage
			}

			addr := eng.mesh.AddressForNode(owner)
			if addr == "" {
				continue // Try next owner
			}

			// Proxy the read
			client, err := eng.cc.get(addr)
			if err != nil {
				continue // Try next owner
			}
			val, ok, err := client.Get(key)
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
	eng.evt.publish(key, hash)

	ts := eng.clock.Now()

	req := eng.pools.setRequests.Get().(*pb.SetRequest)
	req.Key = key
	req.Value = value
	req.Timestamp = ts
	req.NodeId = string(eng.meshConfig.NodeID)

	err := eng.wal.publish(key, hash, req)

	if err != nil {
		return err
	}

	eng.hm.Store(key, hash, Value{
		Data:      value,
		Timestamp: ts,
		NodeID:    string(eng.meshConfig.NodeID),
		Tombstone: false,
	})

	if !eng.meshConfig.SingleNode {
		// todo: refactor
		entry := eng.pools.walEntries.Get().(*pb.WalEntry)
		wrapper := eng.pools.walSetWrappers.Get().(*pb.WalEntry_Set)
		wrapper.Set = req
		entry.Entry = wrapper
		if data, err := proto.Marshal(entry); err == nil {
			eng.mesh.Broadcast(data)
		}
		entry.Entry = nil
		wrapper.Set = nil
		eng.pools.walSetWrappers.Put(wrapper)
		eng.pools.walEntries.Put(entry)
	}

	req.Reset()
	eng.pools.setRequests.Put(req)
	return nil
}

// Delete marks a key as deleted by publishing a tombstone to the WAL.
func (eng *engine) Delete(key Key) error {
	hash := hashFunc(key)
	eng.evt.publishDelete(key, hash)

	ts := eng.clock.Now()

	req := eng.pools.deleteRequests.Get().(*pb.DeleteRequest)
	req.Key = key
	req.Timestamp = ts
	req.NodeId = string(eng.meshConfig.NodeID)

	err := eng.wal.publish(key, hash, req)

	if err != nil {
		return err
	}
	eng.hm.Store(key, hash, Value{
		Timestamp: ts,
		NodeID:    string(eng.meshConfig.NodeID),
		Tombstone: true,
	})

	if !eng.meshConfig.SingleNode {
		// todo: refactor
		entry := eng.pools.walEntries.Get().(*pb.WalEntry)
		wrapper := eng.pools.walDeleteWrappers.Get().(*pb.WalEntry_Delete)
		wrapper.Delete = req
		entry.Entry = wrapper
		if data, err := proto.Marshal(entry); err == nil {
			eng.mesh.Broadcast(data)
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
		NodeID:    string(eng.meshConfig.NodeID),
		Tombstone: true,
	})

	if !eng.meshConfig.SingleNode {
		// todo: refactor
		entry := eng.pools.walEntries.Get().(*pb.WalEntry)
		wrapper := eng.pools.walDeleteWrappers.Get().(*pb.WalEntry_Delete)
		wrapper.Delete = req
		entry.Entry = wrapper
		if data, err := proto.Marshal(entry); err == nil {
			eng.mesh.Broadcast(data)
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
		slog.Info("Loaded state from snapshot", "path", snpPath, "keys", count)
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

func (eng *engine) SyncPull(pullConfig *PullConfig) ([]*pb.SetRequest, []*pb.DeleteRequest, error) {
	return eng.syncer.pull(pullConfig)
}

// used for testing
func (eng *engine) pullWithSyncer(pullConfig *PullConfig, syncer Syncer) ([]*pb.SetRequest, []*pb.DeleteRequest, error) {
	return syncer.pull(pullConfig)
}

func (eng *engine) SyncPush(sets []*pb.SetRequest, deletes []*pb.DeleteRequest) error {
	return eng.syncer.push(sets, deletes)
}

// used for testing
func (eng *engine) pushWithSyncer(sets []*pb.SetRequest, deletes []*pb.DeleteRequest, syncer Syncer) error {
	return syncer.push(sets, deletes)
}

func (eng *engine) Owner(key Key) NodeID {
	if eng.meshConfig.SingleNode {
		return eng.meshConfig.NodeID
	}
	return eng.mesh.Owner(key)
}

func (eng *engine) Addr() string {
	addr := eng.meshConfig.BindAddr
	if addr == "" {
		panic("dkv: bind address not configured")
	}
	return fmt.Sprintf("%s:%d", addr, eng.meshConfig.GrpcPort)
}

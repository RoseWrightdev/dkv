package dkv

import (
	"encoding/gob"
	"log/slog"
	"os"
	"sync"
	"time"

	pb "github.com/rosewrightdev/dkv/api"
)

type Engine interface {
	Get(key Key) (Value, bool)
	Set(key Key, value Value) error
	Delete(key Key) error
	Start()
	Stop()
	Snapshot() error
}

type engine struct {
	hm                *shardedMap
	wal               Waler
	sss               *SnapShotService
	evictionService   Evictor
	setRequestPool    sync.Pool
	snapshotEntryPool sync.Pool
}

type EngineConfig struct {
	walPath         string
	sssPath         string
	walSyncInterval time.Duration
	sssInterval     time.Duration
	walBufferSize   uint32
	walSegments     int
	evictionService Evictor
}

// snapshotEntry is used for streaming serialization
type snapshotEntry struct {
	Key   Key
	Value Value
}

func newEngine(config EngineConfig) (Engine, error) {
	wal, err := newWal(config.walPath, config.walSyncInterval, config.walBufferSize, config.walSegments)
	if err != nil {
		return nil, err
	}

	eng := &engine{
		hm:  newShardedMap(),
		wal: wal,
	}

	eng.setRequestPool = sync.Pool{
		New: func() any {
			return &pb.SetRequest{}
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

	return eng, nil
}

func (eng *engine) Start() {
	eng.sss.start()
	eng.wal.start()
	eng.evictionService.start()
}

func (eng *engine) Stop() {
	eng.sss.stop()
	eng.wal.stop()
	eng.evictionService.stop()
}

func (eng *engine) Get(key Key) (Value, bool) {
	hash := hashFunc(key)
	eng.evictionService.publish(key, hash)
	return eng.hm.Load(key, hash)
}

func (eng *engine) Set(key Key, value Value) error {
	hash := hashFunc(key)
	eng.evictionService.publish(key, hash)
	req := eng.setRequestPool.Get().(*pb.SetRequest)
	req.Key = key
	req.Value = value

	err := eng.wal.publish(key, hash, req)
	eng.setRequestPool.Put(req)

	if err != nil {
		return err
	}

	eng.hm.Store(key, hash, value)
	return nil
}

func (eng *engine) Delete(key Key) error {
	hash := hashFunc(key)
	eng.evictionService.publishDelete(key, hash)
	if err := eng.wal.publish(key, hash, &pb.DeleteRequest{Key: key}); err != nil {
		return err
	}
	eng.hm.Delete(key, hash)
	return nil
}

func (eng *engine) Evict(key Key) error {
	hash := hashFunc(key)
	if err := eng.wal.publish(key, hash, &pb.DeleteRequest{Key: key}); err != nil {
		return err
	}
	eng.hm.Delete(key, hash)
	return nil
}

func (eng *engine) Snapshot() error {
	return eng.sss.create()
}

func (eng *engine) streamToEncoder(enc *gob.Encoder) error {
	var err error
	eng.hm.Range(func(k, v any) bool {
		entry := eng.snapshotEntryPool.Get().(*snapshotEntry)
		entry.Key = k.(Key)
		entry.Value = v.(Value)
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
			eng.hm.Store(entry.Key, hashFunc(entry.Key), entry.Value)
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
		if v == nil {
			eng.hm.Delete(k, h)
		} else {
			eng.hm.Store(k, h, v)
		}
	}
	if len(updates) > 0 {
		slog.Info("Replayed updates from WAL", "count", len(updates))
	}

	return nil
}

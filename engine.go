package dkv

import (
	"encoding/gob"
	"log/slog"
	"os"
	"sync"
	"time"

	pb "github.com/rosewrightdev/dkv/api"
)

type Engine struct {
	hm              *shardedMap
	wal             *Wal
	sss             *SnapShotService
	evictionService Evictor
	setRequestPool  sync.Pool
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

func newEngine(config EngineConfig) (*Engine, error) {
	wal, err := newWal(config.walPath, config.walSyncInterval, config.walBufferSize, config.walSegments)
	if err != nil {
		return nil, err
	}

	eng := &Engine{
		hm:  newShardedMap(),
		wal: wal,
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
	eng.setRequestPool = sync.Pool{
		New: func() any {
			return &pb.SetRequest{}
		},
	}

	return eng, nil
}

func (eng *Engine) Start() {
	eng.sss.start()
	eng.wal.start()
	eng.evictionService.start()
}

func (eng *Engine) Stop() {
	eng.sss.stop()
	eng.wal.stop()
	eng.evictionService.stop()
}

func (eng *Engine) Get(key Key) (Value, bool) {
	eng.evictionService.publish(key)
	shard := eng.hm.getShard(key)
	shard.mu.RLock()
	val, ok := shard.m[key]
	shard.mu.RUnlock()
	return val, ok
}

func (eng *Engine) Set(key Key, value Value) error {
	eng.evictionService.publish(key)
	req := eng.setRequestPool.Get().(*pb.SetRequest)
	req.Key = key
	req.Value = value
	
	err := eng.wal.publish(key, req)
	eng.setRequestPool.Put(req)
	
	if err != nil {
		return err
	}
	
	shard := eng.hm.getShard(key)
	shard.mu.Lock()
	shard.m[key] = value
	shard.mu.Unlock()
	return nil
}

func (eng *Engine) Delete(key Key) error {
	eng.evictionService.publishDelete(key)
	if err := eng.wal.publish(key, &pb.DeleteRequest{Key: key}); err != nil {
		return err
	}
	shard := eng.hm.getShard(key)
	shard.mu.Lock()
	delete(shard.m, key)
	shard.mu.Unlock()
	return nil
}

func (eng *Engine) Evict(key Key) error {
	if err := eng.wal.publish(key, &pb.DeleteRequest{Key: key}); err != nil {
		return err
	}
	eng.hm.Delete(key)
	return nil
}

func (eng *Engine) Snapshot() error {
	return eng.sss.create()
}

func (eng *Engine) streamToEncoder(enc *gob.Encoder) error {
	var err error
	eng.hm.Range(func(k, v any) bool {
		entry := snapshotEntry{Key: k.(Key), Value: v.(Value)}
		if e := enc.Encode(entry); e != nil {
			err = e
			return false
		}
		return true
	})
	return err
}

func (eng *Engine) recover(sssPath string) error {
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
			eng.hm.Store(entry.Key, entry.Value)
			count++
		}
		slog.Info("Loaded state from snapshot", "path", sssPath, "keys", count)
	}

	updates, err := eng.wal.replay()
	if err != nil {
		return err
	}
	for k, v := range updates {
		if v == nil {
			eng.hm.Delete(k)
		} else {
			eng.hm.Store(k, v)
		}
	}
	if len(updates) > 0 {
		slog.Info("Replayed updates from WAL", "count", len(updates))
	}

	return nil
}

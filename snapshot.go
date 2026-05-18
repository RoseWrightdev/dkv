package dkv

import (
	"context"
	"encoding/gob"
	"io"
	"log/slog"
	"os"
	"sync"
	"time"
)

// Snapshoter manages the background persistence of the engine state to disk.
type Snapshoter struct {
	ctx         context.Context
	cancel      context.CancelFunc
	ch          chan struct{}
	wg          sync.WaitGroup
	path        string
	interval    time.Duration
	wal         Waler
	encCallBack func(*gob.Encoder) error
}

type snapshotEntry struct {
	Key       Key
	Data      []byte
	Timestamp int64
	Tombstone bool
}

func newSnapshoter(path string, interval time.Duration, wal Waler, encCallBack func(*gob.Encoder) error) (*Snapshoter, error) {
	ctx, cancel := context.WithCancel(context.Background())

	ch := make(chan struct{}, 1)
	snp := Snapshoter{
		ctx:         ctx,
		cancel:      cancel,
		ch:          ch,
		path:        path,
		interval:    interval,
		wal:         wal,
		encCallBack: encCallBack,
	}

	return &snp, nil
}

// start begins the periodic snapshotting loop.
func (snp *Snapshoter) start() {
	snp.wg.Add(2)
	go snp.producer(snp.ctx)
	go snp.consumer(snp.ctx)
}

// stop gracefully shuts down the snapshotting service.
func (snp *Snapshoter) stop() {
	snp.cancel()
	snp.wg.Wait()
}

func (snp *Snapshoter) producer(ctx context.Context) {
	defer snp.wg.Done()
	ticker := time.NewTicker(snp.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			snp.queueSnapShot()
		}
	}
}

func (snp *Snapshoter) consumer(ctx context.Context) {
	defer snp.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-snp.ch:
			if !ok {
				return
			}
			if err := snp.create(); err != nil {
				slog.Error("Failed to create snapshot", "error", err)
			} else {
				slog.Info("Database snapshot created.")
			}
		}
	}
}

func (snp *Snapshoter) queueSnapShot() {
	select {
	case snp.ch <- struct{}{}:
	default:
		// Snapshot already queued, skip
	}
}

func (snp *Snapshoter) create() error {
	offsets, err := snp.wal.prepareSnapshot()
	if err != nil {
		return err
	}

	tmpPath := snp.path + ".tmp"
	// #nosec G304
	file, err := os.Create(tmpPath)
	if err != nil {
		return err
	}
	defer func() {
		_ = file.Close()
	}()

	// Stream the data directly from the engine to the encoder
	encoder := gob.NewEncoder(file)
	if err := snp.encCallBack(encoder); err != nil {
		return err
	}

	err = file.Sync()
	if err != nil {
		return err
	}
	err = file.Close()
	if err != nil {
		return err
	}

	if err := os.Rename(tmpPath, snp.path); err != nil {
		return err
	}

	if err := snp.wal.clear(offsets); err != nil {
		return err
	}

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

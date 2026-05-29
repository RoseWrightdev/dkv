package dkv

import (
	"context"
	"encoding/gob"
	"log/slog"
	"os"
	"sync"
	"time"
	"github.com/rosewrightdev/dkv/kv"
)

// Snapshotter manages the background persistence of the engine state to disk.
type Snapshotter struct {
	ctx         context.Context
	wal         Waler
	cancel      context.CancelFunc
	ch          chan struct{}
	encCallBack func(*gob.Encoder) error
	path        string
	wg          sync.WaitGroup
	interval    time.Duration
}

type snapshotEntry struct {
	Key       kv.Key
	Data      []byte
	Timestamp int64
	Tombstone bool
}

func newSnapshotter(path string, interval time.Duration, wal Waler, encCallBack func(*gob.Encoder) error) (*Snapshotter, error) {
	ctx, cancel := context.WithCancel(context.Background())

	ch := make(chan struct{}, 1)
	snp := Snapshotter{
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
func (snp *Snapshotter) start() {
	snp.wg.Add(2)
	go snp.producer(snp.ctx)
	go snp.consumer(snp.ctx)
}

// stop gracefully shuts down the snapshotting service.
func (snp *Snapshotter) stop() {
	snp.cancel()
	snp.wg.Wait()
}

func (snp *Snapshotter) producer(ctx context.Context) {
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

func (snp *Snapshotter) consumer(ctx context.Context) {
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

func (snp *Snapshotter) queueSnapShot() {
	select {
	case snp.ch <- struct{}{}:
	default:
		// Snapshot already queued, skip
	}
}

func (snp *Snapshotter) create() error {
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

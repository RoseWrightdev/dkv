// Package snap manages local state snapshotting and WAL space reclamation.
package snap

import (
	"context"
	"encoding/gob"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/rosewrightdev/oryx/core/wal"
	"github.com/rosewrightdev/oryx/kv"
)

// Snapshotter manages the background persistence of the engine state to disk.
type Snapshotter struct {
	ctx         context.Context
	wal         wal.Waler
	cancel      context.CancelFunc
	ch          chan struct{}
	encCallBack func(*gob.Encoder) error
	path        string
	wg          sync.WaitGroup
	startOnce   sync.Once
	Interval    time.Duration
}

// SnapshotEntry represents a single key-value state entry serialized inside a snapshot.
type SnapshotEntry struct {
	Key       kv.Key
	Data      []byte
	Timestamp int64
	NodeID    kv.NodeID
	Tombstone bool
}

// NewSnapshotter initializes a new Snapshotter instance.
func NewSnapshotter(path string, interval time.Duration, wal wal.Waler, encCallBack func(*gob.Encoder) error) (*Snapshotter, error) {
	ctx, cancel := context.WithCancel(context.Background())

	ch := make(chan struct{}, 1)
	snp := Snapshotter{
		ctx:         ctx,
		cancel:      cancel,
		ch:          ch,
		path:        path,
		Interval:    interval,
		wal:         wal,
		encCallBack: encCallBack,
	}

	return &snp, nil
}

// Start begins the periodic snapshotting loop.
// It is idempotent: subsequent calls after the first are no-ops.
func (snp *Snapshotter) Start() {
	snp.startOnce.Do(func() {
		snp.wg.Add(2)
		go snp.producer(snp.ctx)
		go snp.consumer(snp.ctx)
	})
}

// Stop gracefully shuts down the snapshotting service.
func (snp *Snapshotter) Stop() {
	snp.cancel()
	snp.wg.Wait()
}

func (snp *Snapshotter) producer(ctx context.Context) {
	defer snp.wg.Done()
	ticker := time.NewTicker(snp.Interval)
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
			if err := snp.Create(); err != nil {
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

// Create triggers a manual state snapshot and purges processed WAL offsets.
func (snp *Snapshotter) Create() error {
	offsets, err := snp.wal.PrepareSnapshot()
	if err != nil {
		return err
	}

	tmpPath := snp.path + ".tmp"
	// #nosec G304
	file, err := os.Create(tmpPath)
	if err != nil {
		return err
	}

	// Track success so the deferred cleanup only removes tmpPath on failure.
	var success bool
	defer func() {
		_ = file.Close()
		if !success {
			_ = os.Remove(tmpPath)
		}
	}()

	// Stream the data directly from the engine to the encoder
	encoder := gob.NewEncoder(file)
	if err := snp.encCallBack(encoder); err != nil {
		return err
	}

	if err := file.Sync(); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}

	if err := os.Rename(tmpPath, snp.path); err != nil {
		return err
	}

	success = true

	if err := snp.wal.Clear(offsets); err != nil {
		return err
	}

	return nil
}

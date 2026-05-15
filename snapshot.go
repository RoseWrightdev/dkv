package dkv

import (
	"context"
	"encoding/gob"
	"log/slog"
	"os"
	"sync"
	"time"
)

type SnapShotService struct {
	ctx         context.Context
	cancel      context.CancelFunc
	ch          chan struct{}
	wg          sync.WaitGroup
	file        *os.File
	path        string
	interval    time.Duration
	wal         Waler
	encCallBack func(*gob.Encoder) error
}

func newSnapshotService(path string, interval time.Duration, wal Waler, encCallBack func(*gob.Encoder) error) (*SnapShotService, error) {
	ctx, cancel := context.WithCancel(context.Background())

	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		cancel()
		return nil, err
	}

	ch := make(chan struct{}, 1)
	sss := &SnapShotService{
		ctx:         ctx,
		cancel:      cancel,
		ch:          ch,
		file:        file,
		path:        path,
		interval:    interval,
		wal:         wal,
		encCallBack: encCallBack,
	}

	return sss, nil
}

func (sss *SnapShotService) start() {
	sss.wg.Add(2)
	go sss.producer(sss.ctx)
	go sss.consumer(sss.ctx)
}

func (sss *SnapShotService) stop() {
	sss.cancel()
	sss.wg.Wait()
	if sss.file != nil {
		sss.file.Close()
	}
}

func (sss *SnapShotService) producer(ctx context.Context) {
	defer sss.wg.Done()
	ticker := time.NewTicker(sss.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sss.queueSnapShot()
		}
	}
}

func (sss *SnapShotService) consumer(ctx context.Context) {
	defer sss.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-sss.ch:
			if !ok {
				return
			}
			if err := sss.create(); err != nil {
				slog.Error("Failed to create snapshot", "error", err)
			} else {
				slog.Info("Database snapshot created.")
			}
		}
	}
}

func (sss *SnapShotService) queueSnapShot() {
	select {
	case sss.ch <- struct{}{}:
	default:
		// Snapshot already queued, skip
	}
}

func (sss *SnapShotService) create() error {
	tmpPath := sss.path + ".tmp"
	file, err := os.Create(tmpPath)
	if err != nil {
		return err
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {

		}
	}(file)

	// Stream the data directly from the engine to the encoder
	encoder := gob.NewEncoder(file)
	if err := sss.encCallBack(encoder); err != nil {
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

	if err := os.Rename(tmpPath, sss.path); err != nil {
		return err
	}

	newFile, err := os.OpenFile(sss.path, os.O_CREATE|os.O_RDWR, 0644)
	if err == nil {
		if sss.file != nil {
			err := sss.file.Close()
			if err != nil {
				return err
			}
		}
		sss.file = newFile
	}

	if err := sss.wal.clear(); err != nil {
		return err
	}

	return nil
}

package core

import (
	"context"
	"encoding/json"
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
	engCallBack func() *map[Key]Value
}

func newSnapshotService(path string, interval time.Duration, wal Waler, engCallBack func() *map[Key]Value) (*SnapShotService, error) {
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
		engCallBack: engCallBack,
	}

	return sss, nil
}

func (sss *SnapShotService) Start() {
	sss.wg.Add(2)
	go sss.producer(sss.ctx)
	go sss.consumer(sss.ctx)
}

func (sss *SnapShotService) Stop() {
	sss.cancel()
	sss.wg.Wait()
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
			if err := sss.createNewSnapShot(); err != nil {
				slog.Error("Failed to create snapshot", "error", err)
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

func (sss *SnapShotService) createNewSnapShot() error {
	state := sss.engCallBack()

	data, err := json.Marshal(state)
	if err != nil {
		return err
	}

	tmpPath := sss.path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return err
	}

	if err := os.Rename(tmpPath, sss.path); err != nil {
		return err
	}

	newFile, err := os.OpenFile(sss.path, os.O_CREATE|os.O_RDWR, 0644)
	if err == nil {
		if sss.file != nil {
			sss.file.Close()
		}
		sss.file = newFile
	}

	if err := sss.wal.clear(); err != nil {
		return err
	}

	return nil
}

package core

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"time"
)

type SnapShotService struct {
	ctx         context.Context
	cancel      context.CancelFunc
	ch          chan struct{}
	file        *os.File
	path        string
	interval    time.Duration
	wal         Waler
	engCallBack func() *map[Key]Value
}

func NewSnapshotService(path string, interval time.Duration, wal Waler, engCallBack func() *map[Key]Value) (*SnapShotService, error) {
	ctx, cancel := context.WithCancel(context.Background())

	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		cancel()
		return nil, err
	}

	ch := make(chan struct{}, 1)
	sss := &SnapShotService{
		ctx,
		cancel,
		ch,
		file,
		path,
		interval,
		wal,
		engCallBack,
	}

	return sss, nil
}

func (sss *SnapShotService) Start() {
	go sss.producer(sss.ctx)
	sss.consumer(sss.ctx)
}

func (sss *SnapShotService) Stop() {
	sss.cancel()
	close(sss.ch)
}

func (sss *SnapShotService) producer(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			time.Sleep(sss.interval)
			sss.queueSnapShot()
		}
	}
}

func (sss *SnapShotService) consumer(ctx context.Context) {
	for range sss.ch {
		select {
		case <-ctx.Done():
			return
		default:
			if err := sss.createNewSnapShot(); err != nil {
				slog.Error("Failed to create snapshot", "error", err)
			}
		}
	}
}

func (sss *SnapShotService) queueSnapShot() {
	sss.ch <- struct{}{}
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
		sss.file.Close()
		sss.file = newFile
	}

	if err := sss.wal.clear(); err != nil {
		return err
	}

	return nil
}

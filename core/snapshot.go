package core

import (
	"context"
	"log/slog"
	"os"
	"time"
)

type SnapShotService struct {
	ctx      context.Context
	cancel   context.CancelFunc
	ch       chan struct{}
	file     *os.File
	interval time.Duration
	wal      *Wal
}

type SnapShotData = []byte

func NewSnapshotService(snapShotPath string, interval time.Duration, wal *Wal) (*SnapShotService, error) {
	ctx, cancel := context.WithCancel(context.Background())

	file, err := os.OpenFile(snapShotPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
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
		interval,
		wal,
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
			err := sss.createNewSnapShot()
			slog.Error(err.Error())
		}
	}
}

func (sss *SnapShotService) queueSnapShot() {
	sss.ch <- struct{}{}
}

// Creates a snapshot of the database. If no existing snapshot exists, it generates one from the WAL.
// If there is an existing snapshot of the database, it uses the snapshot as a starting point.
// todo: impl
func (sss *SnapShotService) createNewSnapShot() error {
	state, err := sss.wal.Replay()
	if err != nil {
		return err
	}

	return sss.wal.flush()
}

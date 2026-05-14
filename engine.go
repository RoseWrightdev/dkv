package dkv

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	pb "github.com/rosewrightdev/dkv/api"
)

type Key = string
type Value = []byte

type Engine struct {
	hm  *sync.Map
	wal *Wal
	sss *SnapShotService
}

func newEngine(walPath string, sssPath string, walSyncInterval time.Duration, sssInterval time.Duration) (*Engine, error) {
	wal, err := newWal(walPath, walSyncInterval)
	if err != nil {
		return nil, err
	}

	eng := &Engine{
		hm:  &sync.Map{},
		wal: wal,
	}

	if err := eng.recover(sssPath); err != nil {
		slog.Error("Failed to recover database state", "error", err)
	}

	sss, err := newSnapshotService(sssPath, sssInterval, wal, eng.toMap)
	if err != nil {
		return nil, err
	}
	eng.sss = sss

	return eng, nil
}

func (eng *Engine) Start() {
	eng.sss.start()
	eng.wal.start()
}

func (eng *Engine) Stop() {
	eng.sss.stop()
	eng.wal.stop()
}

func (eng *Engine) Get(key Key) (Value, bool) {
	val, ok := eng.hm.Load(key)
	if !ok {
		return nil, false
	}
	return val.(Value), ok
}

func (eng *Engine) Set(key Key, value Value) error {
	if err := eng.wal.publish(&pb.SetRequest{Key: key, Value: value}); err != nil {
		return err
	}
	eng.hm.Store(key, value)
	return nil
}

func (eng *Engine) Delete(key Key) error {
	if err := eng.wal.publish(&pb.DeleteRequest{Key: key}); err != nil {
		return err
	}
	eng.hm.Delete(key)
	return nil
}

func (eng *Engine) toMap() map[Key]Value {
	hm := make(map[Key]Value)
	eng.hm.Range(func(key any, val any) bool {
		hm[key.(Key)] = val.(Value)
		return true
	})
	return hm
}

func (eng *Engine) recover(sssPath string) error {
	if info, err := os.Stat(sssPath); err == nil && info.Size() > 0 {
		data, err := os.ReadFile(sssPath)
		if err != nil {
			return err
		}
		var state map[Key]Value
		if err := json.Unmarshal(data, &state); err != nil {
			return err
		}
		for k, v := range state {
			eng.hm.Store(k, v)
		}
		slog.Info("Loaded state from snapshot", "path", sssPath, "keys", len(state))
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

type EngineBuilder struct {
	walPath         string
	sssPath         string
	walSyncInterval time.Duration
	sssInterval     time.Duration
}

func NewEngineBuilder() *EngineBuilder {
	return &EngineBuilder{}
}

func (eb *EngineBuilder) SetWalPath(path string) {
	eb.walPath = path
}

func (eb *EngineBuilder) SetSssPath(path string) {
	eb.sssPath = path
}

func (eb *EngineBuilder) SetSssInterval(interval time.Duration) {
	eb.sssInterval = interval
}

func (eb *EngineBuilder) SetWalSyncInterval(interval time.Duration) {
	eb.walSyncInterval = interval
}

func (eb *EngineBuilder) GetEngine() (*Engine, error) {
	if isUnit(eb.walPath) {
		return nil, fmt.Errorf("required eb.walPath is unset cogfigure eb.walPath with SetWalPath(path string)")
	}

	if isUnit(eb.sssPath) {
		return nil, fmt.Errorf("required eb.sssPath is unset cogfigure eb.sssPath with SetSssPath(path string)")
	}

	if isUnit(eb.walSyncInterval) {
		return nil, fmt.Errorf("required eb.walSyncInterval is unset cogfigure eb.walSyncInterval with SetWalSyncInterval(interval time.Duration)")
	}

	if isUnit(eb.sssInterval) {
		return nil, fmt.Errorf("required eb.sssInterval is unset cogfigure eb.sssInterval with SetSssInterval(interval time.Duration)")
	}

	return newEngine(eb.walPath, eb.sssPath, eb.walSyncInterval, eb.sssInterval)
}

func isUnit[T comparable](val T) bool {
	var zero T
	return zero == val
}

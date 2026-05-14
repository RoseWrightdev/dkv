package core

import (
	"sync"
	"time"
)

type Key = string
type Value = []byte

type Engine struct {
	hm  *sync.Map
	Wal *Wal
	sss *SnapShotService
}

func newEngine(walPath string, sssPath string, sssInterval time.Duration) (*Engine, error) {
	wal, err := newWal(walPath)
	if err != nil {
		return nil, err
	}
	wal.Start()

	eng := &Engine{
		hm:  &sync.Map{},
		Wal: wal,
	}

	sss, err := newSnapshotService(sssPath, sssInterval, wal, eng.toMap)
	if err != nil {
		return nil, err
	}
	eng.sss = sss
	eng.sss.Start()

	return eng, nil
}

func (eng *Engine) Stop() {
	eng.sss.Stop()
	eng.Wal.Stop()
}

func (eng *Engine) Get(key Key) (Value, bool) {
	val, ok := eng.hm.Load(key)
	return val.(Value), ok
}

func (eng *Engine) Set(key Key, value Value) {
	eng.hm.Store(key, value)
}

func (eng *Engine) Delete(key Key) {
	eng.hm.Delete(key)
}

func (eng *Engine) Exists(key Key) bool {
	_, ok := eng.hm.Load(key)
	return ok
}

func (eng *Engine) toMap() *map[Key]Value {
	hm := make(map[Key]Value)
	eng.hm.Range(func(key any, val any) bool {
		hm[key.(Key)] = val.(Value)
		return true
	})
	return &hm
}

type EngineBuilder struct {
	walPath     string
	sssPath     string
	sssInterval time.Duration
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

func (eb *EngineBuilder) GetEngine() (*Engine, error) {
	return newEngine(eb.walPath, eb.sssPath, eb.sssInterval)
}

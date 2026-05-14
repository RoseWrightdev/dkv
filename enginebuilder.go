package dkv

import (
	"fmt"
	"time"
)

type EngineBuilder struct {
	walPath         string
	sssPath         string
	walSyncInterval time.Duration
	sssInterval     time.Duration
	walBufferSize   uint32
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

func (eb *EngineBuilder) SetWalBufferSize(size uint32) {
	eb.walBufferSize = size
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

	if isUnit(eb.walBufferSize) {
		return nil, fmt.Errorf("required eb.walBufferSize is unset cogfigure eb.walBufferSize with SetWalBufferSize(size uint32)")
	}

	config := EngineConfig{
		walPath:         eb.walPath,
		sssPath:         eb.sssPath,
		walSyncInterval: eb.walSyncInterval,
		sssInterval:     eb.sssInterval,
		walBufferSize:   eb.walBufferSize,
	}

	return newEngine(config)
}

func isUnit[T comparable](val T) bool {
	var zero T
	return zero == val
}

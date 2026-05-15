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
	walSegments     int
	evictionService Evictor
}

func NewEngineBuilder() *EngineBuilder {
	return &EngineBuilder{}
}

func (eb *EngineBuilder) SetWalPath(path string) *EngineBuilder {
	eb.walPath = path
	return eb
}

func (eb *EngineBuilder) SetSssPath(path string) *EngineBuilder {
	eb.sssPath = path
	return eb
}

func (eb *EngineBuilder) SetSssInterval(interval time.Duration) *EngineBuilder {
	eb.sssInterval = interval
	return eb
}

func (eb *EngineBuilder) SetWalSyncInterval(interval time.Duration) *EngineBuilder  {
	eb.walSyncInterval = interval
	return eb
}

func (eb *EngineBuilder) SetWalBufferSize(size uint32) *EngineBuilder {
	eb.walBufferSize = size
	return eb
}

func (eb *EngineBuilder) SetWalSegments(count int) *EngineBuilder {
	eb.walSegments = count
	return eb
}

func (eb *EngineBuilder) SetEvictionService(evictor Evictor) *EngineBuilder {
	eb.evictionService = evictor
	return eb
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

	if isUnit(eb.walSegments) {
		return nil, fmt.Errorf("required eb.walSegments is unset configure eb.walSegments with SetWalSegments(count int)")
	}

	if eb.evictionService == nil {
		return nil, fmt.Errorf("required eb.evictionService is unset configure eb.evictionService with SetEvictionService(evictor Evictor)")
	}

	config := EngineConfig{
		walPath:         eb.walPath,
		sssPath:         eb.sssPath,
		walSyncInterval: eb.walSyncInterval,
		sssInterval:     eb.sssInterval,
		walBufferSize:   eb.walBufferSize,
		walSegments:     eb.walSegments,
		evictionService: eb.evictionService,
	}

	return newEngine(config)
}

func isUnit[T comparable](val T) bool {
	var zero T
	return zero == val
}

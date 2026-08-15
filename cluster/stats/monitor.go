// Package stats provides capacity monitoring and dynamic load shedding triggers.
package stats

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// Monitor tracks resource/capacity occupancy metrics and dynamically sheds
// load by lowering traffic weights under high pressure.
type Monitor struct {
	ctx       context.Context
	cancel    context.CancelFunc
	ticker    *time.Ticker
	occupancy func() float64
	setWeight func(int)
	weight    int
	startOnce sync.Once
	wg        sync.WaitGroup
}

// NewMonitor initializes a new load Monitor.
func NewMonitor(occupancy func() float64, setWeight func(int)) *Monitor {
	return &Monitor{
		occupancy: occupancy,
		setWeight: setWeight,
		weight:    128,
	}
}

// Start spawns the background resource monitoring loop.
// It is idempotent: subsequent calls after the first are no-ops.
func (m *Monitor) Start() {
	m.startOnce.Do(func() {
		m.ctx, m.cancel = context.WithCancel(context.Background())
		m.ticker = time.NewTicker(3 * time.Second)
		m.wg.Add(1)
		go m.run()
	})
}

// Stop gracefully cancels the background monitoring loop and waits for it
// to exit before returning. Safe to call concurrently with database teardown.
func (m *Monitor) Stop() {
	if m.cancel != nil {
		m.cancel()
		m.wg.Wait()
	}
}

func (m *Monitor) run() {
	defer m.wg.Done()
	defer m.ticker.Stop()
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-m.ticker.C:
			ratio := m.occupancy()
			newWeight := 128
			if ratio >= 0.90 {
				// Shed load severely by lowering weight to 16
				newWeight = 16
			} else if ratio >= 0.80 {
				// Reduce weight partially to 64
				newWeight = 64
			}

			if newWeight != m.weight {
				slog.Info("Stats Monitor: capacity weight changed dynamically", "occupancy", ratio, "oldWeight", m.weight, "newWeight", newWeight)
				m.weight = newWeight
				m.setWeight(newWeight)
			}
		}
	}
}

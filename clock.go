package dkv

import (
	"sync/atomic"
	"time"
)

// Clock defines an interface for providing timestamps.
type Clock interface {
	Now() int64
}

// RealClock provides wall-clock time in nanoseconds.
type RealClock struct{}

func (c *RealClock) Now() int64 {
	return time.Now().UnixNano()
}

// HybridLogicalClock (placeholder for future implementation if needed)
// For now, we use a simple Lamport-ish clock or just Wall clock.
// Given LWW, Wall clock is standard but needs monotonicity.

// MonotonicClock ensures that timestamps never go backwards on a single node.
type MonotonicClock struct {
	lastTimestamp atomic.Int64
}

func (c *MonotonicClock) Now() int64 {
	for {
		now := time.Now().UnixNano()
		last := c.lastTimestamp.Load()
		if now <= last {
			now = last + 1
		}
		if c.lastTimestamp.CompareAndSwap(last, now) {
			return now
		}
	}
}

// Package clock provides a Hybrid Logical Clock implementation.
package clock

import (
	"log/slog"
	"math"
	"sync/atomic"
	"time"
)

// Clocker defines an interface for providing distributed-safe timestamps.
type Clocker interface {
	// Now returns a Hybrid Logical Clock (HLC) timestamp.
	Now() int64
	// Update adjusts the local clock based on a remote timestamp.
	Update(remote int64)
}

const (
	// logicalBits defines how many bits are reserved for the logical counter.
	// 16 bits allow for 65,536 events per millisecond.
	logicalBits = 16
	// logicalMask is used to extract the logical counter from a 64-bit timestamp.
	logicalMask = (1 << logicalBits) - 1
	// maxPhysical bounds the physical component so a corrupted system clock
	// saturates instead of overflowing c.state past math.MaxInt64 (#104).
	maxPhysical = uint64(math.MaxInt64) >> logicalBits
)

// Clock implements a Hybrid Logical Clock.
type Clock struct {
	state            atomic.Uint64
	saturationLogged atomic.Bool
}

func clampPhysical(p uint64) uint64 {
	if p > maxPhysical {
		return maxPhysical
	}
	return p
}

// NewClock initializes a new Hybrid Logical Clock.
func NewClock() *Clock {
	return &Clock{}
}

// Now returns the current HLC timestamp and advances the local state.
func (c *Clock) Now() int64 {
	// Sample physical time once before the CAS loop. Retries reuse this value
	// so we avoid a time.Now() syscall on every failed CAS iteration.
	now := clampPhysical(uint64(max(time.Now().UnixMilli(), 0)))

	for {
		old := c.state.Load()
		oldPhysical := old >> logicalBits
		oldLogical := old & logicalMask

		// Already at the ceiling: physical time can never catch up, so
		// ratcheting further would spin forever. Report once and return (#104).
		if oldPhysical >= maxPhysical {
			if c.saturationLogged.CompareAndSwap(false, true) {
				slog.Error("HLC clock saturated at the maximum representable value; check the system clock", "state", old)
			}
			return math.MaxInt64
		}

		// Overflow: logical counter exhausted and physical time hasn't advanced.
		// Sleep 1ms to let the wall clock tick, then re-sample.
		if now <= oldPhysical && oldLogical >= logicalMask {
			time.Sleep(1 * time.Millisecond)
			now = clampPhysical(uint64(max(time.Now().UnixMilli(), 0)))
			continue
		}

		var newPhysical, newLogical uint64
		if now > oldPhysical {
			newPhysical = now
			newLogical = 0
		} else {
			newPhysical = oldPhysical
			newLogical = oldLogical + 1
		}

		newVal := (newPhysical << logicalBits) | (newLogical & logicalMask)
		if c.state.CompareAndSwap(old, newVal) {
			return int64(newVal)
		}
		// CAS lost the race — re-sample time and retry. Physical time may have
		// advanced since our initial sample, keeping timestamps accurate under
		// high contention without paying a syscall on every iteration.
		now = clampPhysical(uint64(max(time.Now().UnixMilli(), 0)))
	}
}

// Update incorporates a remote timestamp to maintain causality.
// Should be called on every incoming message containing a timestamp.
func (c *Clock) Update(remote int64) {
	if remote < 0 {
		return // Ignore invalid/negative remote timestamps
	}

	// #nosec G115
	remoteU := uint64(remote)
	remotePhysical := remoteU >> logicalBits
	remoteLogical := remoteU & logicalMask

	// remotePhysical can never exceed maxPhysical since remote is non-negative.
	now := clampPhysical(uint64(max(time.Now().UnixMilli(), 0)))
	if remotePhysical > now+5000 {
		return // Ignore excessively drifted remote timestamps to prevent clock poisoning
	}

	for {
		old := c.state.Load()
		oldPhysical := old >> logicalBits
		oldLogical := old & logicalMask

		// See Now(): once already at the ceiling, physical time can never
		// catch up further, so stop instead of spinning forever (#104).
		if oldPhysical >= maxPhysical {
			if c.saturationLogged.CompareAndSwap(false, true) {
				slog.Error("HLC clock saturated at the maximum representable value; check the system clock", "state", old)
			}
			return
		}

		chosenPhysical := max(oldPhysical, max(remotePhysical, now))

		// Overflow: logical counter would exceed mask — sleep and re-sample.
		if chosenPhysical == oldPhysical {
			var expectedLogical uint64
			if chosenPhysical == remotePhysical {
				expectedLogical = max(oldLogical, remoteLogical) + 1
			} else {
				expectedLogical = oldLogical + 1
			}
			if expectedLogical > logicalMask {
				time.Sleep(1 * time.Millisecond)
				now = clampPhysical(uint64(max(time.Now().UnixMilli(), 0)))
				continue
			}
		} else if chosenPhysical == remotePhysical && remoteLogical+1 > logicalMask {
			time.Sleep(1 * time.Millisecond)
			now = clampPhysical(uint64(max(time.Now().UnixMilli(), 0)))
			continue
		}

		var newPhysical, newLogical uint64
		switch chosenPhysical {
		case oldPhysical:
			if chosenPhysical == remotePhysical {
				newPhysical = oldPhysical
				newLogical = max(oldLogical, remoteLogical) + 1
			} else {
				newPhysical = oldPhysical
				newLogical = oldLogical + 1
			}
		case remotePhysical:
			newPhysical = remotePhysical
			newLogical = remoteLogical + 1
		default:
			newPhysical = chosenPhysical
			newLogical = 0
		}

		newVal := (newPhysical << logicalBits) | (newLogical & logicalMask)
		if c.state.CompareAndSwap(old, newVal) {
			return
		}
		// CAS failed — re-sample time and retry.
		now = clampPhysical(uint64(max(time.Now().UnixMilli(), 0)))
	}
}

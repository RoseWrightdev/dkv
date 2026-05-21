package dkv

import (
	"math"
	"sync/atomic"
	"time"
)

// Clock defines an interface for providing distributed-safe timestamps.
// Clock defines the interface for distributed timestamps.
type Clock interface {
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
)

// HLC implements a Hybrid Logical Clock.
type HLC struct {
	state atomic.Uint64
}

// NewHLC initializes a new Hybrid Logical Clock.
func NewHLC() *HLC {
	return &HLC{}
}

// Now returns the current HLC timestamp and advances the local state.
func (c *HLC) Now() int64 {
	// Sample physical time once before the CAS loop. Retries reuse this value
	// so we avoid a time.Now() syscall on every failed CAS iteration.
	now := uint64(max(time.Now().UnixMilli(), 0))

	for {
		old := c.state.Load()
		oldPhysical := old >> logicalBits
		oldLogical := old & logicalMask

		// Overflow: logical counter exhausted and physical time hasn't advanced.
		// Sleep 1ms to let the wall clock tick, then re-sample.
		if now <= oldPhysical && oldLogical >= logicalMask {
			time.Sleep(1 * time.Millisecond)
			now = uint64(max(time.Now().UnixMilli(), 0))
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
			if newVal <= math.MaxInt64 {
				return int64(newVal)
			}
			return math.MaxInt64
		}
		// CAS lost the race — re-sample time and retry. Physical time may have
		// advanced since our initial sample, keeping timestamps accurate under
		// high contention without paying a syscall on every iteration.
		now = uint64(max(time.Now().UnixMilli(), 0))
	}
}

// Update incorporates a remote timestamp to maintain causality.
// Should be called on every incoming message containing a timestamp.
func (c *HLC) Update(remote int64) {
	if remote < 0 {
		return // Ignore invalid/negative remote timestamps
	}

	// #nosec G115
	remoteU := uint64(remote)
	remotePhysical := remoteU >> logicalBits
	remoteLogical := remoteU & logicalMask

	// Sample once for the drift guard and as the initial loop value.
	// Re-sample only after overflow sleep or a failed CAS.
	now := uint64(max(time.Now().UnixMilli(), 0))
	if remotePhysical > now+5000 {
		return // Ignore excessively drifted remote timestamps to prevent clock poisoning
	}

	for {
		old := c.state.Load()
		oldPhysical := old >> logicalBits
		oldLogical := old & logicalMask

		maxPhysical := now
		if remotePhysical > maxPhysical {
			maxPhysical = remotePhysical
		}
		if oldPhysical > maxPhysical {
			maxPhysical = oldPhysical
		}

		// Overflow: logical counter would exceed mask — sleep and re-sample.
		if maxPhysical == oldPhysical {
			var expectedLogical uint64
			if maxPhysical == remotePhysical {
				expectedLogical = max(oldLogical, remoteLogical) + 1
			} else {
				expectedLogical = oldLogical + 1
			}
			if expectedLogical > logicalMask {
				time.Sleep(1 * time.Millisecond)
				now = uint64(max(time.Now().UnixMilli(), 0))
				continue
			}
		} else if maxPhysical == remotePhysical && remoteLogical+1 > logicalMask {
			time.Sleep(1 * time.Millisecond)
			now = uint64(max(time.Now().UnixMilli(), 0))
			continue
		}

		var newPhysical, newLogical uint64
		switch maxPhysical {
		case oldPhysical:
			if maxPhysical == remotePhysical {
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
			newPhysical = maxPhysical
			newLogical = 0
		}

		newVal := (newPhysical << logicalBits) | (newLogical & logicalMask)
		if c.state.CompareAndSwap(old, newVal) {
			return
		}
		// CAS failed — re-sample time and retry.
		now = uint64(max(time.Now().UnixMilli(), 0))
	}
}

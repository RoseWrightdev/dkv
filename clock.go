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
	for {
		old := c.state.Load()
		oldPhysical := old >> logicalBits
		oldLogical := old & logicalMask

		nowVal := max(time.Now().UnixMilli(), 0)
		now := uint64(nowVal)

		// Coordinated Backoff on Overflow:
		// If physical time hasn't progressed and the logical counter is exhausted,
		// sleep immediately to bypass atomic spinning contention entirely.
		if now <= oldPhysical && oldLogical >= logicalMask {
			time.Sleep(1 * time.Millisecond)
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
	}
}

// Update incorporates a remote timestamp to maintain causality.
// Should be called on every incoming message containing a timestamp.
func (c *HLC) Update(remote int64) {
	// todo: refactor
	if remote < 0 {
		return // Ignore invalid/negative remote timestamps
	}

	// #nosec G115
	remoteU := uint64(remote)

	remotePhysical := remoteU >> logicalBits
	remoteLogical := remoteU & logicalMask

	// Max drift threshold: 5 seconds (5000 ms)
	nowVal := max(time.Now().UnixMilli(), 0)
	now := uint64(nowVal)
	if remotePhysical > now+5000 {
		return // Ignore excessively drifted remote timestamps to prevent clock poisoning
	}

	for {
		old := c.state.Load()
		oldPhysical := old >> logicalBits
		oldLogical := old & logicalMask

		nowVal = max(time.Now().UnixMilli(), 0)
		now = uint64(nowVal)

		maxPhysical := now
		if remotePhysical > maxPhysical {
			maxPhysical = remotePhysical
		}
		if oldPhysical > maxPhysical {
			maxPhysical = oldPhysical
		}

		// If the advanced logical counter under LWW rules exceeds the counter mask,
		// sleep immediately before attempting any CAS operation to bypass atomic spinning contention.
		if maxPhysical == oldPhysical {
			var expectedLogical uint64
			if maxPhysical == remotePhysical {
				expectedLogical = max(oldLogical, remoteLogical) + 1
			} else {
				expectedLogical = oldLogical + 1
			}
			if expectedLogical > logicalMask {
				time.Sleep(1 * time.Millisecond)
				continue
			}
		} else if maxPhysical == remotePhysical {
			if remoteLogical+1 > logicalMask {
				time.Sleep(1 * time.Millisecond)
				continue
			}
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
	}
}

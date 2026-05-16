package dkv

import (
	"sync/atomic"
	"time"
)

// Clock defines an interface for providing distributed-safe timestamps.
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
		oldPhysical := int64(old >> logicalBits)
		oldLogical := int64(old & logicalMask)

		now := time.Now().UnixMilli()
		var newPhysical, newLogical int64

		if now > oldPhysical {
			newPhysical = now
			newLogical = 0
		} else {
			newPhysical = oldPhysical
			newLogical = oldLogical + 1
		}

		newVal := uint64((newPhysical << logicalBits) | (newLogical & logicalMask))
		if c.state.CompareAndSwap(old, newVal) {
			return int64(newVal)
		}
	}
}

// Update incorporates a remote timestamp to maintain causality.
// Should be called on every incoming message containing a timestamp.
func (c *HLC) Update(remote int64) {
	remoteU := uint64(remote)
	remotePhysical := int64(remoteU >> logicalBits)
	remoteLogical := int64(remoteU & logicalMask)

	for {
		old := c.state.Load()
		oldPhysical := int64(old >> logicalBits)
		oldLogical := int64(old & logicalMask)

		now := time.Now().UnixMilli()
		
		maxPhysical := now
		if remotePhysical > maxPhysical {
			maxPhysical = remotePhysical
		}
		if oldPhysical > maxPhysical {
			maxPhysical = oldPhysical
		}

		var newPhysical, newLogical int64
		if maxPhysical == oldPhysical && maxPhysical == remotePhysical {
			newPhysical = oldPhysical
			newLogical = max(oldLogical, remoteLogical) + 1
		} else if maxPhysical == oldPhysical {
			newPhysical = oldPhysical
			newLogical = oldLogical + 1
		} else if maxPhysical == remotePhysical {
			newPhysical = remotePhysical
			newLogical = remoteLogical + 1
		} else {
			newPhysical = maxPhysical
			newLogical = 0
		}

		newVal := uint64((newPhysical << logicalBits) | (newLogical & logicalMask))
		if c.state.CompareAndSwap(old, newVal) {
			return
		}
	}
}

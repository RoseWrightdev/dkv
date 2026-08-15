package clock

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestClock_Monotonicity(t *testing.T) {
	hlc := NewClock()

	ts1 := hlc.Now()
	ts2 := hlc.Now()
	assert.GreaterOrEqual(t, ts2, ts1)

	// Update with future time
	future := ts2 + 1000
	hlc.Update(int64(future))

	ts3 := hlc.Now()
	assert.GreaterOrEqual(t, ts3, int64(future))

	// Update with past time (should still be monotonic)
	past := ts3 - 500
	hlc.Update(int64(past))
	ts4 := hlc.Now()
	assert.GreaterOrEqual(t, ts4, ts3)
}

func TestClock_Drift(t *testing.T) {
	hlc := NewClock()
	now := time.Now().UnixMilli()

	// Future jump within drift limit (e.g., 2 seconds)
	futurePhysical := now + 2000
	futureHLC := futurePhysical << logicalBits
	hlc.Update(int64(futureHLC))

	ts := hlc.Now()
	// Should be around 'futureHLC'
	assert.InDelta(t, float64(futureHLC), float64(ts), float64(100<<logicalBits))
}

func TestClock_PoisoningProtection(t *testing.T) {
	hlc := NewClock()
	initialTS := hlc.Now()

	// 1. Extreme future drift (1 hour) - should be ignored
	now := time.Now().UnixMilli()
	extremeFutureHLC := (now + int64(time.Hour/time.Millisecond)) << logicalBits
	hlc.Update(int64(extremeFutureHLC))

	tsFuture := hlc.Now()
	// It should NOT have jumped to the extreme future; should be near initial physical time
	assert.Less(t, tsFuture, int64(extremeFutureHLC))
	assert.InDelta(t, float64(initialTS), float64(tsFuture), float64(500<<logicalBits))

	// 2. Negative HLC timestamp - should be ignored
	hlc.Update(-1000)
	tsNeg := hlc.Now()
	assert.GreaterOrEqual(t, tsNeg, tsFuture)
	assert.InDelta(t, float64(initialTS), float64(tsNeg), float64(500<<logicalBits))
}

// TestClock_SaturationDoesNotHangOrOverflow pins #104: a state already at the
// ceiling must make Now/Update return promptly, not spin forever (#104).
func TestClock_SaturationDoesNotHangOrOverflow(t *testing.T) {
	hlc := NewClock()
	// Physical at the ceiling, logical exhausted: pre-fix, this spins forever
	// in the overflow-retry loop since physical time can't advance further.
	hlc.state.Store((maxPhysical << logicalBits) | logicalMask)

	done := make(chan int64, 1)
	go func() { done <- hlc.Now() }()

	select {
	case ts := <-done:
		assert.Equal(t, int64(math.MaxInt64), ts)
	case <-time.After(2 * time.Second):
		t.Fatal("Now() did not return; likely spinning forever on a saturated clock")
	}

	// The state itself must stay pinned at the ceiling, not grow past it.
	assert.LessOrEqual(t, hlc.state.Load(), uint64(math.MaxInt64))

	// Update must also return promptly rather than hang.
	updateDone := make(chan struct{})
	go func() {
		hlc.Update(time.Now().UnixMilli() << logicalBits)
		close(updateDone)
	}()
	select {
	case <-updateDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Update() did not return on a saturated clock")
	}
	assert.LessOrEqual(t, hlc.state.Load(), uint64(math.MaxInt64))
}

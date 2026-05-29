package clock

import (
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

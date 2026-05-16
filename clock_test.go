package dkv

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestHLC_Monotonicity(t *testing.T) {
	hlc := NewHLC()
	
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

func TestHLC_Drift(t *testing.T) {
	hlc := NewHLC()
	now := time.Now().UnixNano()
	
	// Huge future jump
	future := now + int64(time.Hour)
	hlc.Update(future)
	
	ts := hlc.Now()
	// Should be around 'future'
	assert.InDelta(t, future, ts, float64(time.Second))
}

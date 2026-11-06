package backoff

import (
	"testing"
	"time"
)

func TestDurationStaysWithinBounds(t *testing.T) {
	p := Policy{Base: 100 * time.Millisecond, Cap: 5 * time.Second}
	for attempt := 1; attempt <= 20; attempt++ {
		for i := 0; i < 50; i++ {
			d := p.Duration(attempt)
			if d < 0 || d > p.Cap {
				t.Fatalf("attempt %d: delay %v out of bounds [0, %v]", attempt, d, p.Cap)
			}
		}
	}
}

func TestDurationGrowsWithAttemptBeforeCapping(t *testing.T) {
	p := Policy{Base: 10 * time.Millisecond, Cap: time.Hour}
	// With full jitter any single sample can be small, so compare maxima
	// observed across many samples per attempt instead of raw samples.
	maxAt := func(attempt int) time.Duration {
		var max time.Duration
		for i := 0; i < 200; i++ {
			if d := p.Duration(attempt); d > max {
				max = d
			}
		}
		return max
	}
	m1, m3, m6 := maxAt(1), maxAt(3), maxAt(6)
	if !(m1 < m3 && m3 < m6) {
		t.Fatalf("expected growing backoff ceiling, got maxima m1=%v m3=%v m6=%v", m1, m3, m6)
	}
}

func TestDurationRespectsCapAtHighAttempts(t *testing.T) {
	p := Policy{Base: time.Second, Cap: 2 * time.Second}
	for i := 0; i < 50; i++ {
		if d := p.Duration(50); d > p.Cap {
			t.Fatalf("attempt 50 exceeded cap: %v > %v", d, p.Cap)
		}
	}
}

// Package backoff computes retry delays using exponential backoff with full
// jitter (per the AWS Architecture Blog's "Exponential Backoff And Jitter"
// recommendation: jitter across the *entire* [0, cap] range, not just added
// noise on top of the exponential value — that full spread is what actually
// prevents retry storms from staying synchronized across many workers that
// all failed at the same moment, e.g. right after a leader/reaper event).
package backoff

import (
	"math/rand"
	"time"
)

type Policy struct {
	Base time.Duration // delay for attempt 1
	Cap  time.Duration // never wait longer than this
}

func Default() Policy {
	return Policy{Base: 500 * time.Millisecond, Cap: 2 * time.Minute}
}

// Duration returns the delay to wait before retrying the given attempt
// number (1-indexed: the delay before the *first* retry, i.e. after the
// first failure, is Duration(1)).
func (p Policy) Duration(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	exp := p.Base
	for i := 1; i < attempt && exp < p.Cap; i++ {
		exp *= 2
		if exp > p.Cap {
			exp = p.Cap
			break
		}
	}
	if exp > p.Cap {
		exp = p.Cap
	}
	// Full jitter: uniform random in [0, exp].
	if exp <= 0 {
		return 0
	}
	return time.Duration(rand.Int63n(int64(exp) + 1))
}

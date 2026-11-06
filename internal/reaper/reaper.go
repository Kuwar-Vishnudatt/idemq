// Package reaper runs the background loop that reclaims jobs whose lease
// expired without a heartbeat or ack — the mechanism that turns a crashed or
// partitioned worker's abandoned job back into a job someone else can pick
// up, instead of it sitting in_flight forever.
package reaper

import (
	"context"
	"log"
	"time"

	"github.com/Kuwar-Vishnudatt/idemq/internal/backoff"
	"github.com/Kuwar-Vishnudatt/idemq/internal/store"
)

type Reaper struct {
	db       *store.DB
	interval time.Duration
	policy   backoff.Policy
}

func New(db *store.DB, interval time.Duration, policy backoff.Policy) *Reaper {
	return &Reaper{db: db, interval: interval, policy: policy}
}

// Run blocks, sweeping for expired leases every interval, until ctx is
// cancelled.
func (r *Reaper) Run(ctx context.Context) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n, err := r.db.ReapExpired(time.Now(), r.policy.Duration)
			if err != nil {
				log.Printf("reaper: sweep error: %v", err)
				continue
			}
			if n > 0 {
				log.Printf("reaper: reclaimed %d job(s) with expired leases", n)
			}
		}
	}
}

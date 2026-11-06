package integration

import (
	"context"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/Kuwar-Vishnudatt/idemq/pkg/client"
)

// TestChaosNoJobLostOrDuplicatedUnderWorkerCrashes is the adversarial
// counterpart to TestConcurrentWorkersProcessEachJobExactlyOnce: instead of
// well-behaved workers, a fraction of claims are abandoned mid-flight (no
// heartbeat, no ack, no nack — simulating a worker process that crashes or
// gets stuck right after claiming), forcing the reaper to do real work
// reclaiming leases while other workers are concurrently claiming and
// completing jobs. The invariant under test: every job is eventually acked,
// and none is ever acked more than once, no matter how many times it had to
// be reclaimed first.
func TestChaosNoJobLostOrDuplicatedUnderWorkerCrashes(t *testing.T) {
	ts := startServer(t, tempDBPath(t), 15*time.Millisecond)
	producer := newClient(ts)
	ctx := context.Background()

	const numJobs = 150
	const numWorkers = 12
	const crashProbability = 0.35
	// High max_attempts: a reaped-and-requeued job consumes an attempt, and
	// with ~35% of claims "crashing" a job can need several reclaims before
	// a non-crashing worker finishes it. This test is about lost/duplicated
	// jobs, not DLQ behavior, so give it plenty of room before dead-lettering
	// would mask that distinction.
	const maxAttempts = 100

	want := make(map[string]bool, numJobs)
	for i := 0; i < numJobs; i++ {
		id, _, err := producer.EnqueueWithOpts(ctx, "chaos", i, client.EnqueueOpts{MaxAttempts: maxAttempts})
		if err != nil {
			t.Fatalf("enqueue: %v", err)
		}
		want[id] = true
	}

	var mu sync.Mutex
	acked := make(map[string]int)

	runWorker := func(ctx context.Context, rng *rand.Rand) {
		c := client.New(ts.URL)
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			job, err := c.Claim(ctx, "chaos", 60*time.Millisecond)
			if err != nil {
				select {
				case <-ctx.Done():
					return
				case <-time.After(10 * time.Millisecond):
				}
				continue
			}

			if rng.Float64() < crashProbability {
				// Simulated crash: abandon the job entirely. Do not
				// heartbeat, ack, or nack. The reaper must reclaim it.
				continue
			}

			if err := c.Ack(ctx, job.ID, job.LeaseToken); err == nil {
				mu.Lock()
				acked[job.ID]++
				mu.Unlock()
			}
			// A fenced ack (another reclaim already happened concurrently
			// with this one finishing) is an expected, benign outcome under
			// chaos, not recorded as a completion.
		}
	}

	runCtx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		rng := rand.New(rand.NewSource(int64(i) + 1))
		go func() {
			defer wg.Done()
			runWorker(runCtx, rng)
		}()
	}

	deadline := time.After(30 * time.Second)
waitLoop:
	for {
		mu.Lock()
		done := len(acked) == numJobs
		mu.Unlock()
		if done {
			break waitLoop
		}
		select {
		case <-deadline:
			mu.Lock()
			n := len(acked)
			mu.Unlock()
			t.Fatalf("timed out under chaos: acked %d/%d jobs", n, numJobs)
		case <-time.After(25 * time.Millisecond):
		}
	}
	cancel()
	wg.Wait()

	for id := range want {
		if acked[id] != 1 {
			t.Errorf("job %s acked %d times under chaos, want exactly 1", id, acked[id])
		}
	}

	dead, err := producer.ListDead(ctx, "chaos")
	if err != nil {
		t.Fatal(err)
	}
	if len(dead) != 0 {
		t.Errorf("expected no jobs dead-lettered (max_attempts=%d should be more than enough headroom), got %d", maxAttempts, len(dead))
	}
}

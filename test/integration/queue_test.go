package integration

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Kuwar-Vishnudatt/idemq/pkg/client"
)

// TestConcurrentWorkersProcessEachJobExactlyOnce is the real "distributed"
// proof: many independent HTTP client goroutines (standing in for separate
// worker processes/machines) claim from one live server backed by one
// SQLite file, with no in-process coordination between them beyond the
// server's own fencing. If claim/ack weren't atomic, we'd see a job
// processed by two workers at once or acked twice.
func TestConcurrentWorkersProcessEachJobExactlyOnce(t *testing.T) {
	ts := startServer(t, tempDBPath(t), 50*time.Millisecond)
	producer := newClient(ts)

	const numJobs = 200
	const numWorkers = 10

	want := make(map[string]bool, numJobs)
	for i := 0; i < numJobs; i++ {
		id, _, err := producer.Enqueue(context.Background(), "work", map[string]int{"n": i}, "")
		if err != nil {
			t.Fatalf("enqueue: %v", err)
		}
		want[id] = true
	}

	var mu sync.Mutex
	seen := make(map[string]int)
	var inFlight sync.Map // jobID -> true while a handler is actively running it

	handler := func(ctx context.Context, job *client.Job) error {
		if _, already := inFlight.LoadOrStore(job.ID, true); already {
			t.Errorf("job %s handled concurrently by two workers at once", job.ID)
		}
		defer inFlight.Delete(job.ID)

		time.Sleep(2 * time.Millisecond) // simulate work, widen the window for a race to show up

		mu.Lock()
		seen[job.ID]++
		mu.Unlock()
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			client.RunWorker(ctx, client.New(ts.URL), client.WorkerConfig{
				Queue:        "work",
				PollInterval: 10 * time.Millisecond,
			}, handler)
		}()
	}

	deadline := time.After(15 * time.Second)
	for {
		mu.Lock()
		done := len(seen) == numJobs
		mu.Unlock()
		if done {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("timed out: processed %d/%d jobs", len(seen), numJobs)
		case <-time.After(20 * time.Millisecond):
		}
	}
	cancel()
	wg.Wait()

	for id := range want {
		if seen[id] != 1 {
			t.Errorf("job %s was acked %d times, want exactly 1", id, seen[id])
		}
	}
}

// TestPoisonMessageDeadLettersThenCanBeReplayed verifies the full failure
// lifecycle: a handler that always errors exhausts max_attempts, the job
// lands in the DLQ (and is no longer claimable), an operator can inspect it
// via the DLQ listing, and requeuing it makes it processable again.
func TestPoisonMessageDeadLettersThenCanBeReplayed(t *testing.T) {
	ts := startServer(t, tempDBPath(t), 20*time.Millisecond)
	c := newClient(ts)
	ctx := context.Background()

	id, _, err := c.EnqueueWithOpts(ctx, "emails", map[string]string{"to": "a@example.com"}, client.EnqueueOpts{MaxAttempts: 3})
	if err != nil {
		t.Fatal(err)
	}

	for attempt := 1; attempt <= 3; attempt++ {
		job, err := c.Claim(ctx, "emails", time.Second)
		if err != nil {
			t.Fatalf("claim attempt %d: %v", attempt, err)
		}
		if err := c.Nack(ctx, job.ID, job.LeaseToken, "smtp: poison payload", 0); err != nil {
			t.Fatalf("nack attempt %d: %v", attempt, err)
		}
	}

	// Exhausted: no longer claimable.
	if _, err := c.Claim(ctx, "emails", time.Second); !errors.Is(err, client.ErrNoJob) {
		t.Fatalf("expected dead-lettered job to be unclaimable, got %v", err)
	}

	dead, err := c.ListDead(ctx, "emails")
	if err != nil {
		t.Fatal(err)
	}
	if len(dead) != 1 || dead[0].ID != id {
		t.Fatalf("expected the poison job in the DLQ, got %+v", dead)
	}

	if err := c.RequeueDead(ctx, id); err != nil {
		t.Fatalf("requeue: %v", err)
	}

	job, err := c.Claim(ctx, "emails", time.Second)
	if err != nil {
		t.Fatalf("expected replayed job to be claimable: %v", err)
	}
	if job.ID != id || job.Attempts != 1 {
		t.Fatalf("expected fresh attempt counter after replay, got %+v", job)
	}
	if err := c.Ack(ctx, job.ID, job.LeaseToken); err != nil {
		t.Fatalf("ack: %v", err)
	}
}

// TestZombieWorkerIsFencedAfterReclaim simulates the scenario fencing tokens
// exist for: worker A claims a job and then stalls forever (crashed, GC
// paused, network-partitioned — it never heartbeats or acks). The reaper
// reclaims the lease, worker B claims and finishes the same job, and then
// worker A "wakes up" and tries to ack with its now-stale token. The server
// must refuse it — otherwise A's late ack could incorrectly mark a job
// someone else is actively reprocessing (or already finished) as done.
func TestZombieWorkerIsFencedAfterReclaim(t *testing.T) {
	ts := startServer(t, tempDBPath(t), 20*time.Millisecond)
	c := newClient(ts)
	ctx := context.Background()

	_, _, err := c.Enqueue(ctx, "work", map[string]string{"task": "ship it"}, "")
	if err != nil {
		t.Fatal(err)
	}

	zombie, err := c.Claim(ctx, "work", 30*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}

	// Give the reaper time to notice the expired lease and requeue the job.
	time.Sleep(150 * time.Millisecond)

	reclaimer, err := c.Claim(ctx, "work", time.Second)
	if err != nil {
		t.Fatalf("expected reclaim to succeed after lease expiry: %v", err)
	}
	if reclaimer.ID != zombie.ID {
		t.Fatalf("expected the same job to be reclaimed")
	}
	if reclaimer.LeaseToken == zombie.LeaseToken {
		t.Fatalf("expected a fresh lease token after reclaim")
	}

	if err := c.Ack(ctx, zombie.ID, zombie.LeaseToken); !errors.Is(err, client.ErrFenced) {
		t.Fatalf("expected zombie's stale ack to be fenced, got %v", err)
	}

	if err := c.Ack(ctx, reclaimer.ID, reclaimer.LeaseToken); err != nil {
		t.Fatalf("current owner's ack should succeed: %v", err)
	}
}

// TestIdempotentEnqueueOverHTTP checks the producer-side guarantee end to
// end: a producer that retries an enqueue call (e.g. after a timeout, unsure
// whether its first POST landed) must not create a second job.
func TestIdempotentEnqueueOverHTTP(t *testing.T) {
	ts := startServer(t, tempDBPath(t), time.Second)
	c := newClient(ts)
	ctx := context.Background()

	key := "order-confirmation-42"
	id1, dup1, err := c.Enqueue(ctx, "emails", map[string]string{"to": "x@example.com"}, key)
	if err != nil || dup1 {
		t.Fatalf("first enqueue: id=%s dup=%v err=%v", id1, dup1, err)
	}
	for i := 0; i < 5; i++ {
		id2, dup2, err := c.Enqueue(ctx, "emails", map[string]string{"to": "x@example.com"}, key)
		if err != nil {
			t.Fatalf("retry %d: %v", i, err)
		}
		if !dup2 || id2 != id1 {
			t.Fatalf("retry %d: expected duplicate of %s, got id=%s dup=%v", i, id1, id2, dup2)
		}
	}

	stats, err := c.Stats(ctx, "emails")
	if err != nil {
		t.Fatal(err)
	}
	if stats.Pending != 1 {
		t.Fatalf("expected exactly 1 job despite 6 enqueue calls, got stats=%+v", stats)
	}
}

// TestCrashRecovery kills the server process (closes it without any
// graceful ack/heartbeat) mid-job and verifies a freshly started server
// against the same database file recovers: the orphaned in-flight job is
// reaped and becomes claimable again, exactly as it would after a real
// process crash and restart.
func TestCrashRecovery(t *testing.T) {
	dbPath := tempDBPath(t)

	ts1 := startServer(t, dbPath, time.Hour) // reaper interval irrelevant; we crash before it'd fire
	c1 := newClient(ts1)
	ctx := context.Background()

	id, _, err := c1.Enqueue(ctx, "work", map[string]string{"task": "durable"}, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c1.Claim(ctx, "work", 10*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	// ts1's db/server are torn down by t.Cleanup only at test end, so close
	// the DB explicitly now to simulate the crash happening right here.
	ts1.DB.Close()

	time.Sleep(20 * time.Millisecond) // let the short lease actually expire

	ts2 := startServer(t, dbPath, 20*time.Millisecond)
	c2 := newClient(ts2)

	time.Sleep(100 * time.Millisecond) // let ts2's reaper run at least once

	job, err := c2.Claim(ctx, "work", time.Second)
	if err != nil {
		t.Fatalf("expected job to survive the crash and be reclaimable: %v", err)
	}
	if job.ID != id {
		t.Fatalf("unexpected job reclaimed: %+v", job)
	}
	if err := c2.Ack(ctx, job.ID, job.LeaseToken); err != nil {
		t.Fatalf("ack: %v", err)
	}
}

func TestPriorityServedFirstOverHTTP(t *testing.T) {
	ts := startServer(t, tempDBPath(t), time.Second)
	c := newClient(ts)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		if _, _, err := c.EnqueueWithOpts(ctx, "work", fmt.Sprintf("low-%d", i), client.EnqueueOpts{Priority: 0}); err != nil {
			t.Fatal(err)
		}
	}
	highID, _, err := c.EnqueueWithOpts(ctx, "work", "urgent", client.EnqueueOpts{Priority: 100})
	if err != nil {
		t.Fatal(err)
	}

	job, err := c.Claim(ctx, "work", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if job.ID != highID {
		t.Fatalf("expected high-priority job claimed first, got %s", job.ID)
	}
}

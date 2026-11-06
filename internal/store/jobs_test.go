package store

import (
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func newTestDB(t *testing.T) *DB {
	t.Helper()
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestEnqueueAndClaim(t *testing.T) {
	db := newTestDB(t)
	id, dup, err := db.Enqueue("emails", []byte("hello"), EnqueueOpts{})
	if err != nil || dup {
		t.Fatalf("enqueue: id=%s dup=%v err=%v", id, dup, err)
	}

	job, err := db.Claim("emails", time.Minute)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if job.ID != id || job.Status != StatusInFlight || job.Attempts != 1 {
		t.Fatalf("unexpected job: %+v", job)
	}

	if _, err := db.Claim("emails", time.Minute); !errors.Is(err, ErrNoJob) {
		t.Fatalf("expected ErrNoJob on second claim, got %v", err)
	}
}

func TestIdempotentEnqueueDedupes(t *testing.T) {
	db := newTestDB(t)
	id1, dup1, err := db.Enqueue("emails", []byte("v1"), EnqueueOpts{IdempotencyKey: "order-42"})
	if err != nil || dup1 {
		t.Fatalf("first enqueue: %v dup=%v", err, dup1)
	}
	id2, dup2, err := db.Enqueue("emails", []byte("v2-different-payload"), EnqueueOpts{IdempotencyKey: "order-42"})
	if err != nil {
		t.Fatalf("second enqueue: %v", err)
	}
	if !dup2 || id2 != id1 {
		t.Fatalf("expected duplicate pointing at %s, got id=%s dup=%v", id1, id2, dup2)
	}

	stats, err := db.Stats("emails")
	if err != nil {
		t.Fatal(err)
	}
	if stats.Pending != 1 {
		t.Fatalf("expected exactly 1 job to exist, got stats=%+v", stats)
	}
}

func TestAckRequiresMatchingLeaseToken(t *testing.T) {
	db := newTestDB(t)
	id, _, _ := db.Enqueue("q", []byte("x"), EnqueueOpts{})
	job, err := db.Claim("q", time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	if err := db.Ack(id, job.LeaseToken+999); !errors.Is(err, ErrFenced) {
		t.Fatalf("expected ErrFenced for wrong token, got %v", err)
	}
	if err := db.Ack(id, job.LeaseToken); err != nil {
		t.Fatalf("ack with correct token should succeed: %v", err)
	}
	// Second ack with the now-stale (but previously correct) token must fail:
	// the job is no longer in_flight.
	if err := db.Ack(id, job.LeaseToken); !errors.Is(err, ErrFenced) {
		t.Fatalf("expected ErrFenced on double-ack, got %v", err)
	}
}

func TestNackRetriesThenDeadLetters(t *testing.T) {
	db := newTestDB(t)
	id, _, _ := db.Enqueue("q", []byte("poison"), EnqueueOpts{MaxAttempts: 2})

	for attempt := 1; attempt <= 2; attempt++ {
		job, err := db.Claim("q", time.Minute)
		if err != nil {
			t.Fatalf("claim attempt %d: %v", attempt, err)
		}
		if job.ID != id {
			t.Fatalf("claimed wrong job")
		}
		if err := db.Nack(id, job.LeaseToken, "handler panicked", 0); err != nil {
			t.Fatalf("nack attempt %d: %v", attempt, err)
		}
	}

	job, err := db.Get(id)
	if err != nil {
		t.Fatal(err)
	}
	if job.Status != StatusDead {
		t.Fatalf("expected job dead-lettered after max_attempts, got status=%s attempts=%d", job.Status, job.Attempts)
	}

	dead, err := db.ListDead("q")
	if err != nil || len(dead) != 1 {
		t.Fatalf("ListDead: %v %+v", err, dead)
	}

	if err := db.RequeueDead(id); err != nil {
		t.Fatalf("requeue dead: %v", err)
	}
	job, _ = db.Get(id)
	if job.Status != StatusPending || job.Attempts != 0 {
		t.Fatalf("expected reset pending job, got %+v", job)
	}
}

func TestReapExpiredRequeuesAndFencesOriginalWorker(t *testing.T) {
	db := newTestDB(t)
	id, _, _ := db.Enqueue("q", []byte("x"), EnqueueOpts{})

	job, err := db.Claim("q", 10*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	staleToken := job.LeaseToken

	time.Sleep(30 * time.Millisecond)
	n, err := db.ReapExpired(time.Now(), func(int) time.Duration { return 0 })
	if err != nil {
		t.Fatalf("reap: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected 1 job reaped, got %d", n)
	}

	// The job should be claimable again by a new worker, with a new token.
	job2, err := db.Claim("q", time.Minute)
	if err != nil {
		t.Fatalf("expected reclaim to succeed: %v", err)
	}
	if job2.ID != id {
		t.Fatalf("expected same job id")
	}
	if job2.LeaseToken == staleToken {
		t.Fatalf("expected a fresh lease token after reap")
	}

	// The original (zombie) worker, waking up late, must not be able to ack
	// using its now-stale token — someone else holds the job now.
	if err := db.Ack(id, staleToken); !errors.Is(err, ErrFenced) {
		t.Fatalf("expected stale worker's ack to be fenced, got %v", err)
	}

	// The new worker's ack with the current token must succeed.
	if err := db.Ack(id, job2.LeaseToken); err != nil {
		t.Fatalf("current worker's ack should succeed: %v", err)
	}
}

func TestHeartbeatExtendsLeaseAndPreventsReap(t *testing.T) {
	db := newTestDB(t)
	id, _, _ := db.Enqueue("q", []byte("x"), EnqueueOpts{})
	job, err := db.Claim("q", 20*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(10 * time.Millisecond)
	if err := db.Heartbeat(id, job.LeaseToken, 200*time.Millisecond); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}

	time.Sleep(20 * time.Millisecond) // past the original lease, within the extended one
	n, err := db.ReapExpired(time.Now(), func(int) time.Duration { return 0 })
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("expected heartbeated job to survive reap, but %d jobs were reaped", n)
	}

	if err := db.Ack(id, job.LeaseToken); err != nil {
		t.Fatalf("ack after heartbeat should still work: %v", err)
	}
}

func TestPersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	id, _, _ := db.Enqueue("q", []byte("durable"), EnqueueOpts{})
	if _, err := db.Claim("q", time.Millisecond); err != nil {
		t.Fatal(err)
	}
	db.Close() // simulate a crash: job left in_flight with an already-expiring lease

	time.Sleep(5 * time.Millisecond)

	db2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()

	n, err := db2.ReapExpired(time.Now(), func(int) time.Duration { return 0 })
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("expected the crashed in-flight job to be reaped after restart, got n=%d", n)
	}

	job, err := db2.Get(id)
	if err != nil {
		t.Fatal(err)
	}
	if job.Status != StatusPending {
		t.Fatalf("expected job pending again after restart+reap, got %s", job.Status)
	}
}

func TestPriorityOrdering(t *testing.T) {
	db := newTestDB(t)
	lowID, _, _ := db.Enqueue("q", []byte("low"), EnqueueOpts{Priority: 0})
	highID, _, _ := db.Enqueue("q", []byte("high"), EnqueueOpts{Priority: 10})
	_ = lowID

	job, err := db.Claim("q", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if job.ID != highID {
		t.Fatalf("expected high-priority job claimed first, got %s", job.ID)
	}
}

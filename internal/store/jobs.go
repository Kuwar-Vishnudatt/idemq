package store

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/Kuwar-Vishnudatt/idemq/internal/idgen"
)

const (
	StatusPending   = "pending"
	StatusInFlight  = "in_flight"
	StatusCompleted = "completed"
	StatusDead      = "dead"
)

var (
	// ErrNoJob is returned by Claim when no job is currently claimable.
	ErrNoJob = errors.New("store: no job available")
	// ErrFenced is returned by Heartbeat/Ack/Nack when the caller's lease
	// token no longer matches the job's current lease — either another
	// worker has since reclaimed it (the original lease expired and the
	// reaper requeued it), or the job has already reached a terminal state.
	// This is the fencing-token check: without it, a worker that stalls
	// past its lease (GC pause, network partition, slow handler) can wake
	// up and ack/nack a job that a *different* worker is now processing,
	// silently corrupting delivery guarantees. See docs/ARCHITECTURE.md.
	ErrFenced = errors.New("store: stale lease token (job was reclaimed or already finalized)")
	// ErrNotFound is returned when a job ID does not exist.
	ErrNotFound = errors.New("store: job not found")
)

type Job struct {
	ID             string
	Queue          string
	IdempotencyKey string
	Payload        []byte
	Status         string
	Priority       int
	Attempts       int
	MaxAttempts    int
	AvailableAt    time.Time
	LeaseToken     int64
	LeaseExpiresAt time.Time
	LastError      string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

type EnqueueOpts struct {
	IdempotencyKey string
	Delay          time.Duration
	MaxAttempts    int
	Priority       int
}

// Enqueue inserts a new job. If opts.IdempotencyKey is set and a job with
// the same (queue, idempotency_key) already exists, Enqueue does NOT insert
// a duplicate — it returns the existing job's ID and duplicate=true. This is
// what makes retried *producer* calls safe (e.g. an API server that retries
// an enqueue request after a timeout, unsure whether the first attempt
// landed): the second call is a no-op from the queue's point of view.
func (d *DB) Enqueue(queue string, payload []byte, opts EnqueueOpts) (id string, duplicate bool, err error) {
	if opts.MaxAttempts <= 0 {
		opts.MaxAttempts = 5
	}
	now := time.Now()
	availableAt := now.Add(opts.Delay)
	newID := idgen.New()

	var idemKey any
	if opts.IdempotencyKey != "" {
		idemKey = opts.IdempotencyKey
	}

	_, err = d.write.Exec(`
		INSERT INTO jobs (id, queue, idempotency_key, payload, status, priority,
			attempts, max_attempts, available_at, lease_token, lease_expires_at,
			last_error, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?, 0, 0, '', ?, ?)`,
		newID, queue, idemKey, payload, StatusPending, opts.Priority,
		opts.MaxAttempts, availableAt.UnixNano(), now.UnixNano(), now.UnixNano())

	if err == nil {
		return newID, false, nil
	}
	if !isUniqueViolation(err) {
		return "", false, fmt.Errorf("enqueue: %w", err)
	}

	// Unique constraint hit: a job with this idempotency key already exists.
	// Look it up and return it instead of propagating the conflict — this is
	// the whole point of the idempotency key.
	var existing string
	qerr := d.read.QueryRow(
		`SELECT id FROM jobs WHERE queue = ? AND idempotency_key = ?`,
		queue, opts.IdempotencyKey,
	).Scan(&existing)
	if qerr != nil {
		return "", false, fmt.Errorf("enqueue: resolve duplicate: %w", qerr)
	}
	return existing, true, nil
}

// Claim atomically selects the highest-priority, earliest-available pending
// job in the queue, marks it in_flight, assigns it a fresh monotonically
// increasing lease token, and returns it. Returns ErrNoJob if nothing is
// claimable right now.
func (d *DB) Claim(queue string, leaseDuration time.Duration) (*Job, error) {
	tx, err := d.write.Begin()
	if err != nil {
		return nil, fmt.Errorf("claim: begin: %w", err)
	}
	defer tx.Rollback()

	now := time.Now()
	var id string
	err = tx.QueryRow(`
		SELECT id FROM jobs
		WHERE queue = ? AND status = ? AND available_at <= ?
		ORDER BY priority DESC, available_at ASC
		LIMIT 1`,
		queue, StatusPending, now.UnixNano(),
	).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNoJob
	}
	if err != nil {
		return nil, fmt.Errorf("claim: select: %w", err)
	}

	var token int64
	if err := tx.QueryRow(`UPDATE lease_seq SET value = value + 1 WHERE name = 'lease' RETURNING value`).Scan(&token); err != nil {
		return nil, fmt.Errorf("claim: next lease token: %w", err)
	}

	leaseExpires := now.Add(leaseDuration)
	res, err := tx.Exec(`
		UPDATE jobs SET status = ?, lease_token = ?, lease_expires_at = ?,
			attempts = attempts + 1, updated_at = ?
		WHERE id = ? AND status = ?`,
		StatusInFlight, token, leaseExpires.UnixNano(), now.UnixNano(), id, StatusPending)
	if err != nil {
		return nil, fmt.Errorf("claim: update: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		// Lost a race with another claim in between the SELECT and UPDATE
		// within this same process. Can't happen with MaxOpenConns(1) since
		// transactions on the one write connection can't interleave, but
		// the check costs nothing and documents the invariant explicitly.
		return nil, ErrNoJob
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("claim: commit: %w", err)
	}

	return d.Get(id)
}

// Heartbeat extends an in-flight job's lease. It fails with ErrFenced if the
// token doesn't match the job's current lease (the lease already expired and
// was reclaimed by the reaper, or the job already reached a terminal state).
func (d *DB) Heartbeat(id string, token int64, extend time.Duration) error {
	res, err := d.write.Exec(`
		UPDATE jobs SET lease_expires_at = ?, updated_at = ?
		WHERE id = ? AND lease_token = ? AND status = ?`,
		time.Now().Add(extend).UnixNano(), time.Now().UnixNano(), id, token, StatusInFlight)
	if err != nil {
		return fmt.Errorf("heartbeat: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrFenced
	}
	return nil
}

// Ack marks a job completed. Fenced callers (stale lease) get ErrFenced
// instead of silently "succeeding" — a zombie worker must never be able to
// mark a job someone else is now processing (or already finished) as done.
func (d *DB) Ack(id string, token int64) error {
	res, err := d.write.Exec(`
		UPDATE jobs SET status = ?, updated_at = ?
		WHERE id = ? AND lease_token = ? AND status = ?`,
		StatusCompleted, time.Now().UnixNano(), id, token, StatusInFlight)
	if err != nil {
		return fmt.Errorf("ack: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrFenced
	}
	return nil
}

// Nack reports a failed attempt. If attempts have reached max_attempts the
// job moves to the dead-letter state (StatusDead); otherwise it is
// rescheduled after delay and becomes claimable again. Fenced the same way
// as Ack.
func (d *DB) Nack(id string, token int64, errMsg string, delay time.Duration) error {
	tx, err := d.write.Begin()
	if err != nil {
		return fmt.Errorf("nack: begin: %w", err)
	}
	defer tx.Rollback()

	var attempts, maxAttempts int
	err = tx.QueryRow(`SELECT attempts, max_attempts FROM jobs WHERE id = ? AND lease_token = ? AND status = ?`,
		id, token, StatusInFlight).Scan(&attempts, &maxAttempts)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrFenced
	}
	if err != nil {
		return fmt.Errorf("nack: select: %w", err)
	}

	now := time.Now()
	if attempts >= maxAttempts {
		_, err = tx.Exec(`UPDATE jobs SET status = ?, last_error = ?, updated_at = ? WHERE id = ?`,
			StatusDead, errMsg, now.UnixNano(), id)
	} else {
		_, err = tx.Exec(`UPDATE jobs SET status = ?, last_error = ?, available_at = ?, updated_at = ? WHERE id = ?`,
			StatusPending, errMsg, now.Add(delay).UnixNano(), now.UnixNano(), id)
	}
	if err != nil {
		return fmt.Errorf("nack: update: %w", err)
	}
	return tx.Commit()
}

// ReapExpired finds in_flight jobs whose lease has expired — the worker
// holding them crashed, hung, or got partitioned away without heartbeating
// or acking — and treats each one as an implicit Nack with a synthetic
// "lease expired" error. It reuses the exact same fencing-respecting UPDATE
// as Nack (keyed on the lease_token read a moment earlier), so if the
// original worker heartbeats or acks in the brief window between the reaper
// reading and writing, the reaper's write simply affects zero rows and is
// skipped — no lost update, no double-requeue.
func (d *DB) ReapExpired(now time.Time, delayFor func(attempt int) time.Duration) (int, error) {
	rows, err := d.read.Query(`
		SELECT id, lease_token FROM jobs WHERE status = ? AND lease_expires_at < ?`,
		StatusInFlight, now.UnixNano())
	if err != nil {
		return 0, fmt.Errorf("reap: query: %w", err)
	}
	type expired struct {
		id    string
		token int64
	}
	var candidates []expired
	for rows.Next() {
		var e expired
		if err := rows.Scan(&e.id, &e.token); err != nil {
			rows.Close()
			return 0, fmt.Errorf("reap: scan: %w", err)
		}
		candidates = append(candidates, e)
	}
	rows.Close()

	reaped := 0
	for _, c := range candidates {
		j, err := d.Get(c.id)
		if err != nil {
			continue
		}
		delay := delayFor(j.Attempts)
		err = d.Nack(c.id, c.token, "lease expired: worker did not heartbeat or ack in time", delay)
		if err == nil {
			reaped++
		} else if !errors.Is(err, ErrFenced) {
			return reaped, fmt.Errorf("reap: nack %s: %w", c.id, err)
		}
		// ErrFenced here just means the original worker finished the job in
		// the window between our read and write; correctly not double-counted.
	}
	return reaped, nil
}

func (d *DB) Get(id string) (*Job, error) {
	j, err := scanJob(d.read.QueryRow(`
		SELECT id, queue, COALESCE(idempotency_key,''), payload, status, priority,
			attempts, max_attempts, available_at, lease_token, lease_expires_at,
			COALESCE(last_error,''), created_at, updated_at
		FROM jobs WHERE id = ?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	return j, err
}

func (d *DB) ListDead(queue string) ([]Job, error) {
	rows, err := d.read.Query(`
		SELECT id, queue, COALESCE(idempotency_key,''), payload, status, priority,
			attempts, max_attempts, available_at, lease_token, lease_expires_at,
			COALESCE(last_error,''), created_at, updated_at
		FROM jobs WHERE queue = ? AND status = ? ORDER BY updated_at DESC`,
		queue, StatusDead)
	if err != nil {
		return nil, fmt.Errorf("list dead: %w", err)
	}
	defer rows.Close()

	var out []Job
	for rows.Next() {
		j, err := scanJobRows(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *j)
	}
	return out, rows.Err()
}

// RequeueDead moves a dead-lettered job back to pending with a reset attempt
// counter, available immediately. Used for manual DLQ replay after a human
// has fixed whatever made the job unprocessable.
func (d *DB) RequeueDead(id string) error {
	res, err := d.write.Exec(`
		UPDATE jobs SET status = ?, attempts = 0, available_at = ?, updated_at = ?
		WHERE id = ? AND status = ?`,
		StatusPending, time.Now().UnixNano(), time.Now().UnixNano(), id, StatusDead)
	if err != nil {
		return fmt.Errorf("requeue dead: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrNotFound
	}
	return nil
}

type QueueStats struct {
	Pending   int
	InFlight  int
	Completed int
	Dead      int
}

func (d *DB) Stats(queue string) (QueueStats, error) {
	var s QueueStats
	rows, err := d.read.Query(`SELECT status, COUNT(*) FROM jobs WHERE queue = ? GROUP BY status`, queue)
	if err != nil {
		return s, fmt.Errorf("stats: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var status string
		var n int
		if err := rows.Scan(&status, &n); err != nil {
			return s, err
		}
		switch status {
		case StatusPending:
			s.Pending = n
		case StatusInFlight:
			s.InFlight = n
		case StatusCompleted:
			s.Completed = n
		case StatusDead:
			s.Dead = n
		}
	}
	return s, rows.Err()
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanJob(r rowScanner) (*Job, error) { return scanJobRows(r) }

func scanJobRows(r rowScanner) (*Job, error) {
	var j Job
	var availableAt, leaseExpiresAt, createdAt, updatedAt int64
	err := r.Scan(&j.ID, &j.Queue, &j.IdempotencyKey, &j.Payload, &j.Status, &j.Priority,
		&j.Attempts, &j.MaxAttempts, &availableAt, &j.LeaseToken, &leaseExpiresAt,
		&j.LastError, &createdAt, &updatedAt)
	if err != nil {
		return nil, err
	}
	j.AvailableAt = time.Unix(0, availableAt)
	j.LeaseExpiresAt = time.Unix(0, leaseExpiresAt)
	j.CreatedAt = time.Unix(0, createdAt)
	j.UpdatedAt = time.Unix(0, updatedAt)
	return &j, nil
}

func isUniqueViolation(err error) bool {
	// modernc.org/sqlite wraps the sqlite error code in its own error type;
	// rather than importing its internals, match on the message sqlite
	// itself produces, which is stable across driver versions.
	return err != nil && containsAny(err.Error(), "UNIQUE constraint failed", "constraint failed: UNIQUE")
}

func containsAny(s string, subs ...string) bool {
	for _, sub := range subs {
		if len(s) >= len(sub) {
			for i := 0; i+len(sub) <= len(s); i++ {
				if s[i:i+len(sub)] == sub {
					return true
				}
			}
		}
	}
	return false
}

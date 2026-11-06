# idemq

A durable, distributed job queue with lease-based claiming, fencing-token
safety, exponential-backoff retries, and automatic dead-letter handling —
the production-grade pieces behind tools like Sidekiq, Celery, SQS, and
Cloud Tasks, built from scratch in Go to understand (and get right) the
failure modes those systems spend most of their complexity on.

```
   producers                    idemq-server                    workers
┌─────────────┐    HTTP     ┌──────────────────┐    HTTP     ┌─────────────┐
│ enqueue(job,│ ──────────▶ │  SQLite (WAL)     │ ◀────────── │ claim()     │
│ idempotency_│             │  jobs table:      │             │ heartbeat() │
│  key)       │             │   pending         │ ──────────▶ │ ack()/nack()│
└─────────────┘             │   in_flight       │             └─────────────┘
                             │   completed       │                   │
                             │   dead (DLQ)      │                   │  many of
                        ┌───▶│                   │                   │  these,
                        │    └──────────────────┘                   │  any machine
                   reaper (background):
                   sweeps in_flight jobs whose
                   lease_expires_at has passed,
                   requeues them with backoff
```

Any number of worker *processes* — on any machine — can claim from the same
server concurrently. Safety doesn't depend on workers behaving correctly: a
monotonic **fencing token** issued on every claim means a worker that stalls
past its lease (GC pause, crash, network partition) can never corrupt a job
another worker has since picked up, even if it "wakes up" later and tries to
ack or nack.

## Why this exists

Every one of the properties below is something a real queue gets wrong if
you don't design for it explicitly, and each is demonstrated with a test
that would fail if the property were broken — see
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the full reasoning and the
bugs found along the way:

- **Idempotent enqueue.** A producer retrying an enqueue call after a
  timeout (unsure whether the first attempt landed) supplies the same
  idempotency key and gets back the *same* job, not a duplicate.
- **Exactly-once-delivery is not the promise — exactly-once-effect is.**
  idemq delivers **at-least-once**, like almost every real queue. What it
  guarantees is that a job can never be marked complete by two different
  claimants, and gives producers the tool (idempotency keys) to make retried
  side effects themselves safe — this is the actual Stripe-style idempotency
  pattern, not a claim that distributed exactly-once execution exists (it
  doesn't; see ARCHITECTURE.md).
- **Fencing tokens**, not just lease timeouts. A lease timeout alone tells a
  *stalled* worker nothing — it can still come back and ack. Every claim
  gets a strictly increasing token; ack/nack/heartbeat are rejected unless
  the token is still current.
- **Poison-message handling.** A job that fails `max_attempts` times moves
  to a dead-letter state automatically, is inspectable via the API, and can
  be replayed once fixed.
- **Exponential backoff with full jitter**, not fixed-interval retry —
  avoids retry storms synchronizing across workers.
- **Crash recovery.** The server can be killed mid-job and restarted against
  the same SQLite file; orphaned in-flight jobs are reaped and reclaimed.

## Quickstart

```bash
make build
make demo     # starts a server + two flaky demo workers, enqueues sample jobs
```

Then, in another terminal:

```bash
curl localhost:8080/v1/queues/emails/stats
curl localhost:8080/v1/queues/emails/dlq
```

## API

| Method & path                          | Purpose                                   |
|-----------------------------------------|--------------------------------------------|
| `POST /v1/queues/{queue}/enqueue`       | Add a job (optionally idempotent, delayed, prioritized) |
| `POST /v1/queues/{queue}/claim`         | Atomically claim the next job; `204` if none |
| `POST /v1/jobs/{id}/heartbeat`          | Extend an in-flight job's lease            |
| `POST /v1/jobs/{id}/ack`                | Mark a job completed                       |
| `POST /v1/jobs/{id}/nack`               | Report failure → retry with backoff, or dead-letter |
| `GET  /v1/queues/{queue}/dlq`           | List dead-lettered jobs                    |
| `POST /v1/jobs/{id}/requeue`            | Replay a dead-lettered job                 |
| `GET  /v1/queues/{queue}/stats`         | Pending/in-flight/completed/dead counts    |

`ack`/`nack`/`heartbeat` return `409 Conflict` when the caller's lease token
is stale — see `pkg/client.ErrFenced`.

## Client SDK

`pkg/client` is a reference worker implementation: `client.RunWorker` claims
a job, runs your handler with an automatic background heartbeat keeping the
lease alive, and acks or nacks based on the result.

```go
client.RunWorker(ctx, client.New("http://localhost:8080"), client.WorkerConfig{
    Queue: "emails",
}, func(ctx context.Context, job *client.Job) error {
    var payload struct{ To, Subject string }
    json.Unmarshal(job.Payload, &payload)
    return sendEmail(payload.To, payload.Subject) // return non-nil to retry
})
```

See `cmd/idemq-worker-demo` for a complete runnable example (it simulates a
flaky downstream email provider to exercise retries and the DLQ).

## Testing

```bash
make test-all    # unit + integration, with a real HTTP server + SQLite file
make test-race   # same, with the Go race detector
```

Integration tests in `test/integration/` run a real `httptest` server backed
by a real SQLite file and drive it with the actual HTTP client package —
including spinning up 10 concurrent worker goroutines against 200 jobs to
prove no job is ever double-delivered, and a "zombie worker" scenario that
proves a stale-leased worker is fenced out after reclaim.

## What's deliberately out of scope (v1)

- **Multi-writer horizontal scaling of the broker itself.** This is a
  single-server broker (SQLite, one writer connection) with many concurrent
  *workers* — the common "one broker, N workers" shape (Sidekiq, SQS,
  Celery). It does not shard writes across multiple broker nodes the way a
  Postgres-backed queue using `SELECT ... FOR UPDATE SKIP LOCKED` across
  several app servers could. See ARCHITECTURE.md for the concrete migration
  path if broker-side write throughput ever becomes the bottleneck.
- **Workflow/orchestration features** (job DAGs, cron scheduling, saga
  compensation) — this is a queue, not a workflow engine.
- **At-most-once delivery mode.** Not offered; at-least-once + idempotency
  keys is the deliberately chosen tradeoff.

## Project layout

```
cmd/idemq-server/       the broker binary
cmd/idemq-worker-demo/  example worker (simulated flaky email send)
internal/store/         SQLite-backed job table: enqueue/claim/ack/nack/reap
internal/backoff/       exponential backoff with full jitter
internal/reaper/        background lease-expiry sweep
internal/api/           HTTP handlers
pkg/client/             worker SDK (Client + RunWorker)
test/integration/       end-to-end tests against a real server
docs/ARCHITECTURE.md    design rationale + bugs found while building this
```

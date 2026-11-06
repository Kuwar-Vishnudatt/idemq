# Architecture

## What problem this solves

Almost every backend eventually needs to run work asynchronously and
reliably: send an email, charge a card, call a flaky third-party API,
process an upload. The moment that work is retried on failure, two hard
problems appear that most tutorials skip:

1. **A worker can fail *after* doing the real-world side effect but
   *before* telling the queue it succeeded** (crash between "charged the
   card" and "ack"). If the queue just redelivers after a timeout, the next
   worker double-charges the card.
2. **A worker can fail *after* claiming a job but before doing anything**
   (crash before it even starts). The queue must notice and give the job to
   someone else — but it must also make sure the *first* worker, if it
   wakes back up later, can't still act on a job it no longer owns.

idemq's entire design is organized around these two problems. It does not
claim to solve (1) in general — no queue can, because the side effect lives
outside the queue's control — but it gives producers the tool that makes (1)
solvable in practice (idempotency keys), and it solves (2) completely and
verifiably (fencing tokens), which is the part a queue actually can
guarantee.

## Why "exactly-once" is the wrong goal, and what the right one is

It's worth being precise here because "exactly-once" is thrown around
loosely and Stripe-style companies explicitly screen for whether a candidate
understands the distinction:

- **Exactly-once *delivery*** (the handler function is invoked exactly one
  time, ever, no matter what fails) is provably impossible in an
  asynchronous system with a worker that can crash mid-execution — you
  cannot distinguish "crashed before running the handler" from "crashed
  after running the handler but before acking" without the handler itself
  recording what it did. This is the same impossibility result underlying
  why TCP can't give you exactly-once message delivery either.
- **Exactly-once *effect*** (the side effect happens once, even though the
  handler might run more than once) *is* achievable, and is what every real
  system actually means when it says "exactly-once." It's achieved by
  making the handler idempotent — using an idempotency key to detect and
  skip a replayed side effect — not by the queue magically guaranteeing
  single delivery.

idemq is explicit about which half it owns:

- The **queue** guarantees a job is never marked `completed` by two
  different claimants at once (see Fencing tokens, below), and gives
  producers a dedupe key so *retried enqueues* don't create duplicate work
  items in the first place.
- The **handler** is responsible for using that same idempotency key (or its
  own) to make the side effect itself safe to run more than once — e.g. "has
  this idempotency key already been charged?" before calling the payment
  API. idemq can't do this part for you because it doesn't know what your
  handler does; it can only make sure *delivery* is honest about being
  at-least-once, not pretend to be something it isn't.

## Fencing tokens: the actual hard part

A naive lease-based queue looks like this: claim a job, get a lease that
expires in N seconds, and if you don't ack in time, the lease expires and
someone else gets it. The bug hiding in that design: **a worker that merely
stalls** (long GC pause, slow disk, a network partition that heals later)
doesn't know its lease expired. It can wake up and ack a job that a second
worker has since claimed and is actively processing — or has already
finished. This is exactly the failure mode Martin Kleppmann's well-known
"How to do distributed locking" post uses to argue that lease timeouts alone
are not a safety mechanism, only a liveness one.

idemq's fix: every `Claim` call issues a strictly monotonically increasing
**lease token** (`internal/store/jobs.go`, backed by a single-row counter
table incremented inside the same transaction as the claim). `Heartbeat`,
`Ack`, and `Nack` all require the caller's token to still match the job's
current token *and* the job to still be `in_flight`:

```sql
UPDATE jobs SET status = 'completed', updated_at = ?
WHERE id = ? AND lease_token = ? AND status = 'in_flight'
```

If `RowsAffected() == 0`, the caller's lease was stale — `ErrFenced`. This
single `WHERE` clause is the entire safety mechanism: it works whether the
staleness is caused by lease expiry + reaper reclaim, or by the job already
having been acked. `TestZombieWorkerIsFencedAfterReclaim` and the store-level
`TestReapExpiredRequeuesAndFencesOriginalWorker` construct the scenario
directly: claim with a very short lease, let it expire and get reclaimed by
a second caller, then prove the *original* caller's ack is rejected.

The reaper (`internal/reaper`) uses the identical mechanism on itself: it
reads a candidate's current lease token, then calls the same fencing-checked
`Nack` path using that token. If the original worker heartbeats or acks in
the narrow window between the reaper's read and write, the reaper's write
simply affects zero rows and is silently skipped — there's no separate
"am I still allowed to reap this" check because the fencing check *is* that
check, reused.

## Why SQLite, and what that choice costs

The storage engine is SQLite (pure-Go driver, `modernc.org/sqlite`, no
cgo), accessed through exactly one write connection
(`db.SetMaxOpenConns(1)`). This is a deliberate, scoped choice, not a
shortcut:

- **What it buys**: real ACID transactions, a durable on-disk WAL, and —
  because all writes from this process funnel through one connection — claim
  /ack/nack/reap are trivially linearizable *within one server process*
  without hand-rolling any locking protocol. The single-writer constraint
  that makes SQLite awkward for multi-process write-heavy apps is exactly
  what makes it easy to reason about correctly here.
- **What it costs**: write throughput is bounded by one SQLite connection
  doing one transaction at a time, and — more importantly — **this design
  assumes exactly one `idemq-server` process per database file.** Running
  two server processes against the same file is not memory-safe from a
  correctness standpoint the way, say, two clients of a Postgres-backed
  queue using `SELECT ... FOR UPDATE SKIP LOCKED` would be: cross-process
  writers aren't serialized by `MaxOpenConns(1)` (that only constrains this
  process), and while the `WHERE status = 'pending'` guard in `Claim`'s
  `UPDATE` would still prevent two processes from claiming the same job
  (the loser's `UPDATE` affects zero rows), you'd see `SQLITE_BUSY` lock
  contention under any real concurrency between the two processes. This is
  reasoned from SQLite's locking model, not discovered by running two
  server processes against one file — doing that is simply out of scope for
  v1, and the manual/integration tests never do it.
- **The horizontal-scaling path, if write throughput ever matters more than
  simplicity**: swap the storage layer for Postgres and the claim query for
  `SELECT id FROM jobs WHERE status='pending' ... FOR UPDATE SKIP LOCKED
  LIMIT 1`, which lets multiple *broker* processes claim concurrently
  without stepping on each other (this is exactly how `que-go` and `river`
  work). The `store.DB` interface boundary in this codebase was kept narrow
  enough that this swap wouldn't touch `internal/api`, `internal/reaper`, or
  `pkg/client` at all — only `internal/store`.

What's *not* scoped out: many **worker** processes, on many machines, hitting
one `idemq-server` over HTTP. That's the actual distribution model here —
see the README diagram — and it's what `test/integration` exercises with
real concurrent HTTP clients, not goroutines sharing memory.

## Backoff: full jitter, not fixed or naive exponential

`internal/backoff` implements the AWS Architecture Blog's "full jitter"
recommendation: the retry delay for attempt N is a *uniform random value in
`[0, min(cap, base * 2^(N-1))]`* — not the exponential value itself, and not
the exponential value plus a little noise on top. The reasoning: if many
jobs fail at the same moment (e.g. the reaper just reclaimed a batch of
leases after a worker fleet restart), fixed or lightly-jittered exponential
backoff keeps them roughly synchronized, so they all retry in near-lockstep
and hit the downstream dependency in correlated bursts. Full jitter spreads
them across the entire interval, which is what actually breaks up a retry
storm. `backoff_test.go` checks the bound (`TestDurationStaysWithinBounds`,
`TestDurationRespectsCapAtHighAttempts`) and that the *ceiling* genuinely
grows with attempt number even though any individual sample can be small
(`TestDurationGrowsWithAttemptBeforeCapping`, which compares maxima over many
samples rather than asserting on a single draw — a naive assertion on one
sample would be flaky by construction given full jitter's whole point is
wide variance).

## Testing strategy: what was actually load-bearing

The riskiest correctness properties here aren't "does the HTTP handler parse
JSON correctly" — they're concurrency and failure properties that are easy
to get subtly wrong and that only show up under real contention:

- **No double delivery under concurrent claims** — many goroutines, real
  HTTP, one server (`TestConcurrentWorkersProcessEachJobExactlyOnce`): 200
  jobs, 10 workers, with an explicit `sync.Map`-based check that no two
  workers are ever processing the same job ID at once, not just a final
  count check.
- **Fencing actually fences** — a worker that stalls past its lease cannot
  ack after reclaim (`TestZombieWorkerIsFencedAfterReclaim`, plus the
  store-level unit test for the same property without the HTTP layer in the
  way, to isolate whether a future bug would be in the storage logic or the
  API layer).
- **Chaos**: `TestChaosNoJobLostOrDuplicatedUnderWorkerCrashes` runs 12
  concurrent workers against 150 jobs where ~35% of claims are simply
  abandoned (no heartbeat, no ack, no nack — simulating a hard crash
  mid-processing), forcing the reaper to do real reclaim work concurrently
  with live claims and acks, then asserts every job was acked *exactly*
  once. This is the test modeled after raft-kv's fault-injection
  discipline: proving the safety property under adversarial conditions
  instead of only under the happy path.
- **Crash recovery**: the server process is killed (DB connection closed
  with no graceful ack) mid-job, and a fresh server process against the same
  SQLite file recovers the orphaned lease (`TestCrashRecovery`).
- **Idempotent enqueue** and **poison-message dead-lettering + replay** are
  each tested at both the store level (fast, isolated) and over real HTTP
  (end-to-end, through the actual client SDK a worker would use).

Honestly: unlike raft-kv (project #1 in this series), this project's riskiest
properties passed their adversarial tests on the first full run, across
repeated `-race` runs. That's not because the problem is easier — fencing
tokens and lease-based reclaim have the exact same class of subtle,
easy-to-get-wrong failure modes as the Raft event loop and linearizability
checker did — but because several of the specific mistakes raft-kv's testing
surfaced the hard way (treating "any failure" as equivalent to "definitely
didn't happen," not distinguishing a stale actor from a current one, not
testing under real injected faults from the start) were known going in and
designed around from the first draft: the fencing-token check and the chaos
test existed in the first version of this codebase rather than being added
after a failure surfaced. The lesson transferred; it didn't need to be
relearned.

## What's deliberately out of scope (v1)

- Multi-process / multi-broker horizontal write scaling (see "Why SQLite,"
  above) — the natural next step is a Postgres + `SKIP LOCKED` backend.
- Workflow orchestration (DAGs of jobs, sagas, cron scheduling beyond a
  simple `delay_ms` on enqueue). idemq is a queue, not a workflow engine.
- At-most-once delivery. Not offered as an option; the design leans fully
  into at-least-once + idempotency keys rather than supporting both modes.
- ReadIndex-style read optimizations or any consensus layer — there's no
  consensus here at all by design; HA for the broker itself (vs. the
  workers, which are already arbitrarily horizontally scalable) would come
  from the Postgres backend swap, not from re-adding Raft.

# goqueue

A production-quality generic durable job queue for Go backed by MySQL (Azure MySQL 8.0.44 / InnoDB).

## Features

- Atomic claim via `SELECT ... FOR UPDATE SKIP LOCKED` — no double-processing under concurrency
- Exponential backoff with jitter on failure
- Idempotency keys for deduplication
- Job priorities (higher = claimed first)
- Delayed scheduling (`WithDelay`)
- Lease TTL + automatic lease reaper goroutine
- Transactional enqueue (`EnqueueTx`) for outbox pattern
- Graceful worker shutdown on context cancellation
- Race-detector clean; each Queue instance owns its own RNG (no global state)

## Quick start

```go
db, _ := sql.Open("mysql", dsn)

q, err := goqueue.New(db, goqueue.Config{
    QueueName: "emails",
})

// Create table (idempotent)
q.AutoMigrate(ctx)

// Enqueue
id, err := q.Enqueue(ctx, json.RawMessage(`{"to":"user@example.com"}`),
    goqueue.WithIdempotencyKey("welcome-user-42"),
    goqueue.WithPriority(5),
)

// Claim and process manually
jobs, err := q.Claim(ctx)
for _, job := range jobs {
    if err := process(job); err != nil {
        q.Fail(ctx, job.ID, err.Error())
    } else {
        q.Complete(ctx, job.ID)
    }
}

// Or use the built-in worker pool
q.RunWorker(ctx, func(ctx context.Context, job goqueue.Job) error {
    return process(job)
})
```

## Config defaults

| Field          | Default              |
|----------------|----------------------|
| ClaimBatchSize | 1                    |
| LeaseTTL       | 5 minutes            |
| MaxAttempts    | 8                    |
| BackoffBase    | 5s                   |
| BackoffMax     | 10m                  |
| BackoffJitter  | 0 (disabled)         |
| ReaperInterval | 30s                  |
| PollInterval   | 2s                   |
| Concurrency    | 1                    |
| WorkerID       | hostname-pid         |
| TableName      | queue_jobs           |

Notes:
- `LeaseTTL`, `WithDelay`, backoff, and `ExtendLease` are implemented in MySQL using `... INTERVAL ? SECOND`, so they have 1-second resolution. Sub-second values are rounded up to the next second.
- `BackoffJitter` is opt-in: `0` disables jitter. When enabled, jitter is sampled uniformly from `[0, BackoffJitter]` and then clamped by `BackoffMax`. `BackoffJitter` must be `<= BackoffMax`.
- `StartReaper` runs one pass immediately on startup, then repeats every `ReaperInterval`. `RunWorker` starts it automatically.

## Backoff formula

```
base  = min(BackoffBase * 2^(attempt-1), BackoffMax)
delay = min(base + rand(0, BackoffJitter), BackoffMax)
```

## State machine

```
pending → claimed → done
claimed → failed  → pending   (attempts < max_attempts, backoff applied)
claimed → failed  → abandoned (attempts >= max_attempts)
claimed → abandoned           (explicit Abandon() call)
reaper: claimed + expired lease → pending  (attempts < max_attempts)
reaper: claimed + expired lease → abandoned (attempts >= max_attempts)
```

## Sentinel errors

```go
goqueue.ErrDuplicateJob    // idempotency key already exists
goqueue.ErrNotClaimed      // job not owned by this worker
goqueue.ErrInvalidPayload  // payload is not valid JSON
goqueue.ErrQueueNameRequired
```

## DDL

```go
q.DDL()         // returns CREATE TABLE IF NOT EXISTS ... string
q.AutoMigrate() // executes DDL against the DB
```

## Integration tests

Requires a MySQL instance. Set `GOQUEUE_TEST_DSN` and run:

```bash
GOQUEUE_TEST_DSN="user:pass@tcp(host:3306)/dbname?parseTime=true" \
  go test -tags integration -race ./...
```

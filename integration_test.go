//go:build integration

package goqueue_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/user/goqueue"
)

var testDB *sql.DB

func TestMain(m *testing.M) {
	dsn := os.Getenv("GOQUEUE_TEST_DSN")
	if dsn == "" {
		fmt.Fprintln(os.Stderr, "GOQUEUE_TEST_DSN not set; skipping integration tests")
		os.Exit(0)
	}
	var err error
	testDB, err = sql.Open("mysql", dsn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "sql.Open: %v\n", err)
		os.Exit(1)
	}
	ctx := context.Background()
	q, err := goqueue.New(testDB, goqueue.Config{QueueName: "migration"})
	if err != nil {
		fmt.Fprintf(os.Stderr, "goqueue.New: %v\n", err)
		os.Exit(1)
	}
	if err := q.AutoMigrate(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "AutoMigrate: %v\n", err)
		os.Exit(1)
	}
	code := m.Run()
	testDB.Close()
	os.Exit(code)
}

func randQueue() string {
	return fmt.Sprintf("test_%d_%d", time.Now().UnixNano(), rand.Intn(10000))
}

func newQ(t *testing.T, opts ...func(*goqueue.Config)) *goqueue.Queue {
	t.Helper()
	cfg := goqueue.Config{QueueName: randQueue()}
	for _, o := range opts {
		o(&cfg)
	}
	q, err := goqueue.New(testDB, cfg)
	require.NoError(t, err)
	return q
}

func payload(t *testing.T, v any) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return b
}

func TestIntegrationBasicRoundTrip(t *testing.T) {
	q := newQ(t)
	ctx := context.Background()

	id, err := q.Enqueue(ctx, payload(t, map[string]string{"hello": "world"}))
	require.NoError(t, err)
	assert.Positive(t, id)

	jobs, err := q.Claim(ctx)
	require.NoError(t, err)
	require.Len(t, jobs, 1)
	assert.Equal(t, id, jobs[0].ID)

	require.NoError(t, q.Complete(ctx, jobs[0].ID))
}

func TestIntegrationClaimEmptyQueue(t *testing.T) {
	q := newQ(t)
	ctx := context.Background()

	jobs, err := q.Claim(ctx)
	require.NoError(t, err)
	assert.Empty(t, jobs)
}

func TestIntegrationConcurrentClaim_NoDoubleProcess(t *testing.T) {
	q := newQ(t, func(c *goqueue.Config) { c.ClaimBatchSize = 1 })
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		_, err := q.Enqueue(ctx, payload(t, i))
		require.NoError(t, err)
	}

	var mu sync.Mutex
	seen := make(map[int64]bool)
	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			jobs, err := q.Claim(ctx)
			if err != nil || len(jobs) == 0 {
				return
			}
			mu.Lock()
			defer mu.Unlock()
			for _, j := range jobs {
				assert.False(t, seen[j.ID], "job %d claimed twice", j.ID)
				seen[j.ID] = true
			}
		}()
	}
	wg.Wait()
	assert.LessOrEqual(t, len(seen), 5)
}

func TestIntegrationIdempotencyKeyDuplicate(t *testing.T) {
	q := newQ(t)
	ctx := context.Background()

	_, err := q.Enqueue(ctx, payload(t, 1), goqueue.WithIdempotencyKey("key1"))
	require.NoError(t, err)

	_, err = q.Enqueue(ctx, payload(t, 2), goqueue.WithIdempotencyKey("key1"))
	assert.ErrorIs(t, err, goqueue.ErrDuplicateJob)
}

func TestIntegrationIdempotencyKeyNullAllowsMultiple(t *testing.T) {
	q := newQ(t)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		_, err := q.Enqueue(ctx, payload(t, i))
		require.NoError(t, err)
	}
}

func TestIntegrationFailRetry(t *testing.T) {
	q := newQ(t)
	ctx := context.Background()

	_, err := q.Enqueue(ctx, payload(t, "x"))
	require.NoError(t, err)

	jobs, err := q.Claim(ctx)
	require.NoError(t, err)
	require.Len(t, jobs, 1)

	require.NoError(t, q.Fail(ctx, jobs[0].ID, "transient error"))

	// Job should now be pending again with a future next_attempt_at.
	var state string
	var nextAttempt time.Time
	row := testDB.QueryRowContext(ctx,
		"SELECT state, next_attempt_at FROM queue_jobs WHERE id=?", jobs[0].ID)
	require.NoError(t, row.Scan(&state, &nextAttempt))
	assert.Equal(t, "pending", state)
	assert.True(t, nextAttempt.After(time.Now()), "next_attempt_at should be in the future")
}

func TestIntegrationFailExhaustedBecomesAbandoned(t *testing.T) {
	q := newQ(t, func(c *goqueue.Config) {
		c.MaxAttempts = 2
		c.BackoffBase = 1 * time.Second
		c.BackoffMax = 2 * time.Second
		c.BackoffJitter = 0
	})
	ctx := context.Background()

	jobID, err := q.Enqueue(ctx, payload(t, "x"), goqueue.WithMaxAttempts(2))
	require.NoError(t, err)

	for i := 0; i < 2; i++ {
		// Wait for next_attempt_at if needed.
		var jobs []goqueue.Job
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			jobs, err = q.Claim(ctx)
			require.NoError(t, err)
			if len(jobs) > 0 {
				break
			}
			time.Sleep(500 * time.Millisecond)
		}
		require.Len(t, jobs, 1)
		require.NoError(t, q.Fail(ctx, jobs[0].ID, "error"))
	}

	var state string
	row := testDB.QueryRowContext(ctx, "SELECT state FROM queue_jobs WHERE id=?", jobID)
	require.NoError(t, row.Scan(&state))
	assert.Equal(t, "abandoned", state)
}

func TestIntegrationAbandonIsTerminal(t *testing.T) {
	q := newQ(t)
	ctx := context.Background()

	_, err := q.Enqueue(ctx, payload(t, "y"))
	require.NoError(t, err)

	jobs, err := q.Claim(ctx)
	require.NoError(t, err)
	require.Len(t, jobs, 1)

	require.NoError(t, q.Abandon(ctx, jobs[0].ID, "manual abandon"))

	more, err := q.Claim(ctx)
	require.NoError(t, err)
	assert.Empty(t, more)
}

func TestIntegrationReaperReclaimsExpiredLease(t *testing.T) {
	q := newQ(t, func(c *goqueue.Config) {
		c.LeaseTTL = 1 * time.Second
		c.ReaperInterval = 100 * time.Millisecond
	})
	ctx := context.Background()

	_, err := q.Enqueue(ctx, payload(t, "reap"))
	require.NoError(t, err)

	jobs, err := q.Claim(ctx)
	require.NoError(t, err)
	require.Len(t, jobs, 1)

	// Wait for lease to expire then manually reap.
	time.Sleep(2 * time.Second)
	require.NoError(t, q.Reap(ctx))

	// Job should be claimable again.
	jobs2, err := q.Claim(ctx)
	require.NoError(t, err)
	assert.Len(t, jobs2, 1)
}

func TestIntegrationCompleteAfterLeaseExpiredReturnsErrNotClaimed(t *testing.T) {
	q := newQ(t, func(c *goqueue.Config) { c.LeaseTTL = 1 * time.Second })
	ctx := context.Background()

	_, err := q.Enqueue(ctx, payload(t, "expire"))
	require.NoError(t, err)

	jobs, err := q.Claim(ctx)
	require.NoError(t, err)
	require.Len(t, jobs, 1)

	time.Sleep(2 * time.Second)
	require.NoError(t, q.Reap(ctx))

	// After reaping, the job is no longer claimed by this worker.
	err = q.Complete(ctx, jobs[0].ID)
	assert.ErrorIs(t, err, goqueue.ErrNotClaimed)
}

func TestIntegrationExtendLeaseProtectsFromReaper(t *testing.T) {
	q := newQ(t, func(c *goqueue.Config) { c.LeaseTTL = 2 * time.Second })
	ctx := context.Background()

	_, err := q.Enqueue(ctx, payload(t, "extend"))
	require.NoError(t, err)

	jobs, err := q.Claim(ctx)
	require.NoError(t, err)
	require.Len(t, jobs, 1)

	// Extend lease before it expires.
	require.NoError(t, q.ExtendLease(ctx, jobs[0].ID, 30*time.Second))

	time.Sleep(3 * time.Second)
	require.NoError(t, q.Reap(ctx))

	// Job should still be claimed (not reclaimed by reaper).
	var state string
	row := testDB.QueryRowContext(ctx, "SELECT state FROM queue_jobs WHERE id=?", jobs[0].ID)
	require.NoError(t, row.Scan(&state))
	assert.Equal(t, "claimed", state)
}

func TestIntegrationEnqueueTxRollback(t *testing.T) {
	q := newQ(t)
	ctx := context.Background()

	tx, err := testDB.BeginTx(ctx, nil)
	require.NoError(t, err)

	_, err = q.EnqueueTx(ctx, tx, payload(t, "rollback"))
	require.NoError(t, err)

	require.NoError(t, tx.Rollback())

	jobs, err := q.Claim(ctx)
	require.NoError(t, err)
	assert.Empty(t, jobs)
}

func TestIntegrationEnqueueTxCommit(t *testing.T) {
	q := newQ(t)
	ctx := context.Background()

	tx, err := testDB.BeginTx(ctx, nil)
	require.NoError(t, err)

	id, err := q.EnqueueTx(ctx, tx, payload(t, "commit"))
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	jobs, err := q.Claim(ctx)
	require.NoError(t, err)
	require.Len(t, jobs, 1)
	assert.Equal(t, id, jobs[0].ID)
}

func TestIntegrationPriorityOrdering(t *testing.T) {
	q := newQ(t)
	ctx := context.Background()

	_, err := q.Enqueue(ctx, payload(t, "low"), goqueue.WithPriority(0))
	require.NoError(t, err)
	_, err = q.Enqueue(ctx, payload(t, "high"), goqueue.WithPriority(10))
	require.NoError(t, err)

	jobs, err := q.Claim(ctx)
	require.NoError(t, err)
	require.Len(t, jobs, 1)

	var v string
	require.NoError(t, json.Unmarshal(jobs[0].Payload, &v))
	assert.Equal(t, "high", v)
}

func TestIntegrationDelayedEnqueue(t *testing.T) {
	q := newQ(t)
	ctx := context.Background()

	_, err := q.Enqueue(ctx, payload(t, "delayed"), goqueue.WithDelay(5*time.Second))
	require.NoError(t, err)

	// Should not be claimable yet.
	jobs, err := q.Claim(ctx)
	require.NoError(t, err)
	assert.Empty(t, jobs)

	// After 5s it should be claimable.
	time.Sleep(6 * time.Second)
	jobs, err = q.Claim(ctx)
	require.NoError(t, err)
	assert.Len(t, jobs, 1)
}

func TestIntegrationMultipleQueuesIsolated(t *testing.T) {
	qa := newQ(t)
	qb := newQ(t)
	ctx := context.Background()

	_, err := qa.Enqueue(ctx, payload(t, "a"))
	require.NoError(t, err)

	jobs, err := qb.Claim(ctx)
	require.NoError(t, err)
	assert.Empty(t, jobs, "queue b should not see queue a's jobs")
}

func TestIntegrationRunWorker(t *testing.T) {
	q := newQ(t, func(c *goqueue.Config) {
		c.PollInterval = 200 * time.Millisecond
		c.Concurrency = 2
	})
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	for i := 0; i < 5; i++ {
		_, err := q.Enqueue(ctx, payload(t, i))
		require.NoError(t, err)
	}

	var mu sync.Mutex
	processed := make(map[int64]bool)

	go q.RunWorker(ctx, func(ctx context.Context, job goqueue.Job) error {
		mu.Lock()
		processed[job.ID] = true
		mu.Unlock()
		return nil
	})

	// Wait until all 5 are processed or timeout.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(processed)
		mu.Unlock()
		if n >= 5 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 5, len(processed))
}

// TestIntegrationWorkerNoBatchLeakWithSlowHandler verifies that RunWorker forces
// single-claim per goroutine even when ClaimBatchSize > 1 is configured.
// Without the ClaimBatchSize=1 override in worker.go, a slow handler would allow
// pre-claimed jobs to expire and get re-queued, causing duplicate processing.
func TestIntegrationWorkerNoBatchLeakWithSlowHandler(t *testing.T) {
	// LeaseTTL=2s, handler=1.5s, ClaimBatchSize=3, Concurrency=1:
	// Without the fix, two jobs are claimed at t=0; job2's lease expires at t=2s
	// but it only starts processing at t=1.5s, causing duplicate processing.
	q := newQ(t, func(c *goqueue.Config) {
		c.PollInterval = 100 * time.Millisecond
		c.ReaperInterval = 300 * time.Millisecond
		c.LeaseTTL = 2 * time.Second
		c.ClaimBatchSize = 3
		c.Concurrency = 1
	})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	_, err := q.Enqueue(ctx, payload(t, 1))
	require.NoError(t, err)
	_, err = q.Enqueue(ctx, payload(t, 2))
	require.NoError(t, err)

	var mu sync.Mutex
	processCount := make(map[int64]int)

	go q.RunWorker(ctx, func(ctx context.Context, job goqueue.Job) error {
		time.Sleep(1500 * time.Millisecond) // slow: exceeds LeaseTTL/2
		mu.Lock()
		processCount[job.ID]++
		mu.Unlock()
		return nil
	})

	// Wait until both jobs are processed.
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(processCount)
		mu.Unlock()
		if n >= 2 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, processCount, 2, "both jobs should be processed")
	for id, count := range processCount {
		assert.Equal(t, 1, count, "job %d should be processed exactly once, got %d", id, count)
	}
}

func TestIntegrationWorkerFailsAndRetries(t *testing.T) {
	q := newQ(t, func(c *goqueue.Config) {
		c.PollInterval = 200 * time.Millisecond
		c.BackoffBase = 1 * time.Second
		c.BackoffMax = 2 * time.Second
		c.BackoffJitter = 0
	})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	_, err := q.Enqueue(ctx, payload(t, "retry-me"), goqueue.WithMaxAttempts(3))
	require.NoError(t, err)

	var mu sync.Mutex
	callCount := 0

	go q.RunWorker(ctx, func(ctx context.Context, job goqueue.Job) error {
		mu.Lock()
		callCount++
		n := callCount
		mu.Unlock()
		if n < 3 {
			return fmt.Errorf("transient failure %d", n)
		}
		return nil
	})

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := callCount
		mu.Unlock()
		if n >= 3 {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	assert.GreaterOrEqual(t, callCount, 3)
}

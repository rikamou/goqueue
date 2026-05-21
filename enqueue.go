package goqueue

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/go-sql-driver/mysql"
)

// enqueueOptions holds optional parameters for an enqueue operation.
type enqueueOptions struct {
	idempotencyKey *string
	priority       int
	maxAttempts    *int
	delay          time.Duration
}

// EnqueueOption is a functional option for Enqueue and EnqueueTx.
type EnqueueOption func(*enqueueOptions)

// WithIdempotencyKey sets a unique key to prevent duplicate jobs.
func WithIdempotencyKey(key string) EnqueueOption {
	return func(o *enqueueOptions) { o.idempotencyKey = &key }
}

// WithPriority sets the job priority (higher values are claimed first).
func WithPriority(p int) EnqueueOption {
	return func(o *enqueueOptions) { o.priority = p }
}

// WithMaxAttempts overrides the queue-level MaxAttempts for this job. Must be >= 1.
func WithMaxAttempts(n int) EnqueueOption {
	return func(o *enqueueOptions) { o.maxAttempts = &n }
}

// WithDelay defers the job's first availability by the given duration.
func WithDelay(d time.Duration) EnqueueOption {
	return func(o *enqueueOptions) { o.delay = d }
}

// execer is satisfied by both *sql.DB and *sql.Tx.
type execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// Enqueue inserts a new job into the queue and returns its ID.
func (q *Queue) Enqueue(ctx context.Context, payload json.RawMessage, opts ...EnqueueOption) (int64, error) {
	return q.enqueue(ctx, q.db, payload, opts...)
}

// EnqueueTx inserts a new job within an existing transaction and returns its ID.
func (q *Queue) EnqueueTx(ctx context.Context, tx *sql.Tx, payload json.RawMessage, opts ...EnqueueOption) (int64, error) {
	return q.enqueue(ctx, tx, payload, opts...)
}

func (q *Queue) enqueue(ctx context.Context, ex execer, payload json.RawMessage, opts ...EnqueueOption) (int64, error) {
	if !json.Valid(payload) {
		return 0, ErrInvalidPayload
	}

	o := &enqueueOptions{}
	for _, opt := range opts {
		opt(o)
	}

	maxAttempts := q.cfg.MaxAttempts
	if o.maxAttempts != nil {
		if *o.maxAttempts < 1 {
			return 0, fmt.Errorf("goqueue: WithMaxAttempts value must be >= 1, got %d", *o.maxAttempts)
		}
		maxAttempts = *o.maxAttempts
	}

	var query string
	var args []any

	if o.delay > 0 {
		delaySecs := durationToSecs(o.delay)
		query = fmt.Sprintf(
			"INSERT INTO %s (queue_name, idempotency_key, payload, state, priority, max_attempts, next_attempt_at) VALUES (?, ?, ?, 'pending', ?, ?, NOW() + INTERVAL ? SECOND)",
			q.table(),
		)
		args = []any{q.cfg.QueueName, o.idempotencyKey, []byte(payload), o.priority, maxAttempts, delaySecs}
	} else {
		query = fmt.Sprintf(
			"INSERT INTO %s (queue_name, idempotency_key, payload, state, priority, max_attempts, next_attempt_at) VALUES (?, ?, ?, 'pending', ?, ?, NOW())",
			q.table(),
		)
		args = []any{q.cfg.QueueName, o.idempotencyKey, []byte(payload), o.priority, maxAttempts}
	}

	res, err := ex.ExecContext(ctx, query, args...)
	if err != nil {
		var mysqlErr *mysql.MySQLError
		if errors.As(err, &mysqlErr) && mysqlErr.Number == 1062 {
			return 0, ErrDuplicateJob
		}
		return 0, fmt.Errorf("goqueue: enqueue: %w", err)
	}

	id, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("goqueue: enqueue last insert id: %w", err)
	}
	return id, nil
}

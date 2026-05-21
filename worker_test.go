package goqueue

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRunWorkerClaimsAndCompletesAJob(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	q, err := New(db, Config{
		QueueName:      "test",
		WorkerID:       "worker-1",
		Concurrency:    1,
		PollInterval:   5 * time.Millisecond,
		ReaperInterval: 1 * time.Hour,
		LeaseTTL:       5 * time.Minute,
	})
	require.NoError(t, err)

	// StartReaper eager pass.
	mock.ExpectBegin()
	mock.ExpectExec(`UPDATE`).
		WithArgs(durationToSecs(q.cfg.BackoffBase), durationToSecs(q.cfg.BackoffMax), q.cfg.QueueName).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`UPDATE`).
		WithArgs(q.cfg.QueueName).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	// Claim flow for one job.
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT id FROM`).
		WithArgs(q.cfg.QueueName, 1).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(10)))
	mock.ExpectExec(`UPDATE .* AND state='pending'`).
		WithArgs(q.cfg.WorkerID, durationToSecs(q.cfg.LeaseTTL), int64(10)).
		WillReturnResult(sqlmock.NewResult(0, 1))

	cols := []string{
		"id", "queue_name", "idempotency_key", "payload", "state", "priority",
		"attempts", "max_attempts", "next_attempt_at", "claimed_by", "claimed_at",
		"claimed_until", "last_error", "created_at", "updated_at", "completed_at",
	}
	now := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	mock.ExpectQuery(`SELECT id, queue_name`).
		WithArgs(int64(10)).
		WillReturnRows(sqlmock.NewRows(cols).AddRow(
			int64(10), "test", nil, []byte(`{"a":1}`), "claimed", 0,
			1, 8, now, q.cfg.WorkerID, now, now.Add(5*time.Minute),
			nil, now, now, nil,
		))
	mock.ExpectCommit()

	// Complete.
	mock.ExpectExec(`UPDATE`).
		WithArgs(int64(10), q.cfg.WorkerID).
		WillReturnResult(sqlmock.NewResult(0, 1))

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	q.RunWorker(ctx, func(ctx context.Context, job Job) error {
		return nil
	})

	assert.NoError(t, mock.ExpectationsWereMet())
}


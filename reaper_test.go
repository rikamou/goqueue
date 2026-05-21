package goqueue

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReapRunsInSingleTransaction(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	q, err := New(db, Config{
		QueueName:    "test",
		WorkerID:     "worker-1",
		BackoffBase:  5 * time.Second,
		BackoffMax:   10 * time.Minute,
		TableName:    "queue_jobs",
		LeaseTTL:     5 * time.Minute,
		PollInterval: 100 * time.Millisecond,
	})
	require.NoError(t, err)

	mock.ExpectBegin()
	mock.ExpectExec(`UPDATE`).
		WithArgs(durationToSecs(q.cfg.BackoffBase), durationToSecs(q.cfg.BackoffMax), q.cfg.QueueName).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`UPDATE`).
		WithArgs(q.cfg.QueueName).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	err = q.Reap(context.Background())
	require.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestReapReturnsErrorAndRollsBackOnFirstUpdateFailure(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	q, err := New(db, Config{QueueName: "test", WorkerID: "worker-1"})
	require.NoError(t, err)

	mock.ExpectBegin()
	mock.ExpectExec(`UPDATE`).
		WithArgs(durationToSecs(q.cfg.BackoffBase), durationToSecs(q.cfg.BackoffMax), q.cfg.QueueName).
		WillReturnError(fmt.Errorf("boom"))
	mock.ExpectRollback()

	err = q.Reap(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "reaper reclaim")
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestReapReturnsErrorAndRollsBackOnSecondUpdateFailure(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	q, err := New(db, Config{QueueName: "test", WorkerID: "worker-1"})
	require.NoError(t, err)

	mock.ExpectBegin()
	mock.ExpectExec(`UPDATE`).
		WithArgs(durationToSecs(q.cfg.BackoffBase), durationToSecs(q.cfg.BackoffMax), q.cfg.QueueName).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`UPDATE`).
		WithArgs(q.cfg.QueueName).
		WillReturnError(fmt.Errorf("boom2"))
	mock.ExpectRollback()

	err = q.Reap(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "reaper abandon")
	assert.NoError(t, mock.ExpectationsWereMet())
}

package goqueue

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExtendLeaseRejectsNonPositiveExtension(t *testing.T) {
	q, _ := newMockDB(t)
	assert.Error(t, q.ExtendLease(context.Background(), 1, 0))
	assert.Error(t, q.ExtendLease(context.Background(), 1, -1*time.Second))
}

func TestExtendLeaseUpdatesClaimedUntil(t *testing.T) {
	q, mock := newMockDB(t)

	mock.ExpectExec(`UPDATE`).
		WithArgs(int64(2), int64(99), q.cfg.WorkerID).
		WillReturnResult(sqlmock.NewResult(0, 1))

	err := q.ExtendLease(context.Background(), 99, 1500*time.Millisecond) // rounds up to 2s
	require.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestExtendLeaseReturnsErrNotClaimedOnZeroRows(t *testing.T) {
	q, mock := newMockDB(t)

	mock.ExpectExec(`UPDATE`).
		WithArgs(int64(1), int64(99), q.cfg.WorkerID).
		WillReturnResult(sqlmock.NewResult(0, 0))

	err := q.ExtendLease(context.Background(), 99, 1*time.Second)
	assert.ErrorIs(t, err, ErrNotClaimed)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestStartReaperRunsAnEagerPassAndStopsOnCancel(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	q, err := New(db, Config{
		QueueName:      "test",
		WorkerID:       "worker-1",
		ReaperInterval: 1 * time.Hour, // ensure only eager pass runs during test
	})
	require.NoError(t, err)

	// Expectations for the eager Reap() call.
	mock.ExpectBegin()
	mock.ExpectExec(`UPDATE`).
		WithArgs(durationToSecs(q.cfg.BackoffBase), durationToSecs(q.cfg.BackoffMax), q.cfg.QueueName).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`UPDATE`).
		WithArgs(q.cfg.QueueName).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	ctx := context.Background()
	cancel, done := q.StartReaper(ctx)

	// Let the goroutine run its eager pass before we cancel it.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if mock.ExpectationsWereMet() == nil {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	cancel()
	<-done

	assert.NoError(t, mock.ExpectationsWereMet())
}

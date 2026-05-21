package goqueue

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEnqueueWithDelayBuildsInsertSQLWithIntervalSeconds(t *testing.T) {
	q, mock := newMockDB(t)

	// 1500ms rounds up to 2s in SQL (INTERVAL ? SECOND).
	mock.ExpectExec(`INSERT INTO`).
		WithArgs(
			"test",
			nil,
			[]byte(`{"a":1}`),
			0,
			8,
			int64(2),
		).
		WillReturnResult(sqlmock.NewResult(42, 1))

	id, err := q.Enqueue(context.Background(), json.RawMessage(`{"a":1}`), WithDelay(1500*time.Millisecond))
	require.NoError(t, err)
	assert.Equal(t, int64(42), id)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestEnqueueDuplicateKeyReturnsErrDuplicateJob(t *testing.T) {
	q, mock := newMockDB(t)

	mock.ExpectExec(`INSERT INTO`).
		WillReturnError(&mysql.MySQLError{Number: 1062, Message: "Duplicate entry"})

	_, err := q.Enqueue(context.Background(), json.RawMessage(`{"a":1}`), WithIdempotencyKey("k"))
	assert.ErrorIs(t, err, ErrDuplicateJob)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestEnqueueTxUsesProvidedTransaction(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	q, err := New(db, Config{QueueName: "test"})
	require.NoError(t, err)

	mock.ExpectBegin()
	tx, err := db.Begin()
	require.NoError(t, err)

	mock.ExpectExec(`INSERT INTO`).
		WithArgs("test", nil, []byte(`{"a":1}`), 0, 8).
		WillReturnResult(sqlmock.NewResult(7, 1))

	id, err := q.EnqueueTx(context.Background(), tx, json.RawMessage(`{"a":1}`))
	require.NoError(t, err)
	assert.Equal(t, int64(7), id)

	// Clean up transaction (EnqueueTx does not commit/rollback).
	mock.ExpectRollback()
	require.NoError(t, tx.Rollback())
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestWithPriorityAndIdempotencyKeyAreApplied(t *testing.T) {
	q, mock := newMockDB(t)

	mock.ExpectExec(`INSERT INTO`).
		WithArgs("test", "k", []byte(`{"a":1}`), 5, 8).
		WillReturnResult(sqlmock.NewResult(1, 1))

	_, err := q.Enqueue(context.Background(), json.RawMessage(`{"a":1}`),
		WithPriority(5),
		WithIdempotencyKey("k"),
	)
	require.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestEnqueueReturnsWrappedErrorForNonDuplicate(t *testing.T) {
	q, mock := newMockDB(t)

	mock.ExpectExec(`INSERT INTO`).
		WillReturnError(errors.New("some db error"))

	_, err := q.Enqueue(context.Background(), json.RawMessage(`{"a":1}`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "goqueue: enqueue")
	assert.NoError(t, mock.ExpectationsWereMet())
}

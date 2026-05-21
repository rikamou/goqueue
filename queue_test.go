package goqueue

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newMockDB(t *testing.T) (*Queue, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	q, err := New(db, Config{QueueName: "test"})
	require.NoError(t, err)
	return q, mock
}

func TestNewReturnsErrorOnEmptyQueueName(t *testing.T) {
	db, _, _ := sqlmock.New()
	_, err := New(db, Config{})
	assert.ErrorIs(t, err, ErrQueueNameRequired)
}

func TestNewReturnsErrorOnNilDB(t *testing.T) {
	_, err := New(nil, Config{QueueName: "test"})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "db must not be nil")
}

func TestNewAppliesDefaults(t *testing.T) {
	db, _, _ := sqlmock.New()
	q, err := New(db, Config{QueueName: "test"})
	require.NoError(t, err)
	assert.Equal(t, 1, q.cfg.ClaimBatchSize)
	assert.Equal(t, 8, q.cfg.MaxAttempts)
	assert.NotEmpty(t, q.cfg.WorkerID)
	assert.Equal(t, "queue_jobs", q.cfg.TableName)
}

func TestNewRejectsNegativeBackoffJitter(t *testing.T) {
	db, _, _ := sqlmock.New()
	_, err := New(db, Config{QueueName: "test", BackoffJitter: -1})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "BackoffJitter")
}

func TestNewZeroBackoffJitterIsValid(t *testing.T) {
	db, _, _ := sqlmock.New()
	q, err := New(db, Config{QueueName: "test", BackoffJitter: 0})
	require.NoError(t, err)
	assert.Equal(t, 0, int(q.cfg.BackoffJitter))
}

func TestNewRejectsJitterExceedingBackoffMax(t *testing.T) {
	db, _, _ := sqlmock.New()
	_, err := New(db, Config{
		QueueName:     "test",
		BackoffMax:    10 * time.Minute,
		BackoffJitter: 11 * time.Minute,
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "BackoffJitter")
}

func TestNewRejectsNullByteQueueName(t *testing.T) {
	db, _, _ := sqlmock.New()
	_, err := New(db, Config{QueueName: "test\x00name"})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "null")
}

func TestNewRejectsNegativeLeaseTTL(t *testing.T) {
	db, _, _ := sqlmock.New()
	_, err := New(db, Config{QueueName: "test", LeaseTTL: -1 * time.Second})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "LeaseTTL")
}

func TestNewRejectsNullByteWorkerID(t *testing.T) {
	db, _, _ := sqlmock.New()
	_, err := New(db, Config{QueueName: "test", WorkerID: "w\x00id"})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "worker ID")
}

func TestEnqueueInvalidJSONReturnsError(t *testing.T) {
	q, _ := newMockDB(t)
	_, err := q.Enqueue(context.Background(), json.RawMessage("not-json"))
	assert.ErrorIs(t, err, ErrInvalidPayload)
}

func TestEnqueueInvalidMaxAttempts(t *testing.T) {
	q, _ := newMockDB(t)
	_, err := q.Enqueue(context.Background(), json.RawMessage(`{}`), WithMaxAttempts(0))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "WithMaxAttempts")

	_, err = q.Enqueue(context.Background(), json.RawMessage(`{}`), WithMaxAttempts(-5))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "WithMaxAttempts")
}

func TestEnqueueBuildsInsertSQL(t *testing.T) {
	q, mock := newMockDB(t)

	mock.ExpectExec(`INSERT INTO`).
		WithArgs(
			"test",            // queue_name
			nil,               // idempotency_key (nil = no key)
			[]byte(`{"a":1}`), // payload
			0,                 // priority
			8,                 // max_attempts
		).
		WillReturnResult(sqlmock.NewResult(42, 1))

	id, err := q.Enqueue(context.Background(), json.RawMessage(`{"a":1}`))
	require.NoError(t, err)
	assert.Equal(t, int64(42), id)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestClaimReturnsEmptySliceOnNoRows(t *testing.T) {
	q, mock := newMockDB(t)

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT id FROM`).
		WithArgs("test", 1).
		WillReturnRows(sqlmock.NewRows([]string{"id"}))
	mock.ExpectRollback()

	jobs, err := q.Claim(context.Background())
	require.NoError(t, err)
	assert.Equal(t, []Job{}, jobs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestCompleteUpdatesState(t *testing.T) {
	q, mock := newMockDB(t)

	mock.ExpectExec(`UPDATE`).
		WithArgs(int64(99), q.cfg.WorkerID).
		WillReturnResult(sqlmock.NewResult(0, 1))

	err := q.Complete(context.Background(), 99)
	require.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestCompleteReturnsErrNotClaimedOnZeroRows(t *testing.T) {
	q, mock := newMockDB(t)

	mock.ExpectExec(`UPDATE`).
		WithArgs(int64(99), q.cfg.WorkerID).
		WillReturnResult(sqlmock.NewResult(0, 0))

	err := q.Complete(context.Background(), 99)
	assert.ErrorIs(t, err, ErrNotClaimed)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestCompleteReturnsWrappedErrorOnExecFailure(t *testing.T) {
	q, mock := newMockDB(t)

	mock.ExpectExec(`UPDATE`).
		WithArgs(int64(99), q.cfg.WorkerID).
		WillReturnError(assert.AnError)

	err := q.Complete(context.Background(), 99)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "goqueue: complete")
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestFailRetryable(t *testing.T) {
	q, mock := newMockDB(t)

	// attempts=1, max_attempts=8 → retryable: UPDATE to pending with backoff
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT attempts, max_attempts FROM`).
		WithArgs(int64(10), q.cfg.WorkerID).
		WillReturnRows(sqlmock.NewRows([]string{"attempts", "max_attempts"}).AddRow(1, 8))
	// UPDATE args: delaySecs, errMsg, jobID (no claimed_by — row already locked)
	mock.ExpectExec(`UPDATE`).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	err := q.Fail(context.Background(), 10, "some error")
	require.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestFailSelectReturnsWrappedError(t *testing.T) {
	q, mock := newMockDB(t)

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT attempts, max_attempts FROM`).
		WithArgs(int64(10), q.cfg.WorkerID).
		WillReturnError(assert.AnError)
	mock.ExpectRollback()

	err := q.Fail(context.Background(), 10, "some error")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "goqueue: fail select")
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestFailTerminal(t *testing.T) {
	q, mock := newMockDB(t)

	// attempts=8, max_attempts=8 → terminal: UPDATE to abandoned
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT attempts, max_attempts FROM`).
		WithArgs(int64(10), q.cfg.WorkerID).
		WillReturnRows(sqlmock.NewRows([]string{"attempts", "max_attempts"}).AddRow(8, 8))
	// UPDATE args: errMsg, jobID (no claimed_by — row already locked)
	mock.ExpectExec(`UPDATE`).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	err := q.Fail(context.Background(), 10, "fatal error")
	require.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestAbandonReturnsErrNotClaimedOnZeroRows(t *testing.T) {
	q, mock := newMockDB(t)

	mock.ExpectExec(`UPDATE`).
		WithArgs("explicit abandon", int64(7), q.cfg.WorkerID).
		WillReturnResult(sqlmock.NewResult(0, 0))

	err := q.Abandon(context.Background(), 7, "explicit abandon")
	assert.ErrorIs(t, err, ErrNotClaimed)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestAbandonSetsTerminalState(t *testing.T) {
	q, mock := newMockDB(t)

	mock.ExpectExec(`UPDATE`).
		WithArgs("explicit abandon", int64(7), q.cfg.WorkerID).
		WillReturnResult(sqlmock.NewResult(0, 1))

	err := q.Abandon(context.Background(), 7, "explicit abandon")
	require.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestAutoMigrateReturnsWrappedError(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	q, err := New(db, Config{QueueName: "test"})
	require.NoError(t, err)

	mock.ExpectExec(`CREATE TABLE IF NOT EXISTS`).
		WillReturnError(assert.AnError)

	err = q.AutoMigrate(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "goqueue: AutoMigrate")
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestDurationToSecs(t *testing.T) {
	assert.Equal(t, int64(0), durationToSecs(0))
	assert.Equal(t, int64(1), durationToSecs(1)) // 1ns rounds up to 1s
	assert.Equal(t, int64(1), durationToSecs(1e9))
	assert.Equal(t, int64(2), durationToSecs(1e9+1)) // 1.000000001s rounds up to 2s
	assert.Equal(t, int64(0), durationToSecs(-1))
}

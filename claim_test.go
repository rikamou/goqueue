package goqueue

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClaimSelectsIDsUpdatesAndFetchesRows(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	q, err := New(db, Config{
		QueueName:      "test",
		WorkerID:       "worker-1",
		ClaimBatchSize: 2,
		LeaseTTL:       5 * time.Minute,
	})
	require.NoError(t, err)

	mock.ExpectBegin()

	// 1) Lock candidate IDs.
	mock.ExpectQuery(`SELECT id FROM`).
		WithArgs("test", 2).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(10)).AddRow(int64(11)))

	// 2) Claim those IDs.
	mock.ExpectExec(`UPDATE .* AND state='pending'`).
		WithArgs(q.cfg.WorkerID, durationToSecs(q.cfg.LeaseTTL), int64(10), int64(11)).
		WillReturnResult(sqlmock.NewResult(0, 2))

	// 3) Re-fetch full job rows so DB-authored columns are authoritative.
	cols := []string{
		"id", "queue_name", "idempotency_key", "payload", "state", "priority",
		"attempts", "max_attempts", "next_attempt_at", "claimed_by", "claimed_at",
		"claimed_until", "last_error", "created_at", "updated_at", "completed_at",
	}

	claimedAt1 := time.Date(2020, 1, 1, 0, 0, 1, 0, time.UTC)
	claimedAt2 := time.Date(2020, 1, 1, 0, 0, 2, 0, time.UTC)
	claimedUntil1 := time.Date(2020, 1, 1, 0, 5, 1, 0, time.UTC)
	claimedUntil2 := time.Date(2020, 1, 1, 0, 5, 2, 0, time.UTC)
	createdAt := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	updatedAt1 := time.Date(2020, 1, 1, 0, 0, 3, 0, time.UTC)
	updatedAt2 := time.Date(2020, 1, 1, 0, 0, 4, 0, time.UTC)

	mock.ExpectQuery(`SELECT id, queue_name`).
		WithArgs(int64(10), int64(11)).
		WillReturnRows(
			sqlmock.NewRows(cols).
				AddRow(
					int64(10), "test", nil, []byte(`{"a":1}`), "claimed", 0,
					1, 8, createdAt, q.cfg.WorkerID, claimedAt1,
					claimedUntil1, nil, createdAt, updatedAt1, nil,
				).
				AddRow(
					int64(11), "test", nil, []byte(`{"b":2}`), "claimed", 0,
					1, 8, createdAt, q.cfg.WorkerID, claimedAt2,
					claimedUntil2, nil, createdAt, updatedAt2, nil,
				),
		)

	mock.ExpectCommit()

	jobs, err := q.Claim(context.Background())
	require.NoError(t, err)
	require.Len(t, jobs, 2)

	assert.Equal(t, int64(10), jobs[0].ID)
	assert.Equal(t, int64(11), jobs[1].ID)
	assert.Equal(t, "claimed", jobs[0].State)
	assert.Equal(t, "claimed", jobs[1].State)

	require.NotNil(t, jobs[0].ClaimedBy)
	require.NotNil(t, jobs[0].ClaimedAt)
	require.NotNil(t, jobs[0].ClaimedUntil)
	require.NotNil(t, jobs[1].ClaimedBy)
	require.NotNil(t, jobs[1].ClaimedAt)
	require.NotNil(t, jobs[1].ClaimedUntil)

	assert.Equal(t, q.cfg.WorkerID, *jobs[0].ClaimedBy)
	assert.Equal(t, q.cfg.WorkerID, *jobs[1].ClaimedBy)
	assert.Equal(t, claimedAt1, *jobs[0].ClaimedAt)
	assert.Equal(t, claimedAt2, *jobs[1].ClaimedAt)
	assert.Equal(t, claimedUntil1, *jobs[0].ClaimedUntil)
	assert.Equal(t, claimedUntil2, *jobs[1].ClaimedUntil)

	// Ensure pointers are not aliased across jobs.
	assert.NotSame(t, jobs[0].ClaimedAt, jobs[1].ClaimedAt)
	assert.NotSame(t, jobs[0].ClaimedUntil, jobs[1].ClaimedUntil)

	assert.Equal(t, updatedAt1, jobs[0].UpdatedAt)
	assert.Equal(t, updatedAt2, jobs[1].UpdatedAt)

	assert.NoError(t, mock.ExpectationsWereMet())
}

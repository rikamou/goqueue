package goqueue

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// Complete marks a claimed job as done.
func (q *Queue) Complete(ctx context.Context, jobID int64) error {
	query := fmt.Sprintf(
		"UPDATE %s SET state='done', completed_at=NOW() WHERE id=? AND claimed_by=? AND state='claimed'",
		q.table(),
	)
	res, err := q.db.ExecContext(ctx, query, jobID, q.cfg.WorkerID)
	if err != nil {
		return fmt.Errorf("goqueue: complete: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("goqueue: complete rows affected: %w", err)
	}
	if n == 0 {
		return ErrNotClaimed
	}
	return nil
}

// Fail records an error on a claimed job. If the job has remaining attempts it
// is re-queued with exponential backoff; otherwise it is permanently abandoned.
func (q *Queue) Fail(ctx context.Context, jobID int64, errMsg string) error {
	tx, err := q.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("goqueue: fail begin tx: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck

	var attempts, maxAttempts int
	selSQL := fmt.Sprintf(
		"SELECT attempts, max_attempts FROM %s WHERE id=? AND claimed_by=? AND state='claimed' FOR UPDATE",
		q.table(),
	)
	err = tx.QueryRowContext(ctx, selSQL, jobID, q.cfg.WorkerID).Scan(&attempts, &maxAttempts)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotClaimed
		}
		return fmt.Errorf("goqueue: fail select: %w", err)
	}

	// The row is already locked by SELECT FOR UPDATE above; WHERE id=? is sufficient.
	if attempts < maxAttempts {
		delaySecs := durationToSecs(q.calcBackoff(attempts))
		query := fmt.Sprintf(
			"UPDATE %s SET state='pending', claimed_by=NULL, claimed_at=NULL, claimed_until=NULL, next_attempt_at=NOW() + INTERVAL ? SECOND, last_error=? WHERE id=?",
			q.table(),
		)
		if _, err := tx.ExecContext(ctx, query, delaySecs, errMsg, jobID); err != nil {
			return fmt.Errorf("goqueue: fail retry: %w", err)
		}
	} else {
		query := fmt.Sprintf(
			"UPDATE %s SET state='abandoned', claimed_by=NULL, claimed_at=NULL, claimed_until=NULL, completed_at=NOW(), last_error=? WHERE id=?",
			q.table(),
		)
		if _, err := tx.ExecContext(ctx, query, errMsg, jobID); err != nil {
			return fmt.Errorf("goqueue: fail abandon: %w", err)
		}
	}
	return tx.Commit()
}

// Abandon immediately moves a claimed job to the terminal abandoned state.
func (q *Queue) Abandon(ctx context.Context, jobID int64, errMsg string) error {
	query := fmt.Sprintf(
		"UPDATE %s SET state='abandoned', claimed_by=NULL, claimed_at=NULL, claimed_until=NULL, completed_at=NOW(), last_error=? WHERE id=? AND claimed_by=? AND state='claimed'",
		q.table(),
	)
	res, err := q.db.ExecContext(ctx, query, errMsg, jobID, q.cfg.WorkerID)
	if err != nil {
		return fmt.Errorf("goqueue: abandon: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("goqueue: abandon rows affected: %w", err)
	}
	if n == 0 {
		return ErrNotClaimed
	}
	return nil
}

// ExtendLease extends the lease deadline on a claimed job. The new deadline is
// max(current claimed_until, NOW()) + extension so it can never shorten the lease.
// extension must be positive.
func (q *Queue) ExtendLease(ctx context.Context, jobID int64, extension time.Duration) error {
	if extension <= 0 {
		return fmt.Errorf("goqueue: extension must be positive, got %v", extension)
	}
	extSecs := durationToSecs(extension)
	query := fmt.Sprintf(
		"UPDATE %s SET claimed_until=GREATEST(claimed_until, NOW()) + INTERVAL ? SECOND WHERE id=? AND claimed_by=? AND state='claimed'",
		q.table(),
	)
	res, err := q.db.ExecContext(ctx, query, extSecs, jobID, q.cfg.WorkerID)
	if err != nil {
		return fmt.Errorf("goqueue: extend lease: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("goqueue: extend lease rows affected: %w", err)
	}
	if n == 0 {
		return ErrNotClaimed
	}
	return nil
}

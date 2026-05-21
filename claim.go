package goqueue

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
)

func (q *Queue) Claim(ctx context.Context) ([]Job, error) {
	tx, err := q.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("goqueue: claim begin tx: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck

	selectSQL := fmt.Sprintf(
		"SELECT id FROM %s WHERE queue_name=? AND state='pending' AND next_attempt_at<=NOW() ORDER BY priority DESC, id ASC LIMIT ? FOR UPDATE SKIP LOCKED",
		q.table(),
	)

	rows, err := tx.QueryContext(ctx, selectSQL, q.cfg.QueueName, q.cfg.ClaimBatchSize)
	if err != nil {
		return nil, fmt.Errorf("goqueue: claim select: %w", err)
	}

	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, fmt.Errorf("goqueue: claim scan id: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("goqueue: claim rows close: %w", err)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("goqueue: claim rows err: %w", err)
	}

	if len(ids) == 0 {
		return []Job{}, nil
	}

	placeholders := make([]string, len(ids))
	args := make([]any, 0, len(ids)+2)
	args = append(args, q.cfg.WorkerID, durationToSecs(q.cfg.LeaseTTL))
	for i, id := range ids {
		placeholders[i] = "?"
		args = append(args, id)
	}

	updateSQL := fmt.Sprintf(
		"UPDATE %s SET state='claimed', claimed_by=?, claimed_at=NOW(), claimed_until=NOW() + INTERVAL ? SECOND, attempts=attempts+1 WHERE id IN (%s) AND state='pending'",
		q.table(),
		strings.Join(placeholders, ","),
	)

	if _, err := tx.ExecContext(ctx, updateSQL, args...); err != nil {
		return nil, fmt.Errorf("goqueue: claim update: %w", err)
	}

	// Re-fetch claimed rows so DB-authored fields (claimed_at/claimed_until/updated_at)
	// reflect the actual values stored in MySQL.
	fetchPlaceholders := make([]string, len(ids))
	fetchArgs := make([]any, len(ids))
	for i, id := range ids {
		fetchPlaceholders[i] = "?"
		fetchArgs[i] = id
	}
	fetchSQL := fmt.Sprintf(
		"SELECT id, queue_name, idempotency_key, payload, state, priority, attempts, max_attempts, next_attempt_at, claimed_by, claimed_at, claimed_until, last_error, created_at, updated_at, completed_at FROM %s WHERE id IN (%s) ORDER BY priority DESC, id ASC",
		q.table(),
		strings.Join(fetchPlaceholders, ","),
	)

	jobRows, err := tx.QueryContext(ctx, fetchSQL, fetchArgs...)
	if err != nil {
		return nil, fmt.Errorf("goqueue: claim fetch: %w", err)
	}
	defer jobRows.Close()

	jobs, err := scanJobs(jobRows)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("goqueue: claim commit: %w", err)
	}
	return jobs, nil
}

func scanJobs(rows *sql.Rows) ([]Job, error) {
	var jobs []Job
	for rows.Next() {
		var j Job
		var payload []byte
		err := rows.Scan(
			&j.ID, &j.QueueName, &j.IdempotencyKey, &payload, &j.State, &j.Priority,
			&j.Attempts, &j.MaxAttempts, &j.NextAttemptAt, &j.ClaimedBy, &j.ClaimedAt,
			&j.ClaimedUntil, &j.LastError, &j.CreatedAt, &j.UpdatedAt, &j.CompletedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("goqueue: scan job: %w", err)
		}
		j.Payload = json.RawMessage(payload)
		jobs = append(jobs, j)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("goqueue: scan jobs err: %w", err)
	}
	return jobs, nil
}

package goqueue

import (
	"context"
	"fmt"
	"time"
)

// Reap runs a single reaper pass: expired leases are re-queued with per-job
// exponential backoff, or abandoned if max_attempts is exhausted.
func (q *Queue) Reap(ctx context.Context) error {
	baseSecs := durationToSecs(q.cfg.BackoffBase)
	maxSecs := durationToSecs(q.cfg.BackoffMax)

	// Uses the same exponential formula as calcBackoff but without jitter,
	// since per-row random values cannot be added in a single SQL UPDATE.
	reclaimSQL := fmt.Sprintf(
		"UPDATE %s SET state='pending', claimed_by=NULL, claimed_at=NULL, claimed_until=NULL, next_attempt_at=NOW() + INTERVAL LEAST(? * POW(2, attempts-1), ?) SECOND WHERE queue_name=? AND state='claimed' AND claimed_until < NOW() AND attempts < max_attempts",
		q.table(),
	)
	if _, err := q.db.ExecContext(ctx, reclaimSQL, baseSecs, maxSecs, q.cfg.QueueName); err != nil {
		return fmt.Errorf("goqueue: reaper reclaim: %w", err)
	}

	abandonSQL := fmt.Sprintf(
		"UPDATE %s SET state='abandoned', claimed_by=NULL, claimed_at=NULL, claimed_until=NULL, completed_at=NOW() WHERE queue_name=? AND state='claimed' AND claimed_until < NOW() AND attempts >= max_attempts",
		q.table(),
	)
	if _, err := q.db.ExecContext(ctx, abandonSQL, q.cfg.QueueName); err != nil {
		return fmt.Errorf("goqueue: reaper abandon: %w", err)
	}

	return nil
}

// StartReaper launches a background goroutine that periodically reclaims
// expired leases. It returns a cancel function and a done channel that closes
// when the goroutine has fully stopped — use these together for graceful shutdown.
func (q *Queue) StartReaper(ctx context.Context) (cancel func(), done <-chan struct{}) {
	ctx, cancelFn := context.WithCancel(ctx)
	doneCh := make(chan struct{})
	go func() {
		defer close(doneCh)
		ticker := time.NewTicker(q.cfg.ReaperInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := q.Reap(ctx); err != nil && q.cfg.OnReaperError != nil {
					q.cfg.OnReaperError(err)
				}
			}
		}
	}()
	return cancelFn, doneCh
}

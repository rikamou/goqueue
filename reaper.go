package goqueue

import (
	"context"
	"fmt"
	"time"
)

// Reap runs a single reaper pass. Both UPDATEs run in one transaction so they
// cannot interleave with a concurrent Reap or Claim.
func (q *Queue) Reap(ctx context.Context) error {
	tx, err := q.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("goqueue: reaper begin tx: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck

	baseSecs := durationToSecs(q.cfg.BackoffBase)
	maxSecs := durationToSecs(q.cfg.BackoffMax)

	// Note: this rescheduling is intentionally deterministic and does not apply jitter.
	// This allows reclaiming many rows with a single UPDATE, but can cause correlated
	// retries after a mass lease expiry (e.g. crash recovery).
	reclaimSQL := fmt.Sprintf(
		"UPDATE %s SET state='pending', claimed_by=NULL, claimed_at=NULL, claimed_until=NULL, next_attempt_at=NOW() + INTERVAL LEAST(? * POW(2, attempts-1), ?) SECOND WHERE queue_name=? AND state='claimed' AND claimed_until < NOW() AND attempts < max_attempts",
		q.table(),
	)
	if _, err := tx.ExecContext(ctx, reclaimSQL, baseSecs, maxSecs, q.cfg.QueueName); err != nil {
		return fmt.Errorf("goqueue: reaper reclaim: %w", err)
	}

	abandonSQL := fmt.Sprintf(
		"UPDATE %s SET state='abandoned', claimed_by=NULL, claimed_at=NULL, claimed_until=NULL, completed_at=NOW() WHERE queue_name=? AND state='claimed' AND claimed_until < NOW() AND attempts >= max_attempts",
		q.table(),
	)
	if _, err := tx.ExecContext(ctx, abandonSQL, q.cfg.QueueName); err != nil {
		return fmt.Errorf("goqueue: reaper abandon: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("goqueue: reaper commit: %w", err)
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
		// Run once immediately so leases expired during a prior crash are recovered before the first tick.
		if err := q.Reap(ctx); err != nil && q.cfg.OnReaperError != nil {
			q.cfg.OnReaperError(err)
		}
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

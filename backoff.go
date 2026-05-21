package goqueue

import "time"

// calcBackoff returns the delay before the next attempt using saturating
// exponential backoff with optional jitter.
// delay = min(BackoffBase * 2^(attempt-1), BackoffMax) + rand(0, BackoffJitter)
// Uses integer arithmetic to avoid float64 overflow on large attempt counts.
func (q *Queue) calcBackoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	// Saturating doubling: stop as soon as we reach BackoffMax to avoid overflow.
	base := q.cfg.BackoffBase
	for i := 1; i < attempt; i++ {
		if base >= q.cfg.BackoffMax/2 {
			base = q.cfg.BackoffMax
			break
		}
		base *= 2
	}
	if base > q.cfg.BackoffMax {
		base = q.cfg.BackoffMax
	}
	var jitter time.Duration
	if q.cfg.BackoffJitter > 0 {
		jitter = time.Duration(q.rng.Int63n(int64(q.cfg.BackoffJitter) + 1))
	}
	delay := base + jitter
	if delay > q.cfg.BackoffMax || delay < 0 {
		delay = q.cfg.BackoffMax
	}
	return delay
}

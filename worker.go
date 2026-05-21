package goqueue

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// HandlerFunc is called with each claimed job. Return a non-nil error to fail the job.
type HandlerFunc func(ctx context.Context, job Job) error

// RunWorker starts a worker pool that polls for jobs and calls handler for each.
// It blocks until ctx is cancelled. The reaper is started automatically and
// stopped when the worker exits.
//
// On each poll tick a single Claim call is issued with a batch size equal to
// the number of available concurrency slots, eliminating the per-slot DB round-
// trips that occur when the queue is idle.
func (q *Queue) RunWorker(ctx context.Context, handler HandlerFunc) {
	cancelReaper, reaperDone := q.StartReaper(ctx)
	defer func() {
		cancelReaper()
		<-reaperDone
	}()

	var wg sync.WaitGroup
	sem := make(chan struct{}, q.cfg.Concurrency)

	ticker := time.NewTicker(q.cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			wg.Wait()
			return
		case <-ticker.C:
			// Acquire semaphore slots before claiming: a blocking send after claim
			// would let leases tick down while unprocessed, causing the reaper to
			// reclaim and re-run them (duplicate processing).
			acquired := 0
			for i := 0; i < q.cfg.Concurrency; i++ {
				select {
				case sem <- struct{}{}:
					acquired++
				default:
				}
			}
			if acquired == 0 {
				continue
			}
			batchCfg := q.cfg
			batchCfg.ClaimBatchSize = acquired
			batchQ := &Queue{db: q.db, cfg: batchCfg, rng: q.rng}
			jobs, err := batchQ.Claim(ctx)
			if err != nil || len(jobs) == 0 {
				for i := 0; i < acquired; i++ {
					<-sem
				}
				continue
			}
			// Release slots not consumed by actual jobs.
			for i := len(jobs); i < acquired; i++ {
				<-sem
			}
			for _, job := range jobs {
				j := job
				wg.Add(1)
				go func() {
					defer wg.Done()
					defer func() { <-sem }()
					defer func() {
						if r := recover(); r != nil {
							msg := fmt.Sprintf("panic: %v", r)
							failCtx, failCancel := context.WithTimeout(context.Background(), 30*time.Second)
							defer failCancel()
							if ferr := q.Fail(failCtx, j.ID, msg); ferr != nil && q.cfg.OnError != nil {
								q.cfg.OnError(j, "fail", ferr)
							} else if ferr == nil && j.Attempts >= j.MaxAttempts && q.cfg.OnAbandoned != nil {
								callSafe(func() { q.cfg.OnAbandoned(j) })
							}
						}
					}()
					// Detach from worker ctx so Fail/Complete can write to DB even
					// after ctx is cancelled on graceful shutdown.
					opCtx, opCancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
					defer opCancel()
					if herr := handler(ctx, j); herr != nil {
						if ferr := q.Fail(opCtx, j.ID, herr.Error()); ferr != nil && q.cfg.OnError != nil {
							q.cfg.OnError(j, "fail", ferr)
						} else if ferr == nil && j.Attempts >= j.MaxAttempts && q.cfg.OnAbandoned != nil {
							callSafe(func() { q.cfg.OnAbandoned(j) })
						}
					} else {
						if cerr := q.Complete(opCtx, j.ID); cerr != nil && q.cfg.OnError != nil {
							q.cfg.OnError(j, "complete", cerr)
						}
					}
				}()
			}
		}
	}
}

// callSafe calls fn and recovers any panic it raises, preventing user-supplied
// callbacks (e.g. OnAbandoned) from crashing the worker goroutine.
func callSafe(fn func()) {
	defer func() { recover() }()
	fn()
}

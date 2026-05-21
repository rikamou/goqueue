package goqueue

import (
	"context"
	"sync"
	"time"
)

// HandlerFunc is called with each claimed job. Return a non-nil error to fail the job.
type HandlerFunc func(ctx context.Context, job Job) error

// RunWorker starts a worker pool that polls for jobs and calls handler for each.
// It blocks until ctx is cancelled. The reaper is started automatically and
// stopped when the worker exits.
//
// RunWorker always claims one job per goroutine regardless of ClaimBatchSize,
// preventing lease expiry on unprocessed jobs queued within a batch.
func (q *Queue) RunWorker(ctx context.Context, handler HandlerFunc) {
	cancelReaper, reaperDone := q.StartReaper(ctx)
	defer func() {
		cancelReaper()
		<-reaperDone
	}()

	var wg sync.WaitGroup
	sem := make(chan struct{}, q.cfg.Concurrency)

	singleCfg := q.cfg
	singleCfg.ClaimBatchSize = 1
	single := &Queue{db: q.db, cfg: singleCfg, rng: q.rng}

	ticker := time.NewTicker(q.cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			wg.Wait()
			return
		case <-ticker.C:
			for i := 0; i < q.cfg.Concurrency; i++ {
				select {
				case sem <- struct{}{}:
				default:
					continue
				}
				wg.Add(1)
				go func() {
					defer wg.Done()
					defer func() { <-sem }()

					jobs, err := single.Claim(ctx)
					if err != nil || len(jobs) == 0 {
						return
					}
					job := jobs[0]
					if herr := handler(ctx, job); herr != nil {
						if ferr := q.Fail(ctx, job.ID, herr.Error()); ferr != nil && q.cfg.OnError != nil {
							q.cfg.OnError(job, "fail", ferr)
						}
					} else {
						if cerr := q.Complete(ctx, job.ID); cerr != nil && q.cfg.OnError != nil {
							q.cfg.OnError(job, "complete", cerr)
						}
					}
				}()
			}
		}
	}
}

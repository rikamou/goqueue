//go:build integration

package goqueue_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/rikamou/goqueue"
)

// EmailPayload is an example typed job payload for an email queue.
type EmailPayload struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
	Body    string `json:"body"`
}

// openExampleDB opens a MySQL connection from the GOQUEUE_DSN environment variable.
func openExampleDB() *sql.DB {
	dsn := os.Getenv("GOQUEUE_DSN")
	if dsn == "" {
		log.Fatal("GOQUEUE_DSN not set")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("sql.Open: %v", err)
	}
	return db
}

// ExampleQueue_AutoMigrate shows how to create the queue table at startup.
func ExampleQueue_AutoMigrate() {
	db := openExampleDB()
	defer db.Close()

	q, err := goqueue.New(db, goqueue.Config{
		QueueName: "emails",
	})
	if err != nil {
		log.Fatalf("goqueue.New: %v", err)
	}

	ctx := context.Background()

	// AutoMigrate runs CREATE TABLE IF NOT EXISTS. Call it once at startup
	// before enqueueing or consuming jobs.
	if err := q.AutoMigrate(ctx); err != nil {
		log.Fatalf("AutoMigrate: %v", err)
	}

	// DDL() returns the raw CREATE TABLE statement for manual schema management.
	fmt.Println(q.DDL())
}

// ExampleQueue_Enqueue shows the full range of enqueue options.
func ExampleQueue_Enqueue() {
	db := openExampleDB()
	defer db.Close()

	q, err := goqueue.New(db, goqueue.Config{
		QueueName:   "emails",
		MaxAttempts: 5,
	})
	if err != nil {
		log.Fatalf("goqueue.New: %v", err)
	}

	ctx := context.Background()

	// Basic enqueue.
	payload, _ := json.Marshal(EmailPayload{To: "alice@example.com", Subject: "Hello", Body: "Hi!"})
	id, err := q.Enqueue(ctx, payload)
	if err != nil {
		log.Fatalf("Enqueue: %v", err)
	}
	fmt.Printf("enqueued job %d\n", id)

	// High-priority job — claimed before lower-priority jobs.
	urgent, _ := json.Marshal(EmailPayload{To: "ops@example.com", Subject: "Alert", Body: "Down!"})
	_, err = q.Enqueue(ctx, urgent, goqueue.WithPriority(10))
	if err != nil {
		log.Fatalf("Enqueue (priority): %v", err)
	}

	// Idempotent enqueue — duplicate calls with the same key return ErrDuplicateJob.
	confirm, _ := json.Marshal(EmailPayload{To: "bob@example.com", Subject: "Confirm"})
	_, err = q.Enqueue(ctx, confirm, goqueue.WithIdempotencyKey("confirm-bob-2024"))
	if errors.Is(err, goqueue.ErrDuplicateJob) {
		fmt.Println("already enqueued (idempotent)")
	} else if err != nil {
		log.Fatalf("Enqueue (idempotency): %v", err)
	}

	// Delayed job — not eligible for claim until 30 minutes from now.
	reminder, _ := json.Marshal(EmailPayload{To: "carol@example.com", Subject: "Reminder"})
	_, err = q.Enqueue(ctx, reminder,
		goqueue.WithDelay(30*time.Minute),
		goqueue.WithMaxAttempts(3),
	)
	if err != nil {
		log.Fatalf("Enqueue (delayed): %v", err)
	}
}

// ExampleQueue_RunWorker shows the typical worker setup with graceful shutdown.
func ExampleQueue_RunWorker() {
	db := openExampleDB()
	defer db.Close()

	q, err := goqueue.New(db, goqueue.Config{
		QueueName:     "emails",
		Concurrency:   4,
		LeaseTTL:      2 * time.Minute,
		PollInterval:  5 * time.Second,
		BackoffBase:   10 * time.Second,
		BackoffMax:    10 * time.Minute,
		BackoffJitter: 5 * time.Second,
		OnError: func(job goqueue.Job, op string, err error) {
			log.Printf("worker error op=%s job=%d: %v", op, job.ID, err)
		},
	})
	if err != nil {
		log.Fatalf("goqueue.New: %v", err)
	}

	handler := func(ctx context.Context, job goqueue.Job) error {
		var p EmailPayload
		if err := json.Unmarshal(job.Payload, &p); err != nil {
			// Returning an error re-queues with backoff. For a permanent failure
			// (e.g. bad payload), call q.Abandon instead and return nil.
			return fmt.Errorf("unmarshal payload: %w", err)
		}
		log.Printf("sending email to %s (attempt %d/%d)", p.To, job.Attempts, job.MaxAttempts)
		// ... send the email ...
		return nil
	}

	// RunWorker blocks until ctx is cancelled. Cancelling ctx triggers a
	// graceful shutdown: no new jobs are claimed, in-flight jobs complete.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	q.RunWorker(ctx, handler)
}

// ExampleQueue_Claim shows the manual claim/complete/fail cycle
// for use cases where RunWorker's automatic dispatch is not suitable.
func ExampleQueue_Claim() {
	db := openExampleDB()
	defer db.Close()

	q, err := goqueue.New(db, goqueue.Config{
		QueueName:      "emails",
		ClaimBatchSize: 10,
		LeaseTTL:       1 * time.Minute,
	})
	if err != nil {
		log.Fatalf("goqueue.New: %v", err)
	}

	ctx := context.Background()

	jobs, err := q.Claim(ctx)
	if err != nil {
		log.Fatalf("Claim: %v", err)
	}

	for _, job := range jobs {
		var p EmailPayload
		if err := json.Unmarshal(job.Payload, &p); err != nil {
			// Unrecoverable — abandon immediately instead of consuming attempts.
			if aerr := q.Abandon(ctx, job.ID, err.Error()); aerr != nil {
				log.Printf("Abandon job %d: %v", job.ID, aerr)
			}
			continue
		}

		if err := exampleSendEmail(p); err != nil {
			// Transient failure — Fail re-queues with backoff, or abandons at MaxAttempts.
			if ferr := q.Fail(ctx, job.ID, err.Error()); ferr != nil {
				if errors.Is(ferr, goqueue.ErrNotClaimed) {
					log.Printf("job %d: lease expired before Fail", job.ID)
				} else {
					log.Printf("Fail job %d: %v", job.ID, ferr)
				}
			}
			continue
		}

		if cerr := q.Complete(ctx, job.ID); cerr != nil {
			if errors.Is(cerr, goqueue.ErrNotClaimed) {
				log.Printf("job %d: lease expired before Complete", job.ID)
			} else {
				log.Printf("Complete job %d: %v", job.ID, cerr)
			}
		}
	}
}

// exampleSendEmail is a stub used by ExampleQueue_Claim.
func exampleSendEmail(_ EmailPayload) error { return nil }

// ExampleQueue_ExtendLease shows how to keep a long-running job's lease alive.
func ExampleQueue_ExtendLease() {
	db := openExampleDB()
	defer db.Close()

	q, err := goqueue.New(db, goqueue.Config{
		QueueName: "reports",
		LeaseTTL:  1 * time.Minute,
	})
	if err != nil {
		log.Fatalf("goqueue.New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	jobs, err := q.Claim(ctx)
	if err != nil || len(jobs) == 0 {
		return
	}
	job := jobs[0]

	// Extend the lease every 30s in the background. Extend well before
	// ClaimedUntil to absorb Go/MySQL clock skew.
	extendDone := make(chan struct{})
	go func() {
		defer close(extendDone)
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := q.ExtendLease(ctx, job.ID, 1*time.Minute); err != nil {
					if errors.Is(err, goqueue.ErrNotClaimed) {
						log.Printf("job %d: lease expired; stopping extension", job.ID)
					} else {
						log.Printf("ExtendLease job %d: %v", job.ID, err)
					}
					return
				}
			}
		}
	}()

	// ... perform the long-running work ...

	cancel() // stop the extender
	<-extendDone

	// ctx is now cancelled; use context.Background() so the bookkeeping
	// write is not rejected by the already-cancelled context.
	if err := q.Complete(context.Background(), job.ID); err != nil {
		log.Printf("Complete job %d: %v", job.ID, err)
	}
}

// ExampleQueue_StartReaper shows how to run the lease reaper in a
// producer-only process that doesn't use RunWorker.
func ExampleQueue_StartReaper() {
	db := openExampleDB()
	defer db.Close()

	q, err := goqueue.New(db, goqueue.Config{
		QueueName:      "emails",
		ReaperInterval: 30 * time.Second,
		OnReaperError: func(err error) {
			log.Printf("reaper error: %v", err)
		},
	})
	if err != nil {
		log.Fatalf("goqueue.New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// StartReaper runs once immediately on startup (to recover leases from a
	// prior crash), then repeats every ReaperInterval. RunWorker calls
	// StartReaper automatically; use this only in producer-only processes.
	cancelReaper, reaperDone := q.StartReaper(ctx)
	defer func() {
		cancelReaper()
		<-reaperDone
	}()

	// ... producer logic ...
}

package goqueue

import (
	"database/sql"
	"fmt"
	"math/rand"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"
)

var safeIdentifier = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]{0,63}$`)

// Config holds all configuration for a Queue instance.
type Config struct {
	QueueName      string        // required; max 128 chars
	ClaimBatchSize int           // default 1
	LeaseTTL       time.Duration // default 5m; minimum 1s
	MaxAttempts    int           // default 8
	BackoffBase    time.Duration // default 5s
	BackoffMax     time.Duration // default 10m
	// BackoffJitter enables random jitter added to backoff delays.
	// 0 disables jitter; positive samples a random duration in [0, BackoffJitter] (inclusive).
	BackoffJitter  time.Duration
	ReaperInterval time.Duration // default 30s
	PollInterval   time.Duration // default 2s
	Concurrency    int           // default 1
	WorkerID       string        // default hostname+pid; max 128 chars
	TableName      string        // default "queue_jobs"; must match [a-zA-Z_][a-zA-Z0-9_]{0,63}

	// RandSource overrides the random source used for jitter. If nil, a new
	// source seeded from the current time is used. Useful for deterministic tests.
	RandSource rand.Source

	// OnError is called when Complete or Fail returns an error inside RunWorker.
	// op is "complete" or "fail". Optional.
	OnError func(job Job, op string, err error)

	// OnReaperError is called when Reap returns an error inside StartReaper. Optional.
	OnReaperError func(err error)
}

// lockedRand is a concurrency-safe wrapper around *rand.Rand.
type lockedRand struct {
	mu  sync.Mutex
	rng *rand.Rand
}

func (r *lockedRand) Int63n(n int64) int64 {
	r.mu.Lock()
	v := r.rng.Int63n(n)
	r.mu.Unlock()
	return v
}

// Queue is a durable job queue backed by MySQL. It is safe for concurrent use.
type Queue struct {
	db  *sql.DB
	cfg Config
	rng *lockedRand
}

// New creates a new Queue, validates config, and applies defaults.
func New(db *sql.DB, cfg Config) (*Queue, error) {
	if db == nil {
		return nil, fmt.Errorf("goqueue: db must not be nil")
	}
	if cfg.QueueName == "" {
		return nil, ErrQueueNameRequired
	}
	if len(cfg.QueueName) > 128 {
		return nil, fmt.Errorf("goqueue: queue name exceeds 128 characters")
	}
	if strings.ContainsRune(cfg.QueueName, 0) {
		return nil, fmt.Errorf("goqueue: queue name must not contain null bytes")
	}
	if cfg.ClaimBatchSize <= 0 {
		cfg.ClaimBatchSize = 1
	}
	if cfg.LeaseTTL < 0 {
		return nil, fmt.Errorf("goqueue: LeaseTTL must not be negative")
	}
	if cfg.LeaseTTL == 0 {
		cfg.LeaseTTL = 5 * time.Minute
	}
	if cfg.LeaseTTL < time.Second {
		return nil, fmt.Errorf("goqueue: LeaseTTL must be at least 1 second")
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 8
	}
	if cfg.BackoffBase <= 0 {
		cfg.BackoffBase = 5 * time.Second
	}
	if cfg.BackoffMax <= 0 {
		cfg.BackoffMax = 10 * time.Minute
	}
	if cfg.BackoffJitter < 0 {
		return nil, fmt.Errorf("goqueue: BackoffJitter must be >= 0 (use 0 to disable jitter)")
	}
	if cfg.BackoffJitter > cfg.BackoffMax {
		return nil, fmt.Errorf("goqueue: BackoffJitter (%v) must not exceed BackoffMax (%v)", cfg.BackoffJitter, cfg.BackoffMax)
	}
	if cfg.ReaperInterval <= 0 {
		cfg.ReaperInterval = 30 * time.Second
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 2 * time.Second
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 1
	}
	if cfg.WorkerID == "" {
		cfg.WorkerID = defaultWorkerID()
	}
	if strings.ContainsRune(cfg.WorkerID, 0) {
		return nil, fmt.Errorf("goqueue: worker ID must not contain null bytes")
	}
	if len(cfg.WorkerID) > 128 {
		return nil, fmt.Errorf("goqueue: worker ID exceeds 128 characters")
	}
	if cfg.TableName == "" {
		cfg.TableName = "queue_jobs"
	}
	if !safeIdentifier.MatchString(cfg.TableName) {
		return nil, fmt.Errorf("goqueue: invalid table name %q: must match [a-zA-Z_][a-zA-Z0-9_]{0,63}", cfg.TableName)
	}

	src := cfg.RandSource
	if src == nil {
		src = rand.NewSource(time.Now().UnixNano())
	}
	return &Queue{db: db, cfg: cfg, rng: &lockedRand{rng: rand.New(src)}}, nil
}

// table returns the backtick-quoted table name for safe interpolation into SQL.
func (q *Queue) table() string {
	return "`" + q.cfg.TableName + "`"
}

// durationToSecs converts a duration to whole seconds, rounding up.
// A zero or negative duration returns 0.
func durationToSecs(d time.Duration) int64 {
	if d <= 0 {
		return 0
	}
	return int64((d + time.Second - 1) / time.Second)
}

// defaultWorkerID builds a worker identifier from hostname and PID.
func defaultWorkerID() string {
	host, err := os.Hostname()
	if err != nil {
		host = "unknown"
	}
	return fmt.Sprintf("%s-%d", host, os.Getpid())
}

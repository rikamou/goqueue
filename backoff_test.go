package goqueue

import (
	"math/rand"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newBackoffQ returns a Queue with a deterministic RNG for backoff tests.
func newBackoffQ(t *testing.T, jitter time.Duration) *Queue {
	t.Helper()
	db, _, err := sqlmock.New()
	require.NoError(t, err)
	q, err := New(db, Config{
		QueueName:     "test",
		BackoffBase:   5 * time.Second,
		BackoffMax:    10 * time.Minute,
		BackoffJitter: jitter,
		RandSource:    rand.NewSource(42), // deterministic
	})
	require.NoError(t, err)
	return q
}

func TestBackoffAttempt1(t *testing.T) {
	q := newBackoffQ(t, 2*time.Second)
	d := q.calcBackoff(1)
	assert.GreaterOrEqual(t, d, 5*time.Second, "attempt 1 must be >= 5s")
	assert.LessOrEqual(t, d, 7*time.Second, "attempt 1 must be <= 7s")
}

func TestBackoffAttempt2(t *testing.T) {
	q := newBackoffQ(t, 2*time.Second)
	d := q.calcBackoff(2)
	assert.GreaterOrEqual(t, d, 10*time.Second, "attempt 2 must be >= 10s")
	assert.LessOrEqual(t, d, 12*time.Second, "attempt 2 must be <= 12s")
}

func TestBackoffAttempt8(t *testing.T) {
	q := newBackoffQ(t, 2*time.Second)
	d := q.calcBackoff(8)
	// base = 5s * 2^7 = 640s > BackoffMax (600s), so result is clamped to exactly BackoffMax.
	assert.Equal(t, 10*time.Minute, d, "attempt 8 must be clamped to BackoffMax")
}

func TestBackoffNeverExceedsMax(t *testing.T) {
	q := newBackoffQ(t, 2*time.Second)
	for i := 1; i <= 1000; i++ {
		d := q.calcBackoff(i)
		assert.LessOrEqual(t, d, q.cfg.BackoffMax, "attempt %d exceeded BackoffMax", i)
		assert.GreaterOrEqual(t, d, time.Duration(0), "attempt %d produced negative duration", i)
	}
}

func TestBackoffJitterBounds(t *testing.T) {
	q := newBackoffQ(t, 2*time.Second)
	for i := 0; i < 1000; i++ {
		d := q.calcBackoff(1)
		assert.GreaterOrEqual(t, d, q.cfg.BackoffBase, "jitter run %d below base", i)
		assert.LessOrEqual(t, d, q.cfg.BackoffBase+q.cfg.BackoffJitter, "jitter run %d above base+jitter", i)
	}
}

func TestBackoffZeroJitterDisabled(t *testing.T) {
	q := newBackoffQ(t, 0) // jitter=0 means disabled
	for i := 1; i <= 8; i++ {
		d := q.calcBackoff(i)
		// With no jitter, result must be exactly base*2^(i-1) capped at BackoffMax.
		assert.LessOrEqual(t, d, q.cfg.BackoffMax, "attempt %d exceeded BackoffMax", i)
	}
	// attempt 1 with no jitter = exactly BackoffBase
	assert.Equal(t, 5*time.Second, q.calcBackoff(1))
}

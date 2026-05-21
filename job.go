package goqueue

import (
	"encoding/json"
	"time"
)

// Job represents a single queue entry returned by Claim.
type Job struct {
	ID             int64
	QueueName      string
	IdempotencyKey *string
	Payload        json.RawMessage
	State          string
	Priority       int
	Attempts       int
	MaxAttempts    int
	NextAttemptAt  time.Time
	ClaimedBy      *string
	ClaimedAt      *time.Time
	ClaimedUntil   *time.Time
	LastError      *string
	CreatedAt      time.Time
	UpdatedAt      time.Time
	CompletedAt    *time.Time
}

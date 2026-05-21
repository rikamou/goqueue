// Package goqueue provides a generic durable job queue backed by MySQL.
package goqueue

import "errors"

// ErrDuplicateJob is returned when an enqueue violates a unique idempotency key constraint.
var ErrDuplicateJob = errors.New("goqueue: duplicate idempotency key")

// ErrNotClaimed is returned when a complete/fail/abandon is attempted by a worker that does not own the job.
var ErrNotClaimed = errors.New("goqueue: job not claimed by this worker")

// ErrInvalidPayload is returned when the provided payload is not valid JSON.
var ErrInvalidPayload = errors.New("goqueue: payload is not valid JSON")

// ErrQueueNameRequired is returned when Config.QueueName is empty.
var ErrQueueNameRequired = errors.New("goqueue: queue name is required")

// Package job provides the core job processing interfaces and types for the goqueue package.
package job

import (
	"context"
	"encoding/json"
	"time"
)

// Job is the interface that must be implemented by any job that can be processed by the queue.
// Implementations should encapsulate the job's data and processing logic.
type Job interface {
	// Process executes the job's logic with the given context.
	// It should return an error if the job fails to process successfully.
	// The context can be used for cancellation and timeout control.
	Process(ctx context.Context) error
}

// JobContext encapsulates a job and its metadata for processing.
// It provides context about when and where the job was enqueued.
type JobContext struct {
	// Job is the actual job implementation to be processed
	Job Job
	// JobID is a unique identifier for this job instance
	JobID string
	// QueueName is the name of the queue this job belongs to
	QueueName string
	// EnqueuedAt is the timestamp when the job was added to the queue
	EnqueuedAt time.Time
}

// QueuedJob represents a job that has been queued for processing.
// It contains the job along with queue-specific metadata.
type QueuedJob struct {
	// Job is the actual job implementation to be processed
	Job Job
	// ID is a unique identifier for this queued job
	ID string
	// EnqueuedAt is the timestamp when the job was added to the queue
	EnqueuedAt time.Time
	// RetryCount tracks how many times this job has been retried
	RetryCount int
}

// RedisQueuedJob is a specialized job type for Redis backend storage.
// It includes additional fields needed for Redis persistence and job reconstruction.
type RedisQueuedJob struct {
	// Job contains the serialized job data
	Job json.RawMessage `json:"job"`
	// ID is a unique identifier for this queued job
	ID string `json:"id"`
	// JobName is the registered name of the job type, used for deserialization
	JobName string `json:"job_name"`
	// EnqueuedAt is the timestamp when the job was added to the queue
	EnqueuedAt time.Time `json:"enqueued_at"`
	// RetryCount tracks how many times this job has been retried
	RetryCount int `json:"retry_count"`
}

package goqueue

import (
	"context"
	"time"

	"github.com/saravanasai/goqueue/config"
	"github.com/saravanasai/goqueue/internal/registry"
	"github.com/saravanasai/goqueue/internal/stats"
	"github.com/saravanasai/goqueue/job"
	"github.com/saravanasai/goqueue/queue"
)

// Job is the interface clients must implement for their jobs.
type Job = job.Job
type Queue = queue.Queue
type JobMetrics = config.JobMetrics
type QueueStats = stats.QueueStats

const DefaultShutdownTimeout = 5 * time.Second

// GetVersion returns the current version of GoQueue
func GetVersion() string {
	return Version
}

// NewQueueWithDefaults creates a new queue with the specified name and configuration,
// using the default shutdown timeout.
//
// This is a convenience function for when you don't need to specify a custom shutdown timeout.
//
// Parameters:
//   - queueName: A unique identifier for the queue
//   - cfg: The queue configuration specifying backend, workers, retry policy, etc.
//
// Returns:
//   - A new Queue instance and nil error on success
//   - nil and an error if queue creation fails
func NewQueueWithDefaults(queueName string, cfg config.Config) (*Queue, error) {
	return NewQueue(queueName, cfg, DefaultShutdownTimeout)
}

// NewQueue creates a new queue with the specified name, configuration, and shutdown timeout.
//
// Parameters:
//   - queueName: A unique identifier for the queue
//   - cfg: The queue configuration specifying backend, workers, retry policy, etc.
//   - shutdownTimeout: Maximum duration to wait for jobs to complete during shutdown
//
// Returns:
//   - A new Queue instance and nil error on success
//   - nil and an error if queue creation fails
func NewQueue(queueName string, cfg config.Config, shutdownTimeout time.Duration) (*Queue, error) {
	return queue.NewQueue(queueName, cfg, shutdownTimeout)
}

// RegisterJob registers a job type with the queue system to enable serialization/deserialization.
//
// This must be called for each job type before using the queue. It associates a string name
// with a function that creates new instances of your job type.
//
// Parameters:
//   - name: A unique string identifier for the job type
//   - constructor: A function that returns a new instance of the job type
func RegisterJob(name string, constructor func() Job) {
	registry.Register(name, constructor)
}

// Dispatch adds a single job to the queue for processing.
//
// The job will be stored in the configured backend and processed by available workers.
//
// Parameters:
//   - q: The queue to dispatch the job to
//   - payload: The job to be processed
//
// Returns:
//   - nil on successful dispatch
//   - an error if dispatch fails
func Dispatch(q *queue.Queue, payload job.Job) error {
	return q.Dispatch(payload)
}

// DispatchBatch adds multiple jobs to the queue for processing.
//
// All jobs will be stored in the configured backend and processed by available workers.
// This is more efficient than calling Dispatch multiple times for individual jobs.
//
// Parameters:
//   - q: The queue to dispatch the jobs to
//   - jobs: Slice of jobs to be processed
//
// Returns:
//   - nil on successful batch dispatch
//   - an error if batch dispatch fails
func DispatchBatch(q *queue.Queue, jobs []job.Job) error {
	return q.DispatchBatch(jobs)
}

// StartWorker launches worker goroutines to process jobs from the queue.
//
// Workers will continue running until the provided context is cancelled.
//
// Parameters:
//   - q: The queue to start workers for
//   - ctx: Context used for cancellation and shutdown
//   - count: Number of worker goroutines to start
//
// Returns:
//   - Error if workers cannot be started
func StartWorker(q *queue.Queue, ctx context.Context, count int) error {
	return q.StartWorkers(ctx, count)
}

// Shutdown gracefully stops the queue, waiting for in-progress jobs to complete.
//
// It will wait up to the timeout duration configured during queue creation for jobs to finish.
//
// Parameters:
//   - q: The queue to shut down
//   - ctx: Context used for cancellation
//
// Returns:
//   - nil if shutdown completes successfully
//   - an error if shutdown fails or times out
func Shutdown(q *queue.Queue, ctx context.Context) error {
	return q.Shutdown(ctx)
}

// GetQueueStats returns current queue statistics and health metrics.
//
// This includes job counts, processing rates, and health indicators.
// If statistics collection is disabled in the queue configuration,
// only basic health information is returned.
//
// Parameters:
//   - q: The queue to get statistics for
//
// Returns:
//   - QueueStats containing the current queue metrics
func GetQueueStats(q *queue.Queue) QueueStats {
	return q.Stats()
}

// IsQueueOverloaded checks if the queue is currently experiencing high load.
//
// The determination is based on the configured thresholds for job count and
// processing ratios. This can be used to implement backpressure mechanisms.
//
// Parameters:
//   - q: The queue to check load status for
//
// Returns:
//   - true if the queue is overloaded
//   - false if the queue is operating normally or if statistics collection is disabled
func IsQueueOverloaded(q *queue.Queue) bool {
	return q.IsOverloaded()
}

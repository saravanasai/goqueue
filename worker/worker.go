// Package worker provides job processing functionality through concurrent worker goroutines.
// It handles job execution, retries, error recovery, and graceful shutdown while maintaining
// configurable concurrency limits and collecting performance metrics.
package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/saravanasai/goqueue/adapter"
	"github.com/saravanasai/goqueue/adapter/utils"
	"github.com/saravanasai/goqueue/config"
	configuration "github.com/saravanasai/goqueue/config"
	"github.com/saravanasai/goqueue/internal/logger"
	"github.com/saravanasai/goqueue/internal/stats"
	"github.com/saravanasai/goqueue/job"
	"github.com/saravanasai/goqueue/middleware"
	"golang.org/x/sync/semaphore"
)

// Worker manages a pool of goroutines that process jobs from a queue.
// It handles job execution, retries, metrics collection, and graceful shutdown.
type Worker struct {
	// store provides access to the job storage backend
	store adapter.Store
	// config contains worker configuration options
	config configuration.Config
	// queueName identifies which queue this worker processes
	queueName string
	// wg tracks active worker goroutines for shutdown
	wg sync.WaitGroup
	// shutdownCh signals workers to stop processing
	shutdownCh chan struct{}
	// isShuttingDown indicates shutdown is in progress
	isShuttingDown int32
	// concurrencySem limits concurrent job processing
	concurrencySem *semaphore.Weighted
	// metricsChannel buffers job completion metrics
	metricsChannel chan configuration.JobMetrics
	// statsCollector gathers worker performance metrics
	statsCollector *stats.Collector
	// logger handles structured logging
	logger logger.Logger
	// handler processes jobs through the middleware chain
	handler middleware.HandlerFunc
}

// NewWorker creates a new Worker instance with the specified configuration.
// It initializes the middleware chain, concurrency controls, and metrics collection.
func NewWorker(store adapter.Store, config configuration.Config, queueName string, statsCollector *stats.Collector, logger logger.Logger) *Worker {
	metricsBufferSize := calculateMetricsBufferSize(config)

	// Create handler chain with configured middlewares
	handler := middleware.Chain(config.Middlewares...)

	return &Worker{
		store:          store,
		config:         config,
		queueName:      queueName,
		shutdownCh:     make(chan struct{}),
		concurrencySem: semaphore.NewWeighted(int64(config.ConcurrencyLimit)),
		metricsChannel: make(chan configuration.JobMetrics, metricsBufferSize),
		statsCollector: statsCollector,
		logger:         logger,
		handler:        handler,
	}
}

// Start launches the specified number of worker goroutines to process jobs.
// It returns an error if the driver is unsupported or if the requested number
// of workers exceeds the configured maximum.
func (w *Worker) Start(ctx context.Context, noOfWorkers int) error {
	supportedDrivers := map[string]bool{
		configuration.DriverMemory: true,
		configuration.DriverRedis:  true,
		configuration.DriverSQS:    true,
	}

	if !supportedDrivers[w.config.Driver] {
		return fmt.Errorf("unsupported driver: %s", w.config.Driver)
	}

	if noOfWorkers > w.config.MaxWorkers {
		return fmt.Errorf("requested workers (%d) exceeds maximum allowed (%d)", noOfWorkers, w.config.MaxWorkers)
	}
	w.logger.Info("Starting workers", "count", noOfWorkers, "queue", w.queueName)

	for i := 0; i < noOfWorkers; i++ {
		w.wg.Add(1)
		go w.workerLoop(ctx, i)
	}

	go w.startMetricsWorker(ctx)

	return nil
}

// workerLoop runs the main job processing loop for a single worker goroutine.
// It continuously polls for jobs and processes them until shutdown is signaled.
func (w *Worker) workerLoop(ctx context.Context, workerID int) {
	defer w.wg.Done()

	w.logger.Info("Worker started", "workerID", workerID, "queue", w.queueName)

	for {
		// Check for shutdown signal
		if atomic.LoadInt32(&w.isShuttingDown) == 1 {
			w.logger.Info("Worker stopping - no new jobs during shutdown", "workerID", workerID)
			return
		}

		// Check for context cancellation or explicit shutdown
		select {
		case <-ctx.Done():
			w.logger.Info("Worker shutting down - context canceled", "workerID", workerID)
			return

		case <-w.shutdownCh:
			w.logger.Info("Worker shutting down - shutdown signal received", "workerID", workerID)
			return

		default:
			// Continue processing
		}

		// Try to get a job
		job, err := w.store.Pop(w.queueName)
		if err != nil {
			// No jobs available, sleep briefly to avoid spinning
			time.Sleep(100 * time.Millisecond)
			continue
		}

		// Check again if shutdown was initiated while we were fetching the job
		if atomic.LoadInt32(&w.isShuttingDown) == 1 {
			w.logger.Info("Worker dropping job - shutdown in progress", "workerID", workerID, "jobID", job.JobID)
			return
		}

		// Validate job
		if job.Job == nil {
			w.logger.Error("Received nil job, skipping", "workerID", workerID, "jobID", job.JobID)
			continue
		}

		// Process the job
		w.logger.Info("Processing job", "workerID", workerID, "jobID", job.JobID, "jobType", fmt.Sprintf("%T", job.Job))
		w.processJobSafely(ctx, workerID, job)
	}
}

// processJobSafely executes a job with panic recovery, retry logic, and metrics collection.
// It respects concurrency limits and handles job completion acknowledgment.
func (w *Worker) processJobSafely(ctx context.Context, workerID int, job job.JobContext) {
	defer func() {
		if r := recover(); r != nil {
			w.logger.Error("Panic recovered in job execution", "workerID", workerID, "jobID", job.JobID, "panic", r)
			if w.config.StatsEnabled && w.statsCollector != nil {
				w.statsCollector.RecordComplete(0, false)
			}
		}
	}()

	// Acquire the semaphore before processing
	if err := w.concurrencySem.Acquire(ctx, 1); err != nil {
		w.logger.Error("Failed to acquire concurrency semaphore", "workerID", workerID, "jobID", job.JobID, "error", err)
		return
	}
	// Release the semaphore after processing
	defer w.concurrencySem.Release(1)

	isCollectorEnabled := w.config.StatsEnabled && w.statsCollector != nil
	if isCollectorEnabled {
		w.statsCollector.RecordDequeue(job.EnqueuedAt)
	}

	maxAttempts := w.config.MaxRetryAttempts
	retryDelay := w.config.RetryDelay
	exponential := w.config.ExponentialBackoff

	// Determine timeout for this job
	timeout := job.Timeout
	if timeout == 0 {
		timeout = w.config.JobTimeout
	}

	// Check if this job has already exceeded max retries
	if job.RetryCount >= maxAttempts {
		w.logger.Error("Job has already exceeded max retries, sending to DLQ", "workerID", workerID, "jobID", job.JobID, "retryCount", job.RetryCount, "maxAttempts", maxAttempts)

		// Send directly to DLQ
		if w.config.DLQAdapter != nil {
			if dlqErr := w.config.DLQAdapter.Push(ctx, &job, fmt.Errorf("job exceeded max retry attempts (%d)", maxAttempts)); dlqErr != nil {
				w.logger.Error("Failed to push job to DLQ", "jobID", job.JobID, "error", dlqErr)
			}
		} else {
			w.logger.Info("No DLQ configured, discarding failed job", "jobID", job.JobID)
		}

		// Acknowledge the job to remove it from processing
		if ackErr := w.store.Ack(w.queueName, job.JobID); ackErr != nil {
			w.logger.Error("Failed to ack job after DLQ", "workerID", workerID, "jobID", job.JobID, "error", ackErr)
		}
		return
	}

	var lastErr error
	// For retried jobs, we only try once more (the retry attempt)
	maxAttemptsThisRun := 1
	if job.RetryCount == 0 {
		// This is a fresh job, allow full retry attempts
		maxAttemptsThisRun = maxAttempts
	}

	for attempt := 1; attempt <= maxAttemptsThisRun; attempt++ {
		startTime := time.Now()
		execCtx := ctx
		// Empty cancel function that will be replaced if we create a timeout context
		cancel := func() {
			// Empty by default, will be replaced with actual cancel function if timeout is set
		}
		if timeout > 0 {
			execCtx, cancel = context.WithTimeout(ctx, timeout)
		}
		done := make(chan error, 1)
		go func() {
			done <- w.handler(execCtx, &job)
		}()
		select {
		case lastErr = <-done:
			// completed or failed
		case <-execCtx.Done():
			lastErr = execCtx.Err()
		}
		cancel()
		processingTime := time.Since(startTime)
		success := lastErr == nil

		if isCollectorEnabled {
			w.statsCollector.RecordComplete(processingTime, success)
		}

		if success {
			if ackErr := w.store.Ack(w.queueName, job.JobID); ackErr != nil {
				w.logger.Error("Failed to ack job", "workerID", workerID, "jobID", job.JobID, "error", ackErr)
			} else {
				w.logger.Info("Completed job", "workerID", workerID, "jobID", job.JobID, "duration", processingTime)
				if w.config.OnJobComplete != nil {
					metrics := config.JobMetrics{
						QueueName: w.queueName,
						JobID:     job.JobID,
						Duration:  processingTime,
						Error:     lastErr,
						Timestamp: time.Now(),
					}
					w.enqueueMetrics(metrics)
				}
			}
			return
		}

		// Timeout error handling
		if lastErr == context.DeadlineExceeded {
			w.logger.Error("Job execution timed out", "workerID", workerID, "jobID", job.JobID, "queue", w.queueName, "timeout", timeout)
		} else {
			w.logger.Error("Failed to process job", "workerID", workerID, "jobID", job.JobID, "attempt", attempt, "retryCount", job.RetryCount, "error", lastErr)
		}

		// Check if we should retry this job
		totalAttempts := job.RetryCount + attempt
		if totalAttempts < maxAttempts {
			delay := retryDelay
			if exponential {
				delay = retryDelay * time.Duration(1<<uint(job.RetryCount))
			}

			// Try driver-specific retry mechanisms
			retryHandled := false

			// Check if we're using Redis driver for non-blocking retries
			if redisStore, ok := w.store.(interface {
				RetryJobWithMetadata(string, interface{}, time.Duration) error
			}); ok {
				// For Redis, create a retry job with metadata instead of blocking
				if redisJob, err := w.createRedisQueuedJob(job, job.RetryCount); err == nil {
					if retryErr := redisStore.RetryJobWithMetadata(w.queueName, redisJob, delay); retryErr != nil {
						w.logger.Error("failed to add job to retry queue", "workerID", workerID, "jobID", job.JobID, "error", retryErr)
					} else {
						w.logger.Info("job added to retry queue for later processing", "workerID", workerID, "jobID", job.JobID, "retryCount", job.RetryCount, "delay", delay)
						// Don't acknowledge the job yet - it will be processed again from retry queue
						retryHandled = true
					}
				} else {
					w.logger.Error("failed to create retry job metadata", "workerID", workerID, "jobID", job.JobID, "error", err)
				}
			} else if sqsStore, ok := w.store.(interface {
				RetryJobWithMetadata(string, interface{}, time.Duration) error
			}); ok {
				// Check if the driver type is SQS for visibility timeout retry
				if w.config.Driver == "sqs" {
					// For SQS, try to directly call the method on the concrete type
					// We'll use a more robust retry method that creates the SQS job internally
					if retryErr := sqsStore.RetryJobWithMetadata(w.queueName, job, delay); retryErr != nil {
						w.logger.Error("failed to change message visibility for retry", "workerID", workerID, "jobID", job.JobID, "error", retryErr)
					} else {
						w.logger.Info("job scheduled for retry using visibility timeout", "workerID", workerID, "jobID", job.JobID, "retryCount", job.RetryCount+1, "delay", delay)
						// Don't acknowledge the job - let it be redelivered after visibility timeout
						retryHandled = true
					}
				}
			}

			// If retry was handled by driver-specific mechanism, return early
			if retryHandled {
				return
			}

			// Fallback to blocking retry (for memory driver or if driver-specific retry failed)
			w.logger.Info("Using fallback blocking retry", "workerID", workerID, "jobID", job.JobID, "delay", delay)

			if err := w.store.RetryJobWithMetadata(w.queueName, job.Job, delay); err != nil {
				w.logger.Error("Fallback retry failed", "workerID", workerID, "jobID", job.JobID, "error", err)
			}
		}
	}

	// All attempts failed
	w.logger.Error("Job failed after max retries", "workerID", workerID, "jobID", job.JobID, "error", lastErr)

	// Try to push to DLQ if configured
	if w.config.DLQAdapter != nil {
		if dlqErr := w.config.DLQAdapter.Push(ctx, &job, lastErr); dlqErr != nil {
			w.logger.Error("Failed to push job to DLQ", "jobID", job.JobID, "error", dlqErr)
		}
	} else {
		w.logger.Info("No DLQ configured, discarding failed job", "jobID", job.JobID)
	}

	// Record metrics if enabled
	if w.config.OnJobComplete != nil {
		metrics := config.JobMetrics{
			QueueName: w.queueName,
			JobID:     job.JobID,
			Duration:  0,
			Error:     lastErr,
			Timestamp: time.Now(),
		}
		w.enqueueMetrics(metrics)
	}
}

// enqueueMetrics adds job completion metrics to the metrics channel.
// If the channel is full, the metrics are dropped and an error is logged.
func (w *Worker) enqueueMetrics(metrics config.JobMetrics) {
	select {
	case w.metricsChannel <- metrics:
	default:
		w.logger.Error("Metrics channel full, dropping metrics", "jobID", metrics.JobID)
	}
}

// startMetricsWorker runs a goroutine that processes job completion metrics
// and calls the configured metrics callback function.
func (w *Worker) startMetricsWorker(ctx context.Context) {
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case metrics := <-w.metricsChannel:
				if err := w.store.EnqueueMetrics(metrics); err != nil {
					w.logger.Error("Failed to enqueue metrics", "error", err)
				}
			default:
				jobCtx, err := w.store.DequeueMetrics(w.queueName)
				if (err == nil && jobCtx != config.JobMetrics{}) {
					w.logger.Info("Metrics worker processing job: inside", "jobID", jobCtx.JobID)
					w.config.OnJobComplete(jobCtx)
				}
			}
		}
	}()
}

// Shutdown initiates a graceful shutdown of all worker goroutines.
// It prevents workers from picking up new jobs and waits for in-progress
// jobs to complete, up to the context's deadline.
func (w *Worker) Shutdown(ctx context.Context) error {
	w.logger.Info("Initiating graceful shutdown", "queue", w.queueName)

	// PHASE 1: Signal shutdown intention
	// This prevents workers from picking up NEW jobs
	atomic.StoreInt32(&w.isShuttingDown, 1)

	// Broadcast shutdown signal to ALL workers
	close(w.shutdownCh)

	// PHASE 2: Wait for workers to finish current jobs
	// Create a channel to signal when all workers are done
	allWorkersDone := make(chan struct{})

	go func() {
		w.wg.Wait() // Block until all workers call wg.Done()
		close(allWorkersDone)
	}()

	// PHASE 3: Wait with timeout
	select {
	case <-allWorkersDone:
		w.logger.Info("All workers shut down gracefully", "queue", w.queueName)
		return nil

	case <-ctx.Done():
		w.logger.Error("Shutdown timeout reached, some workers may be terminated", "queue", w.queueName)
		return ctx.Err()
	}
}

// calculateMetricsBufferSize determines the appropriate size for the metrics channel buffer
// based on the worker configuration, ensuring it stays within reasonable bounds.
func calculateMetricsBufferSize(config config.Config) int {
	// Constants for buffer size calculation
	const (
		baseBufferSize  = 100   // Minimum base buffer size
		bufferPerWorker = 25    // Buffer slots per potential worker
		minBufferSize   = 100   // Minimum total buffer size
		maxBufferSize   = 50000 // Maximum total buffer size to prevent excessive memory usage
	)

	// Calculate buffer size based on max workers configuration
	calculatedSize := baseBufferSize + (config.MaxWorkers * bufferPerWorker)

	// Ensure the buffer size is within reasonable bounds
	if calculatedSize < minBufferSize {
		calculatedSize = minBufferSize
	}
	if calculatedSize > maxBufferSize {
		calculatedSize = maxBufferSize
	}

	return calculatedSize
}

// createRedisQueuedJob creates a RedisQueuedJob from a JobContext for retry purposes
func (w *Worker) createRedisQueuedJob(jobCtx job.JobContext, retryCount int) (job.RedisQueuedJob, error) {
	// Marshal the actual job
	jobPayload, err := json.Marshal(jobCtx.Job)
	if err != nil {
		return job.RedisQueuedJob{}, fmt.Errorf("failed to marshal job: %w", err)
	}

	// Get job name using utils
	jobName := utils.GetJobName(jobCtx.Job)
	if jobName == "" {
		return job.RedisQueuedJob{}, fmt.Errorf("could not determine job name from type")
	}

	return job.RedisQueuedJob{
		Job:        jobPayload,
		JobName:    jobName,
		ID:         jobCtx.JobID,
		EnqueuedAt: jobCtx.EnqueuedAt,
		RetryCount: retryCount,
	}, nil
}

// Package worker provides job processing functionality through concurrent worker goroutines.
// It handles job execution, retries, error recovery, and graceful shutdown while maintaining
// configurable concurrency limits and collecting performance metrics.
package worker

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/saravanasai/goqueue/adapter"
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
		logger:        logger,
		handler:       handler,
	}
}

// Start launches the specified number of worker goroutines to process jobs.
// It returns an error if the driver is unsupported or if the requested number
// of workers exceeds the configured maximum.
func (w *Worker) Start(ctx context.Context, noOfWorkers int) error {
	if w.config.Driver != configuration.DriverMemory && w.config.Driver != configuration.DriverRedis {
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
		if atomic.LoadInt32(&w.isShuttingDown) == 1 {
			w.logger.Info("Worker stopping - no new jobs during shutdown", "workerID", workerID)
			return
		}

		select {
		case <-ctx.Done():
			w.logger.Info("Worker shutting down - context canceled", "workerID", workerID)
			return

		case <-w.shutdownCh:
			w.logger.Info("Worker shutting down - shutdown signal received", "workerID", workerID)
			return

		default:
			job, err := w.store.Pop(w.queueName)
			if err != nil {
				w.logger.Error("Worker pop error", "workerID", workerID, "error", err)
				time.Sleep(100 * time.Millisecond)
				continue
			}

			if atomic.LoadInt32(&w.isShuttingDown) == 1 {
				w.logger.Info("Worker dropping job - shutdown in progress", "workerID", workerID, "jobID", job.JobID)
				return
			}

			w.processJobSafely(ctx, workerID, job)
		}
	}
}

// processJobSafely executes a job with panic recovery, retry logic, and metrics collection.
// It respects concurrency limits and handles job completion acknowledgment.
func (w *Worker) processJobSafely(ctx context.Context, workerID int, job job.JobContext) {
	defer func() {
		if r := recover(); r != nil {
			w.logger.Error("Panic recovered in job execution", "workerID", workerID, "jobID", job.JobID, "panic", r)
			// Record panic in stats if enabled
			if w.config.StatsEnabled && w.statsCollector != nil {
				w.statsCollector.RecordComplete(0, false)
			}
		}
	}()

	defer w.concurrencySem.Release(1)

	if err := w.concurrencySem.Acquire(ctx, 1); err != nil {
		w.logger.Error("Failed to acquire concurrency semaphore", "workerID", workerID, "jobID", job.JobID, "error", err)
		return
	}

	isCollectorEnabled := w.config.StatsEnabled && w.statsCollector != nil

	if isCollectorEnabled {
		w.statsCollector.RecordDequeue(job.EnqueuedAt)
	}

	maxAttempts := w.config.MaxRetryAttempts
	retryDelay := w.config.RetryDelay
	exponential := w.config.ExponentialBackoff

	var attempt int
	var lastErr error
	for attempt = 1; attempt <= maxAttempts; attempt++ {
		startTime := time.Now()
		lastErr = w.handler(ctx, &job)
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

		// Job failed, log and retry if attempts remain
		w.logger.Error("Failed to process job", "workerID", workerID, "jobID", job.JobID, "attempt", attempt, "error", lastErr)
		if attempt < maxAttempts {
			delay := retryDelay
			if exponential {
				delay = retryDelay * time.Duration(1<<uint(attempt-1))
			}
			time.Sleep(delay)
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
				w.logger.Info("Metrics worker processing job", "jobID", jobCtx.JobID)
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
	// close() makes ALL receivers of shutdownCh immediately unblock
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

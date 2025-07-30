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
	"golang.org/x/sync/semaphore"
)

type Worker struct {
	store          adapter.Store
	config         configuration.Config
	queueName      string
	wg             sync.WaitGroup
	shutdownCh     chan struct{}
	isShuttingDown int32
	concurrencySem *semaphore.Weighted
	metricsChannel chan configuration.JobMetrics
	statsCollector *stats.Collector
	logger         logger.Logger
}

func NewWorker(store adapter.Store, config configuration.Config, queueName string, statsCollector *stats.Collector, logger logger.Logger) *Worker {

	metricsBufferSize := calculateMetricsBufferSize(config)

	return &Worker{
		store:          store,
		config:         config,
		queueName:      queueName,
		shutdownCh:     make(chan struct{}),
		concurrencySem: semaphore.NewWeighted(int64(config.ConcurrencyLimit)),
		metricsChannel: make(chan configuration.JobMetrics, metricsBufferSize),
		statsCollector: statsCollector,
		logger:         logger,
	}
}

func (w *Worker) Start(ctx context.Context, noOfWorkers int) error {

	if w.config.Driver != config.DriverMemory && w.config.Driver != config.DriverRedis {
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

func (w *Worker) processJobSafely(ctx context.Context, workerID int, job job.JobContext) {

	if err := w.concurrencySem.Acquire(ctx, 1); err != nil {
		w.logger.Error("Failed to acquire concurrency semaphore", "workerID", workerID, "jobID", job.JobID, "error", err)
		return
	}
	defer w.concurrencySem.Release(1)

	isCollectorEnabled := w.config.StatsEnabled && w.statsCollector != nil

	if isCollectorEnabled {
		w.statsCollector.RecordDequeue(job.EnqueuedAt)
	}

	startTime := time.Now()
	err := job.Job.Process(ctx)
	processingTime := time.Since(startTime)
	success := err == nil

	if isCollectorEnabled {
		w.statsCollector.RecordComplete(processingTime, success)
	}

	if err == nil {
		if ackErr := w.store.Ack(w.queueName, job.JobID); ackErr != nil {
			w.logger.Error("Failed to ack job", "workerID", workerID, "jobID", job.JobID, "error", ackErr)
		} else {
			w.logger.Info("Completed job", "workerID", workerID, "jobID", job.JobID, "duration", processingTime)
			// Handle metrics with clean struct
			if w.config.OnJobComplete != nil {
				metrics := config.JobMetrics{
					QueueName: w.queueName,
					JobID:     job.JobID,
					Duration:  processingTime,
					Error:     err,
					Timestamp: time.Now(),
				}
				w.enqueueMetrics(metrics)
			}
		}
	} else {
		// Job failed - log error (retry logic can be added later)
		w.logger.Error("Failed to process job", "workerID", workerID, "jobID", job.JobID, "error", err)
	}
}

func (w *Worker) enqueueMetrics(metrics config.JobMetrics) {
	select {
	case w.metricsChannel <- metrics:
	default:
		w.logger.Error("Metrics channel full, dropping metrics", "jobID", metrics.JobID)
	}
}

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

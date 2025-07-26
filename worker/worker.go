package worker

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/saravanasai/goqueue/adapter"
	"github.com/saravanasai/goqueue/config"
	"github.com/saravanasai/goqueue/job"
	"golang.org/x/sync/semaphore"
)

type Worker struct {
	store          adapter.Store
	config         config.Config
	queueName      string
	wg             sync.WaitGroup
	shutdownCh     chan struct{}
	isShuttingDown int32
	concurrencySem *semaphore.Weighted
}

func NewWorker(store adapter.Store, config config.Config, queueName string) *Worker {
	return &Worker{
		store:          store,
		config:         config,
		queueName:      queueName,
		shutdownCh:     make(chan struct{}),
		concurrencySem: semaphore.NewWeighted(int64(config.ConcurrencyLimit)),
	}
}

func (w *Worker) Start(ctx context.Context, noOfWorkers int) error {

	if w.config.Driver != config.DriverMemory && w.config.Driver != config.DriverRedis {
		return fmt.Errorf("unsupported driver: %s", w.config.Driver)
	}

	if noOfWorkers > w.config.MaxWorkers {
		return fmt.Errorf("requested workers (%d) exceeds maximum allowed (%d)", noOfWorkers, w.config.MaxWorkers)
	}
	log.Printf("Starting %d workers for queue '%s'", noOfWorkers, w.queueName)

	for i := 0; i < noOfWorkers; i++ {
		w.wg.Add(1)
		go w.workerLoop(ctx, i)
	}

	go w.startMetricsWorker(ctx)

	return nil

}

func (w *Worker) workerLoop(ctx context.Context, workerID int) {
	defer w.wg.Done()

	log.Printf("Worker %d started for queue '%s'", workerID, w.queueName)

	for {
		if atomic.LoadInt32(&w.isShuttingDown) == 1 {
			log.Printf("Worker %d stopping - no new jobs during shutdown", workerID)
			return
		}

		select {
		case <-ctx.Done():
			log.Printf("Worker %d shutting down - context canceled", workerID)
			return

		case <-w.shutdownCh:
			log.Printf("Worker %d shutting down - shutdown signal received", workerID)
			return

		default:
			job, err := w.store.Pop(w.queueName)
			if err != nil {
				log.Printf("Worker %d pop error: %v", workerID, err)
				time.Sleep(100 * time.Millisecond)
				continue
			}

			if atomic.LoadInt32(&w.isShuttingDown) == 1 {
				log.Printf("Worker %d dropping job %s - shutdown in progress", workerID, job.JobID)
				return
			}

			w.processJobSafely(ctx, workerID, job)
		}
	}
}

func (w *Worker) processJobSafely(ctx context.Context, workerID int, job job.JobContext) {

	if err := w.concurrencySem.Acquire(ctx, 1); err != nil {
		log.Printf("Worker %d failed to acquire concurrency semaphore for job %s: %v", workerID, job.JobID, err)
		return
	}
	defer w.concurrencySem.Release(1)

	startTime := time.Now()
	err := job.Job.Process(ctx)
	processingTime := time.Since(startTime)

	if err == nil {
		if ackErr := w.store.Ack(w.queueName, job.JobID); ackErr != nil {
			log.Printf("Worker %d failed to ack job %s: %v", workerID, job.JobID, ackErr)
		} else {
			log.Printf("Worker %d completed job %s in %v", workerID, job.JobID, processingTime)
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
		log.Printf("Worker %d failed to process job %s: %v", workerID, job.JobID, err)
	}
}

func (w *Worker) enqueueMetrics(metrics config.JobMetrics) {
	go func() {
		if err := w.store.EnqueueMetrics(metrics); err != nil {
			log.Printf("Failed to enqueue metrics for job %s: %v", metrics.JobID, err)
		}
	}()
}

func (w *Worker) startMetricsWorker(ctx context.Context) {
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
				jobCtx, err := w.store.DequeueMetrics(w.queueName)
				fmt.Println("Metrics worker processing job:", jobCtx.JobID)
				if (err == nil && jobCtx != config.JobMetrics{}) {
					fmt.Println("Metrics worker processing job:inside")
					w.config.OnJobComplete(jobCtx)
				}
			}
		}
	}()
}

func (w *Worker) Shutdown(ctx context.Context) error {
	log.Printf("Initiating graceful shutdown for queue '%s'", w.queueName)

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
		log.Printf("All workers for queue '%s' shut down gracefully", w.queueName)
		return nil

	case <-ctx.Done():
		log.Printf("Shutdown timeout reached for queue '%s', some workers may be terminated", w.queueName)
		return ctx.Err()
	}
}

// Package queue provides the core job queue functionality, including job dispatching,
// worker management, and queue lifecycle control. It supports multiple backend drivers
// and provides health monitoring and statistics collection.
package queue

import (
	"context"
	"fmt"
	"time"

	"github.com/saravanasai/goqueue/adapter"
	"github.com/saravanasai/goqueue/adapter/memory"
	"github.com/saravanasai/goqueue/adapter/sqs"
	"github.com/saravanasai/goqueue/config"
	"github.com/saravanasai/goqueue/dispatcher"
	"github.com/saravanasai/goqueue/internal/logger"
	"github.com/saravanasai/goqueue/internal/manager"
	"github.com/saravanasai/goqueue/internal/stats"
	"github.com/saravanasai/goqueue/job"
	"github.com/saravanasai/goqueue/worker"
)

// Queue represents a job queue instance that manages job dispatching and processing.
// It coordinates the interaction between job storage, dispatchers, and workers while
// providing monitoring and statistics collection capabilities.
type Queue struct {
	// config holds the queue configuration options
	config config.Config
	// store is the backend storage adapter (memory or Redis)
	store adapter.Store
	// dispatcher handles enqueueing jobs to the store
	dispatcher *dispatcher.Dispatcher
	// worker manages the job processing goroutines
	worker *worker.Worker
	// queueName is the unique identifier for this queue
	queueName string
	// ShutdownTimeout is the maximum duration to wait for graceful shutdown
	ShutdownTimeout time.Duration
	// cancelFunc cancels the queue's context on shutdown
	cancelFunc context.CancelFunc
	// statsCollector gathers queue performance metrics
	statsCollector *stats.Collector
	// logger handles structured logging
	logger logger.Logger
}

// NewQueue creates a new Queue instance with the specified configuration.
// It initializes the appropriate storage backend, dispatcher, and worker components.
// The shutdownTimeout parameter controls how long to wait for graceful shutdown.
//
// Returns an error if the configuration is invalid or if backend initialization fails.
func NewQueue(queueName string, cfg config.Config, shutdownTimeout time.Duration) (*Queue, error) {
	var store adapter.Store
	logger := logger.NewZapLogger()

	if err := cfg.Validate(logger); err != nil {
		return nil, err
	}

	queueCtx, queueCancel := context.WithCancel(context.Background())

	switch cfg.Driver {
	case config.DriverMemory:
		store = memory.NewInMemoryStore(queueName, cfg, logger)
	case config.DriverRedis:
		redisCfg, ok := cfg.DriverConfig.(config.RedisConfig)
		if !ok {
			logger.Error("Invalid Redis config provided")
			queueCancel()
			return nil, fmt.Errorf("invalid Redis config provided")
		}
		redisMgr := manager.NewRedisClientManager(redisCfg.Addr, redisCfg.Password, redisCfg.Db, logger)
		redisMgr.StartPeriodicHealthCheck(queueCtx)
		client := redisMgr.GetClient(redisCfg.Addr, redisCfg.Password, redisCfg.Db)
		store = memory.NewRedisStore(client, cfg, redisMgr, redisCfg.Addr, redisCfg.Db, logger)
	case config.DriverSQS:
		var err error
		store, err = sqs.NewSQSStore(cfg, logger)
		if err != nil {
			logger.Error("Failed to initialize SQS store", "error", err)
			queueCancel()
			return nil, fmt.Errorf("failed to initialize SQS store: %w", err)
		}
	default:
		queueCancel()
		return nil, fmt.Errorf("unsupported driver: %s", cfg.Driver)
	}

	var statsCollector *stats.Collector
	if cfg.StatsEnabled {
		statsOptions := stats.DefaultStatsOptions()
		statsOptions.Enabled = true
		statsCollector = stats.NewCollector(statsOptions)
	}

	q := &Queue{
		config:          cfg,
		store:           store,
		dispatcher:      dispatcher.NewDispatcher(store, statsCollector),
		worker:          worker.NewWorker(store, cfg, queueName, statsCollector, logger),
		queueName:       queueName,
		ShutdownTimeout: shutdownTimeout,
		cancelFunc:      queueCancel,
		statsCollector:  statsCollector,
		logger:          logger,
	}

	return q, nil
}

// Dispatch adds a new job to the queue for processing.
// The job will be stored in the backend and picked up by available workers.
func (q *Queue) Dispatch(job job.Job) error {
	return q.dispatcher.Dispatch(q.queueName, job)
}

// DispatchBatch adds multiple jobs to the queue for processing.
// The jobs will be stored in the backend and picked up by available workers.
func (q *Queue) DispatchBatch(jobs []job.Job) error {
	return q.dispatcher.DispatchBatch(q.queueName, jobs)
}

// StartWorkers launches the specified number of worker goroutines to process jobs.
// The workers will continue running until the context is cancelled.
func (q *Queue) StartWorkers(ctx context.Context, count int) {
	q.worker.Start(ctx, count)
}

// IsHealthy checks if the queue and its backend storage are functioning properly.
// For Redis-backed queues, this includes checking the Redis connection health.
func (q *Queue) IsHealthy() bool {
	if q.config.Driver == config.DriverRedis {
		if redisStore, ok := q.store.(*memory.RedisStore); ok {
			return redisStore.IsHealthy()
		}
	}
	return true
}

// Stats returns current queue statistics including health status and performance metrics.
// If statistics collection is disabled, only basic health information is returned.
func (q *Queue) Stats() stats.QueueStats {
	if q.statsCollector == nil {
		return stats.QueueStats{
			IsHealthy:   q.IsHealthy(),
			LastUpdated: time.Now(),
		}
	}

	return q.statsCollector.GetStats(q.IsHealthy())
}

// IsOverloaded checks if the queue is currently experiencing high load
// based on configured thresholds. Returns false if statistics collection is disabled.
func (q *Queue) IsOverloaded() bool {
	if q.statsCollector == nil {
		return false
	}

	stats := q.statsCollector.GetStats(q.IsHealthy())
	return stats.IsOverloaded
}

// Shutdown gracefully stops the queue, waiting for in-progress jobs to complete
// up to the configured shutdown timeout duration. It cancels the queue context
// and stops all workers.
func (q *Queue) Shutdown(ctx context.Context) error {
	shutdownCtx, cancel := context.WithTimeout(ctx, q.ShutdownTimeout)
	defer cancel()

	if q.cancelFunc != nil {
		q.cancelFunc()
	}

	q.logger.Info("Shutting down queue", "queue", q.queueName)
	return q.worker.Shutdown(shutdownCtx)
}

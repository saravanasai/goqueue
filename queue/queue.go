package queue

import (
	"context"
	"fmt"
	"time"

	"github.com/saravanasai/goqueue/adapter"
	"github.com/saravanasai/goqueue/adapter/memory"
	"github.com/saravanasai/goqueue/config"
	"github.com/saravanasai/goqueue/dispatcher"
	"github.com/saravanasai/goqueue/internal/logger"
	"github.com/saravanasai/goqueue/internal/manager"
	"github.com/saravanasai/goqueue/internal/stats"
	"github.com/saravanasai/goqueue/job"
	"github.com/saravanasai/goqueue/worker"
)

// Queue is the main job queue instance.
type Queue struct {
	config          config.Config
	store           adapter.Store
	dispatcher      *dispatcher.Dispatcher
	worker          *worker.Worker
	queueName       string
	ShutdownTimeout time.Duration
	cancelFunc      context.CancelFunc
	statsCollector  *stats.Collector
	logger          logger.Logger
}

// NewQueue initializes a new Queue instance based on the config.
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

func (q *Queue) Dispatch(job job.Job) error {
	return q.dispatcher.Dispatch(q.queueName, job)
}

func (q *Queue) StartWorkers(ctx context.Context, count int) {
	q.worker.Start(ctx, count)
}

func (q *Queue) IsHealthy() bool {
	if q.config.Driver == config.DriverRedis {
		if redisStore, ok := q.store.(*memory.RedisStore); ok {
			return redisStore.IsHealthy()
		}
	}
	return true
}

func (q *Queue) Stats() stats.QueueStats {
	if q.statsCollector == nil {
		return stats.QueueStats{
			IsHealthy:   q.IsHealthy(),
			LastUpdated: time.Now(),
		}
	}

	return q.statsCollector.GetStats(q.IsHealthy())
}

func (q *Queue) IsOverloaded() bool {
	if q.statsCollector == nil {
		return false
	}

	stats := q.statsCollector.GetStats(q.IsHealthy())
	return stats.IsOverloaded
}

func (q *Queue) Shutdown(ctx context.Context) error {
	shutdownCtx, cancel := context.WithTimeout(ctx, q.ShutdownTimeout)
	defer cancel()

	if q.cancelFunc != nil {
		q.cancelFunc()
	}

	q.logger.Info("Shutting down queue", "queue", q.queueName)
	return q.worker.Shutdown(shutdownCtx)
}

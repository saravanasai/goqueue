package queue

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/saravanasai/goqueue/adapter"
	"github.com/saravanasai/goqueue/adapter/memory"
	"github.com/saravanasai/goqueue/config"
	"github.com/saravanasai/goqueue/dispatcher"
	"github.com/saravanasai/goqueue/internal/manager"
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
}

// NewQueue initializes a new Queue instance based on the config.
func NewQueue(queueName string, cfg config.Config, shutdownTimeout time.Duration) (*Queue, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	var store adapter.Store

	switch cfg.Driver {
	case config.DriverMemory:
		store = memory.NewInMemoryStore(queueName)
	case config.DriverRedis:
		redisCfg, ok := cfg.DriverConfig.(config.RedisConfig)
		if !ok {
			log.Fatal("Invalid Redis config provided")
		}
		redisMgr := manager.NewRedisClientManager()
		client := redisMgr.GetClient(redisCfg.Addr, redisCfg.Password, redisCfg.Db)
		store = memory.NewRedisStore(client)

	default:
		return nil, fmt.Errorf("unsupported driver: %s", cfg.Driver)
	}

	q := &Queue{
		config:          cfg,
		store:           store,
		dispatcher:      dispatcher.NewDispatcher(store),
		worker:          worker.NewWorker(store, cfg, queueName),
		queueName:       queueName,
		ShutdownTimeout: shutdownTimeout,
	}

	return q, nil
}

func (q *Queue) Dispatch(job job.Job) error {
	return q.dispatcher.Dispatch(q.queueName, job)
}

func (q *Queue) StartWorkers(ctx context.Context, count int) {
	q.worker.Start(ctx, count)
}

func (q *Queue) Shutdown(ctx context.Context) error {
	shutdownCtx, cancel := context.WithTimeout(ctx, q.ShutdownTimeout)
	defer cancel()

	log.Printf("Shutting down queue '%s'", q.queueName)
	return q.worker.Shutdown(shutdownCtx)
}

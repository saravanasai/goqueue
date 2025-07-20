package queue

import (
	"context"
	"fmt"

	"github.com/saravanasai/goqueue/adapter"
	"github.com/saravanasai/goqueue/adapter/memory"
	"github.com/saravanasai/goqueue/config"
	"github.com/saravanasai/goqueue/dispatcher"
	"github.com/saravanasai/goqueue/job"
	"github.com/saravanasai/goqueue/worker"
)

// Queue is the main job queue instance.
type Queue struct {
	config     config.Config
	store      adapter.Store
	dispatcher *dispatcher.Dispatcher
	worker     *worker.Worker
}

// NewQueue initializes a new Queue instance based on the config.
func NewQueue(cfg config.Config) (*Queue, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	var store adapter.Store

	switch cfg.Driver {
	case config.DriverMemory:
		store = memory.NewInMemoryStore()

	default:
		return nil, fmt.Errorf("unsupported driver: %s", cfg.Driver)
	}

	q := &Queue{
		config:     cfg,
		store:      store,
		dispatcher: dispatcher.NewDispatcher(store),
		worker:     worker.NewWorker(store, cfg),
	}

	return q, nil
}

// Dispatch pushes a job to the store for processing.
func (q *Queue) Dispatch(job job.Job) error {
	return q.dispatcher.Dispatch(q.config.QueueName, job)
}

func (q *Queue) StartWorkers(ctx context.Context) {
	q.worker.Start(ctx)
}

// Shutdown gracefully stops the consumer and cleans up.
func (q *Queue) Shutdown(ctx context.Context) error {
	shutdownCtx, cancel := context.WithTimeout(ctx, q.config.ShutdownTimeout)
	defer cancel()
	return q.worker.Shutdown(shutdownCtx)
}

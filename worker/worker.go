package worker

import (
	"context"

	"github.com/saravanasai/goqueue/adapter"
	"github.com/saravanasai/goqueue/config"
)

type Worker struct {
	store  adapter.Store
	config config.Config
}

func NewWorker(store adapter.Store, config config.Config) *Worker {
	return &Worker{store: store, config: config}
}

func (w *Worker) Start(ctx context.Context)

func (w *Worker) Shutdown(ctx context.Context) error {
	return nil
}

package worker

import (
	"context"
	"log"

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

func (w *Worker) Start(ctx context.Context) {

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				job, err := w.store.Pop(w.config.QueueName)
				if err != nil {
					log.Println("pop error:", err)
					continue
				}
				job.Job.Process(ctx)
			}
		}
	}()
}

func (w *Worker) Shutdown(ctx context.Context) error {
	return nil
}

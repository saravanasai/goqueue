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

	if w.config.Driver == config.DriverMemory {
		for i := 0; i < w.config.NumWorkers; i++ {
			go func(workerID int) {
				for {
					select {
					case <-ctx.Done():
						log.Printf("Worker %d shutting down", workerID)
						return
					default:
						job, err := w.store.Pop(w.config.QueueName)
						if err != nil {
							log.Printf("Worker %d pop error: %v", workerID, err)
							continue
						}
						job.Job.Process(ctx)
					}
				}
			}(i)
		}
	}

}

func (w *Worker) Shutdown(ctx context.Context) error {
	return nil
}

package worker

import (
	"context"
	"log"

	"github.com/saravanasai/goqueue/adapter"
	"github.com/saravanasai/goqueue/config"
)

type Worker struct {
	store     adapter.Store
	config    config.Config
	queueName string
}

func NewWorker(store adapter.Store, config config.Config, queueName string) *Worker {
	return &Worker{store: store, config: config, queueName: queueName}
}

func (w *Worker) Start(ctx context.Context, noOfWorkers int) {
	if w.config.Driver == config.DriverMemory || w.config.Driver == config.DriverRedis {
		for i := 0; i < noOfWorkers; i++ {
			go func(workerID int) {
				for {
					select {
					case <-ctx.Done():
						log.Printf("Worker %d shutting down", workerID)
						return
					default:
						job, err := w.store.Pop(w.queueName)
						if err != nil {
							log.Printf("Worker %d pop error: %v", workerID, err)
							continue
						}
						isJobCompleted := job.Job.Process(ctx)
						if isJobCompleted == nil {
							isAck := w.store.Ack(w.queueName, job.JobID)
							log.Printf("Done %d Job: %v", job.JobID, isAck)
						}
					}
				}
			}(i)
		}
	}

}

func (w *Worker) Shutdown(ctx context.Context) error {
	return nil
}

package worker

import (
	"context"
	"log"
	"time"

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
				log.Println("[worker] context canceled, shutting down consumer")
				return
			default:
				job, err := w.store.Pop(w.config.QueueName)
				if err != nil {
					log.Printf("[worker] error popping job from queue '%s': %v\n", w.config.QueueName, err)
					continue
				}
				if job == nil {
					log.Printf("[worker] no job found in queue '%s', sleeping...\n", w.config.QueueName)
					time.Sleep(1000 * time.Millisecond)
					continue
				}

				log.Printf("[worker] fetched job from queue '%s', starting processing\n", w.config.QueueName)
				start := time.Now()
				job.Process(ctx)
				duration := time.Since(start)
				log.Printf("[worker] finished processing job in %s\n", duration)

				// TODO: add ack/retry logic here
			}
		}
	}()
}

func (w *Worker) Shutdown(ctx context.Context) error {
	return nil
}

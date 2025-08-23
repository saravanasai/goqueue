package dispatcher

import (
	"time"

	"github.com/saravanasai/goqueue/adapter"
	"github.com/saravanasai/goqueue/internal/stats"
	"github.com/saravanasai/goqueue/job"
)

type Dispatcher struct {
	store          adapter.Store
	statsCollector *stats.Collector
}

func NewDispatcher(store adapter.Store, statsCollector *stats.Collector) *Dispatcher {
	return &Dispatcher{
		store:          store,
		statsCollector: statsCollector,
	}
}

func (ds *Dispatcher) Dispatch(queueName string, job job.Job) error {
	err := ds.store.Push(queueName, job)
	ds.recordEnqueueN(1)
	return err
}

// DispatchWithDelay adds a job to the store for the given queue after a delay.
func (ds *Dispatcher) DispatchWithDelay(queueName string, job job.Job, delay time.Duration) error {
	err := ds.store.Push(queueName, job, delay)
	ds.recordEnqueueN(1)
	return err
}

// DispatchBatch adds multiple jobs to the queue in a single call.
func (ds *Dispatcher) DispatchBatch(queueName string, jobs []job.Job) error {
	err := ds.store.PushBatch(queueName, jobs)
	ds.recordEnqueueN(len(jobs))
	return err
}

// DispatchBatchWithDelay adds multiple jobs to the store for the given queue after a delay.
func (ds *Dispatcher) DispatchBatchWithDelay(queueName string, jobs []job.Job, delay time.Duration) error {
	err := ds.store.PushBatch(queueName, jobs, delay)
	ds.recordEnqueueN(len(jobs))
	return err
}

// recordEnqueueN records n enqueue events in statsCollector if enabled.
func (ds *Dispatcher) recordEnqueueN(n int) {
	if ds.statsCollector != nil {
		for i := 0; i < n; i++ {
			ds.statsCollector.RecordEnqueue()
		}
	}
}

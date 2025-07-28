package dispatcher

import (
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
	if ds.statsCollector != nil {
		ds.statsCollector.RecordEnqueue()
	}
	return err
}

package dispatcher

import (
	"github.com/saravanasai/goqueue/adapter"
	"github.com/saravanasai/goqueue/job"
)

type Dispatcher struct {
	store adapter.Store
}

func NewDispatcher(store adapter.Store) *Dispatcher {

	return &Dispatcher{store: store}
}

func (ds *Dispatcher) Dispatch(queueName string, job job.Job) error {
	return nil
}

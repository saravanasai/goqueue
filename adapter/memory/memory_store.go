package memory

import (
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/saravanasai/goqueue/job"
)

var idCounter uint64

func generateID() string {
	return strconv.FormatUint(atomic.AddUint64(&idCounter, 1), 10)
}

type queue struct {
	jobChannel chan job.QueuedJob
}
type InMemoryStore struct {
	Queue map[string]*queue
}

func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{Queue: make(map[string]*queue)}
}

func (store *InMemoryStore) Push(queueName string, jb job.Job) error {
	q, ok := store.Queue[queueName]
	if !ok {
		q = &queue{make(chan job.QueuedJob, 1000)}
		store.Queue[queueName] = q
	}
	meta := job.QueuedJob{
		Job:        jb,
		ID:         generateID(),
		EnqueuedAt: time.Now(),
		RetryCount: 0,
	}
	q.jobChannel <- meta
	return nil
}

func (store *InMemoryStore) Pop(queueName string) (job.Job, error) {

	q, ok := store.Queue[queueName]
	if !ok {
		return nil, fmt.Errorf("queue not found")
	}
	job := <-q.jobChannel
	return job.Job, nil

}

func (store *InMemoryStore) Ack(jobID string) error {
	return nil
}

func (store *InMemoryStore) Retry(job job.Job, delay time.Duration) error {
	return nil
}

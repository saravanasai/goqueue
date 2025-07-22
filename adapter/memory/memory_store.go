package memory

import (
	"fmt"
	"time"

	"github.com/saravanasai/goqueue/job"
)

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

func (store *InMemoryStore) Pop(queueName string) (job.JobContext, error) {

	q, ok := store.Queue[queueName]
	if !ok {
		return job.JobContext{}, fmt.Errorf("queue not found")
	}
	popedJob := <-q.jobChannel
	return job.JobContext{Job: popedJob.Job, JobID: popedJob.ID, QueueName: queueName}, nil

}

func (store *InMemoryStore) Ack(queueName string, payload string) error {
	return nil
}

func (store *InMemoryStore) Retry(job job.Job, delay time.Duration) error {
	return nil
}

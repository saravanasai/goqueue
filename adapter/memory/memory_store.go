package memory

import (
	"time"

	"github.com/saravanasai/goqueue/job"
)

type InMemoryStore struct {
	queue []byte
}

func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{queue: make([]byte, 3)}
}

func (store *InMemoryStore) Push(queue string, job job.Job) error {
	return nil
}

func (store *InMemoryStore) Pop(queue string) (job.Job, error) {
	return nil, nil
}

func (store *InMemoryStore) Ack(jobID string) error {
	return nil
}

func (store *InMemoryStore) Retry(job job.Job, delay time.Duration) error {
	return nil
}

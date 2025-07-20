package memory

import (
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
	jobs []job.QueuedJob
	head int
}
type InMemoryStore struct {
	queue map[string]*queue
}

func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{queue: make(map[string]*queue)}
}

func (store *InMemoryStore) Push(queueName string, jb job.Job) error {
	q, ok := store.queue[queueName]
	if !ok {
		q = &queue{jobs: make([]job.QueuedJob, 0), head: 0}
		store.queue[queueName] = q
	}
	meta := job.QueuedJob{
		Job:        jb,
		ID:         generateID(), // implement this
		EnqueuedAt: time.Now(),
		RetryCount: 0,
	}
	q.jobs = append(q.jobs, meta)
	return nil
}

func (store *InMemoryStore) Pop(queueName string) (job.Job, error) {
	q, ok := store.queue[queueName]
	if !ok || q.head >= len(q.jobs) {
		return nil, nil
	}
	// Get the queuedJob at the head
	meta := q.jobs[q.head]
	q.head++
	// Compact the slice if head is large to avoid memory leak
	if q.head > 100 && q.head > len(q.jobs)/2 {
		q.jobs = q.jobs[q.head:]
		q.head = 0
	}
	return meta.Job, nil

}

func (store *InMemoryStore) Ack(jobID string) error {
	return nil
}

func (store *InMemoryStore) Retry(job job.Job, delay time.Duration) error {
	return nil
}

package memory

import (
	"fmt"
	"time"

	"github.com/saravanasai/goqueue/adapter/utils"
	"github.com/saravanasai/goqueue/config"
	"github.com/saravanasai/goqueue/internal/logger"
	"github.com/saravanasai/goqueue/job"
)

type queue struct {
	jobChannel chan job.QueuedJob
}
type InMemoryStore struct {
	Queue  map[string]*queue
	Config config.Config
	Logger logger.Logger
}

func NewInMemoryStore(queueName string, config config.Config, logger logger.Logger) *InMemoryStore {
	return &InMemoryStore{Queue: make(map[string]*queue), Config: config, Logger: logger}
}

func (store *InMemoryStore) Push(queueName string, jb job.Job) error {
	q, ok := store.Queue[queueName]
	if !ok {
		q = &queue{make(chan job.QueuedJob, 1000)}
		store.Queue[queueName] = q
	}
	meta := job.QueuedJob{
		Job:        jb,
		ID:         utils.GenerateID(),
		EnqueuedAt: time.Now(),
		RetryCount: 0,
	}
	q.jobChannel <- meta
	return nil
}

// PushBatch adds multiple jobs to the in-memory queue.
func (store *InMemoryStore) PushBatch(queueName string, jobs []job.Job) error {
	q, ok := store.Queue[queueName]
	if !ok {
		q = &queue{make(chan job.QueuedJob, 1000)}
		store.Queue[queueName] = q
	}
	for _, jb := range jobs {
		meta := job.QueuedJob{
			Job:        jb,
			ID:         utils.GenerateID(),
			EnqueuedAt: time.Now(),
			RetryCount: 0,
		}
		q.jobChannel <- meta
	}
	return nil
}

func (store *InMemoryStore) Pop(queueName string) (job.JobContext, error) {

	q, ok := store.Queue[queueName]
	if !ok {
		store.Logger.Error("queue not found", "queue", queueName)
		return job.JobContext{}, fmt.Errorf("queue not found")
	}
	popedJob := <-q.jobChannel
	return job.JobContext{Job: popedJob.Job, JobID: popedJob.ID, QueueName: queueName, EnqueuedAt: popedJob.EnqueuedAt}, nil

}

func (store *InMemoryStore) Ack(queueName string, payload string) error {
	return nil
}

func (store *InMemoryStore) Retry(job job.Job, delay time.Duration) error {
	return nil
}

func (store *InMemoryStore) EnqueueMetrics(metrics config.JobMetrics) error {
	if store.Config.OnJobComplete != nil {
		store.Config.OnJobComplete(metrics)
	}
	return nil
}

func (store *InMemoryStore) DequeueMetrics(queueName string) (config.JobMetrics, error) {
	return config.JobMetrics{}, nil
}

func (store *InMemoryStore) IsHealthy() bool {
	return true
}

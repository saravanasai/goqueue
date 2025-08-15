package memory

import (
	"fmt"
	"sync"
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
	Queue      map[string]*queue
	Config     config.Config
	Logger     logger.Logger
	mu         sync.Mutex
	processing map[string]map[string]job.QueuedJob // queueName -> jobID -> queuedJob
	metrics    map[string]chan config.JobMetrics   // queueName -> metrics channel
}

func NewInMemoryStore(queueName string, cfg config.Config, logger logger.Logger) *InMemoryStore {
	return &InMemoryStore{
		Queue:      make(map[string]*queue),
		Config:     cfg,
		Logger:     logger,
		processing: make(map[string]map[string]job.QueuedJob),
		metrics:    make(map[string]chan config.JobMetrics),
	}
}

func (store *InMemoryStore) ensureQueueInitialized(queueName string) {
	if _, ok := store.Queue[queueName]; !ok {
		store.Queue[queueName] = &queue{make(chan job.QueuedJob, 1000)}
	}
	if _, ok := store.processing[queueName]; !ok {
		store.processing[queueName] = make(map[string]job.QueuedJob)
	}
	if _, ok := store.metrics[queueName]; !ok {
		store.metrics[queueName] = make(chan config.JobMetrics, 100)
	}
}

func (store *InMemoryStore) Push(queueName string, jb job.Job) error {
	store.mu.Lock()
	store.ensureQueueInitialized(queueName)
	q := store.Queue[queueName]
	store.mu.Unlock()

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
	store.mu.Lock()
	store.ensureQueueInitialized(queueName)
	q := store.Queue[queueName]
	store.mu.Unlock()

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
	store.mu.Lock()
	q, ok := store.Queue[queueName]
	if !ok {
		store.mu.Unlock()
		store.Logger.Error("queue not found", "queue", queueName)
		return job.JobContext{}, fmt.Errorf("queue not found")
	}
	store.mu.Unlock()

	popedJob := <-q.jobChannel

	// move to processing
	store.mu.Lock()
	store.ensureQueueInitialized(queueName)
	store.processing[queueName][popedJob.ID] = popedJob
	store.mu.Unlock()

	return job.JobContext{Job: popedJob.Job, JobID: popedJob.ID, QueueName: queueName, EnqueuedAt: popedJob.EnqueuedAt, RetryCount: popedJob.RetryCount}, nil

}

func (store *InMemoryStore) Ack(queueName string, payload string) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if _, ok := store.processing[queueName]; !ok {
		return fmt.Errorf("no processing queue for %s", queueName)
	}
	if _, exists := store.processing[queueName][payload]; !exists {
		return fmt.Errorf("job id %s not found in processing queue %s", payload, queueName)
	}
	delete(store.processing[queueName], payload)
	return nil
}

func (store *InMemoryStore) Retry(j job.Job, delay time.Duration) error {
	if j == nil {
		return fmt.Errorf("job is nil")
	}
	// Determine a target queue based on job type
	qname := utils.GetJobName(j)
	if qname == "" {
		qname = "default"
	}
	// Schedule requeue after delay
	go func() {
		time.Sleep(delay)
		_ = store.Push(qname, j)
	}()
	return nil
}

func (store *InMemoryStore) EnqueueMetrics(metrics config.JobMetrics) error {
	// Call callback if provided
	if store.Config.OnJobComplete != nil {
		store.Config.OnJobComplete(metrics)
	}
	store.mu.Lock()
	if _, ok := store.metrics[metrics.QueueName]; !ok {
		store.metrics[metrics.QueueName] = make(chan config.JobMetrics, 100)
	}
	ch := store.metrics[metrics.QueueName]
	store.mu.Unlock()

	select {
	case ch <- metrics:
	default:
		// drop if channel full
	}
	return nil
}

func (store *InMemoryStore) DequeueMetrics(queueName string) (config.JobMetrics, error) {
	store.mu.Lock()
	ch, ok := store.metrics[queueName]
	store.mu.Unlock()
	if !ok {
		return config.JobMetrics{}, nil
	}
	select {
	case m := <-ch:
		return m, nil
	case <-time.After(1 * time.Second):
		return config.JobMetrics{}, nil
	}
}

func (store *InMemoryStore) IsHealthy() bool {
	return true
}

package memory

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/danish-a1/goqueue/adapter/utils"
	"github.com/danish-a1/goqueue/config"
	"github.com/danish-a1/goqueue/internal/logger"
	"github.com/danish-a1/goqueue/job"
)

type scheduledJob struct {
	QueuedJob job.QueuedJob
	EnqueueAt time.Time
}

type queue struct {
	mu   sync.Mutex
	jobs []*scheduledJob
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
		store.Queue[queueName] = &queue{
			jobs: []*scheduledJob{},
		}
	}
	if _, ok := store.processing[queueName]; !ok {
		store.processing[queueName] = make(map[string]job.QueuedJob)
	}
	if _, ok := store.metrics[queueName]; !ok {
		store.metrics[queueName] = make(chan config.JobMetrics, 100)
	}
}

func (store *InMemoryStore) GetDbConnection() interface{} {
	return nil
}

func (store *InMemoryStore) Push(queueName string, jb job.Job, delay ...time.Duration) error {
	store.mu.Lock()
	store.ensureQueueInitialized(queueName)
	q := store.Queue[queueName]
	store.mu.Unlock()

	enqueueAt := time.Now()
	if len(delay) > 0 && delay[0] > 0 {
		enqueueAt = enqueueAt.Add(delay[0])
	}
	meta := job.QueuedJob{
		Job:        jb,
		ID:         utils.GenerateID(),
		EnqueuedAt: enqueueAt,
		RetryCount: 0,
	}
	sj := &scheduledJob{QueuedJob: meta, EnqueueAt: enqueueAt}

	q.mu.Lock()
	q.jobs = append(q.jobs, sj)
	// Sort jobs by EnqueueAt ascending
	sort.Slice(q.jobs, func(i, j int) bool {
		return q.jobs[i].EnqueueAt.Before(q.jobs[j].EnqueueAt)
	})
	q.mu.Unlock()
	return nil
}

func (store *InMemoryStore) PushBatch(queueName string, jobs []job.Job, delay ...time.Duration) error {
	store.mu.Lock()
	store.ensureQueueInitialized(queueName)
	q := store.Queue[queueName]
	store.mu.Unlock()

	enqueueAt := time.Now()
	if len(delay) > 0 && delay[0] > 0 {
		enqueueAt = enqueueAt.Add(delay[0])
	}
	q.mu.Lock()
	for _, jb := range jobs {
		meta := job.QueuedJob{
			Job:        jb,
			ID:         utils.GenerateID(),
			EnqueuedAt: enqueueAt,
			RetryCount: 0,
		}
		sj := &scheduledJob{QueuedJob: meta, EnqueueAt: enqueueAt}
		q.jobs = append(q.jobs, sj)
	}
	sort.Slice(q.jobs, func(i, j int) bool {
		return q.jobs[i].EnqueueAt.Before(q.jobs[j].EnqueueAt)
	})
	q.mu.Unlock()
	return nil
}

func (store *InMemoryStore) Pop(queueName string) (job.JobContext, error) {
	store.mu.Lock()
	q, ok := store.Queue[queueName]
	store.mu.Unlock()
	if !ok {
		store.Logger.Error("queue not found", "queue", queueName)
		return job.JobContext{}, fmt.Errorf("queue not found")
	}

	// Check for scheduled jobs ready to run
	q.mu.Lock()
	var popedJob job.QueuedJob
	now := time.Now()
	idx := -1
	for i, sj := range q.jobs {
		if !sj.EnqueueAt.After(now) {
			popedJob = sj.QueuedJob
			idx = i
			break
		}
	}
	if idx >= 0 {
		// Remove from jobs slice
		q.jobs = append(q.jobs[:idx], q.jobs[idx+1:]...)
	}
	q.mu.Unlock()

	if idx == -1 {
		// No job ready to run
		return job.JobContext{}, fmt.Errorf("no job ready to run")
	}

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
		return fmt.Errorf("job cannot be nil")
	}

	return store.Push("", j, delay)
}

func (store *InMemoryStore) RetryJobWithMetadata(queueName string, rJob job.JobContext, delay ...time.Duration) error {
	store.mu.Lock()
	store.ensureQueueInitialized(queueName)
	q := store.Queue[queueName]
	store.mu.Unlock()

	// Increment retry count
	rJob.RetryCount += 1

	// Calculate enqueue time with delay
	enqueueAt := time.Now()
	if len(delay) > 0 && delay[0] > 0 {
		enqueueAt = enqueueAt.Add(delay[0])
	}

	// Create queued job with the same ID
	meta := job.QueuedJob{
		Job:        rJob.Job,
		ID:         rJob.JobID, // Keep the same job ID
		EnqueuedAt: enqueueAt,
		RetryCount: rJob.RetryCount,
	}

	sj := &scheduledJob{QueuedJob: meta, EnqueueAt: enqueueAt}

	q.mu.Lock()
	q.jobs = append(q.jobs, sj)
	// Sort jobs by EnqueueAt ascending
	sort.Slice(q.jobs, func(i, j int) bool {
		return q.jobs[i].EnqueueAt.Before(q.jobs[j].EnqueueAt)
	})
	q.mu.Unlock()

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

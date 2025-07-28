package goqueue

import (
	"context"
	"time"

	"github.com/saravanasai/goqueue/config"
	"github.com/saravanasai/goqueue/internal/registry"
	"github.com/saravanasai/goqueue/internal/stats"
	"github.com/saravanasai/goqueue/job"
	"github.com/saravanasai/goqueue/queue"
)

// Job is the interface clients must implement for their jobs.
type Job = job.Job
type Queue = queue.Queue
type JobMetrics = config.JobMetrics
type QueueStats = stats.QueueStats

const DefaultShutdownTimeout = 5 * time.Second

func NewQueueWithDefaults(queueName string, cfg config.Config) (*Queue, error) {
	return NewQueue(queueName, cfg, DefaultShutdownTimeout)
}

func NewQueue(queueName string, cfg config.Config, shutdownTimeout time.Duration) (*Queue, error) {
	return queue.NewQueue(queueName, cfg, shutdownTimeout)
}

func RegisterJob(name string, constructor func() Job) {
	registry.Register(name, constructor)
}

func Dispatch(q *queue.Queue, payload job.Job) error {
	return q.Dispatch(payload)
}

func StartWorker(q *queue.Queue, ctx context.Context, count int) {
	q.StartWorkers(ctx, count)
}

func Shutdown(q *queue.Queue, ctx context.Context) error {
	return q.Shutdown(ctx)
}

func GetQueueStats(q *queue.Queue) QueueStats {
	return q.Stats()
}

func IsQueueOverloaded(q *queue.Queue) bool {
	return q.IsOverloaded()
}

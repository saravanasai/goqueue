package adapter

import (
	"time"

	"github.com/saravanasai/goqueue/config"
	"github.com/saravanasai/goqueue/job"
)

type Store interface {
	// Push adds a single job to the queue.
	Push(queueName string, job job.Job) error
	// PushBatch adds multiple jobs to the queue in a single call.
	PushBatch(queueName string, jobs []job.Job) error
	Pop(queueName string) (job.JobContext, error)
	Ack(queueName string, payload string) error
	Retry(job job.Job, delay time.Duration) error
	EnqueueMetrics(metrics config.JobMetrics) error
	DequeueMetrics(queueName string) (config.JobMetrics, error)
	IsHealthy() bool
}

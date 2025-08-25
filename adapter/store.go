package adapter

import (
	"time"

	"github.com/saravanasai/goqueue/config"
	"github.com/saravanasai/goqueue/job"
)

type Store interface {
	// Push adds a single job to the queue.
	Push(queueName string, job job.Job, time ...time.Duration) error
	GetDbConnection() interface{}
	// PushBatch adds multiple jobs to the queue in a single call.
	PushBatch(queueName string, jobs []job.Job, time ...time.Duration) error
	Pop(queueName string) (job.JobContext, error)
	Ack(queueName string, payload string) error
	Retry(job job.Job, delay time.Duration) error
	RetryJobWithMetadata(queueName string, job job.JobContext, delay ...time.Duration) error
	EnqueueMetrics(metrics config.JobMetrics) error
	DequeueMetrics(queueName string) (config.JobMetrics, error)
	IsHealthy() bool
}

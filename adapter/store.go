package adapter

import (
	"time"

	"github.com/saravanasai/goqueue/job"
)

type Store interface {
	Push(queueName string, job job.Job) error
	Pop(queueName string) (job.Job, error)
	Ack(jobID string) error
	Retry(job job.Job, delay time.Duration) error
}

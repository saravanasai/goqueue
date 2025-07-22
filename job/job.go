package job

import (
	"context"
	"encoding/json"
	"time"
)

type Job interface {
	Process(ctx context.Context) error
}

type JobContext struct {
	Job       Job
	JobID     string
	QueueName string
}

type QueuedJob struct {
	Job        Job
	ID         string
	EnqueuedAt time.Time
	RetryCount int
}

type RedisQueuedJob struct {
	Job        json.RawMessage `json:"job"`
	ID         string          `json:"id"`
	JobName    string          `json:"job_name"`
	EnqueuedAt time.Time       `json:"enqueued_at"`
	RetryCount int             `json:"retry_count"`
}

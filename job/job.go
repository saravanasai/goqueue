package job

import (
	"context"
	"time"
)

type Job interface {
	Process(ctx context.Context)
}

type QueuedJob struct {
	Job        Job
	ID         string
	EnqueuedAt time.Time
	RetryCount int
}

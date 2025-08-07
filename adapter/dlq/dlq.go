package dlq

import (
	"context"

	"github.com/saravanasai/goqueue/job"
)

// DLQAdapter defines the interface for Dead Letter Queue implementations
type DLQAdapter interface {
	// Push adds a failed job to the dead letter queue
	Push(ctx context.Context, job *job.JobContext, err error) error
}
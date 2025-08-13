// Package dlq provides the Dead Letter Queue (DLQ) interface for handling failed jobs.
// DLQ implementations can store failed jobs for later inspection, retry, or cleanup.
package dlq

import (
	"context"

	"github.com/saravanasai/goqueue/job"
)

// DLQAdapter defines the interface for Dead Letter Queue implementations.
// A DLQ stores jobs that have failed processing after exhausting all retry attempts.
// This allows for manual inspection, debugging, and potential reprocessing of failed jobs.
type DLQAdapter interface {
	// Push adds a failed job to the dead letter queue along with its error information.
	// The context can be used for cancellation and timeout control.
	// The job parameter contains the failed job and its processing context.
	// The err parameter contains the error that caused the job to fail.
	//
	// Returns an error if the job cannot be added to the DLQ.
	Push(ctx context.Context, job *job.JobContext, err error) error
}

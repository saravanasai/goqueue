package jobs

import (
	"context"
	"fmt"

	"github.com/saravanasai/goqueue"
)

// EmailJob implements the job.Job interface
type EmailJob struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
}

// Process implements the job.Job interface
func (e *EmailJob) Process(ctx context.Context) error {
	fmt.Printf("Sending email to %s: %s\n", e.To, e.Subject)
	return nil
}

func init() {
	// Register the EmailJob type for serialization
	goqueue.RegisterJob("EmailJob", func() goqueue.Job {
		return &EmailJob{}
	})
}

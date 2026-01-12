package jobs

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/danish-a1/goqueue"
)

// EmailJob implements the job.Job interface
type FailJob struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
}

// Process implements the job.Job interface
func (e *FailJob) Process(ctx context.Context) error {

	time.Sleep(5 * time.Second)
	fmt.Printf("Sending email to %s: %s\n", e.To, e.Subject)
	return errors.New("Failed job process due to failed job")
}

// Register job type for serialization
func init() {
	goqueue.RegisterJob("FailJob", func() goqueue.Job {
		return &FailJob{}
	})
}

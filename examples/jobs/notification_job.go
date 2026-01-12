package jobs

import (
	"context"
	"fmt"
	"time"

	"github.com/danish-a1/goqueue"
)

// NotificationJob demonstrates a job that can be delayed
type NotificationJob struct {
	UserID      string    `json:"user_id"`
	Message     string    `json:"message"`
	ScheduledAt time.Time `json:"scheduled_at"`
}

// Process implements the job.Job interface
func (n *NotificationJob) Process(ctx context.Context) error {
	fmt.Printf("[%s] Notification for user %s: %s (scheduled for: %s)\n",
		time.Now().Format(time.RFC3339),
		n.UserID,
		n.Message,
		n.ScheduledAt.Format(time.RFC3339))
	return nil
}

// Register job type for serialization
func init() {
	goqueue.RegisterJob("NotificationJob", func() goqueue.Job {
		return &NotificationJob{}
	})
}

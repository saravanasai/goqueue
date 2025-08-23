package jobs

import (
	"context"
	"fmt"
	"time"
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

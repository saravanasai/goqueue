package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/danish-a1/goqueue"
	"github.com/danish-a1/goqueue/config"
	"github.com/danish-a1/goqueue/examples/jobs"
)

func main() {
	// Create a queue with in-memory backend
	cfg := config.NewInMemoryConfig()
	q, err := goqueue.NewQueueWithDefaults("email-queue", cfg)
	if err != nil {
		log.Fatalf("Failed to create queue: %v", err)
	}

	// Start worker pool with 2 concurrent workers
	ctx := context.Background()
	err = q.StartWorkers(ctx, 1)
	if err != nil {
		log.Fatalf("Failed to start workers: %v", err)
	}
	fmt.Println("Started worker pool with 2 workers")

	// Dispatch an email job
	emailJob := &jobs.EmailJob{
		To:      "user@example.com",
		Subject: "Welcome to GoQueue!",
	}

	fmt.Printf("Dispatching email job to: %s\n", emailJob.To)
	if err := q.Dispatch(emailJob); err != nil {
		log.Fatalf("Failed to dispatch job: %v", err)
	}
	fmt.Println("Email job dispatched successfully")

	// Create a notification job with delay
	now := time.Now()
	notificationJob := &jobs.NotificationJob{
		UserID:      "user123",
		Message:     "This is a delayed notification",
		ScheduledAt: now.Add(1 * time.Second),
	}

	// Calculate delay
	delay := notificationJob.ScheduledAt.Sub(now)
	if delay < 0 {
		delay = 0
	}

	fmt.Printf("Dispatching notification job with delay of %.1f seconds\n", delay.Seconds())
	if err := q.DispatchWithDelay(notificationJob, delay); err != nil {
		log.Fatalf("Failed to dispatch notification job: %v", err)
	}
	fmt.Println("Notification job dispatched successfully")

	// Wait to allow workers to process both jobs
	waitTime := 3 * time.Second
	fmt.Printf("Waiting %s for jobs to be processed...\n", waitTime)
	time.Sleep(waitTime)

	// Graceful shutdown
	fmt.Println("Shutting down queue...")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	q.Shutdown(shutdownCtx)
	fmt.Println("Queue shutdown complete")
}

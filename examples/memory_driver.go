package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/saravanasai/goqueue"
	"github.com/saravanasai/goqueue/config"
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

func main() {
	// Create a queue with in-memory backend
	cfg := config.NewInMemoryConfig()
	q, err := goqueue.NewQueueWithDefaults("email-queue", cfg)
	if err != nil {
		log.Fatalf("Failed to create queue: %v", err)
	}

	// Start worker pool with 2 concurrent workers
	ctx := context.Background()
	err = q.StartWorkers(ctx, 2)
	if err != nil {
		log.Fatalf("Failed to start workers: %v", err)
	}
	fmt.Println("Started worker pool with 2 workers")

	// Dispatch a job
	job := &EmailJob{
		To:      "user@example.com",
		Subject: "Welcome to GoQueue!",
	}

	fmt.Printf("Dispatching email job to: %s\n", job.To)
	if err := q.Dispatch(job); err != nil {
		log.Fatalf("Failed to dispatch job: %v", err)
	}
	fmt.Println("Job dispatched successfully")

	// Wait to allow workers to process the job
	fmt.Println("Waiting 2 seconds for job processing...")
	time.Sleep(2 * time.Second)

	// Graceful shutdown
	fmt.Println("Shutting down queue...")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	q.Shutdown(shutdownCtx)
	fmt.Println("Queue shutdown complete")
}

package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/saravanasai/goqueue"
	"github.com/saravanasai/goqueue/config"
	"github.com/saravanasai/goqueue/examples/jobs"
)

func main() {
	// NATS JetStream configuration - replace with your actual values
	natsURL := "nats://localhost:4222" // NATS server URL
	streamName := "email-queue"        // JetStream stream name jobs will be stored under

	// Create a queue with NATS JetStream backend
	cfg := config.NewNATSConfig(natsURL, streamName)

	// Optional: Configure additional settings if needed
	// cfg = cfg.WithMaxWorkers(5)
	// cfg = cfg.WithMaxRetryAttempts(3)
	// cfg = cfg.WithRetryDelay(5 * time.Second)

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

	// NATS JetStream schedules this via its native message scheduler,
	// holding the message and releasing it to the real subject once the
	// delay elapses - no polling or external timer needed.
	fmt.Printf("Dispatching delayed email job to: %s\n", emailJob.To)
	if err := q.DispatchWithDelay(emailJob, 1*time.Minute); err != nil {
		log.Fatalf("Failed to dispatch job: %v", err)
	}
	fmt.Println("Email job delayed dispatched successfully")

	// Wait to allow workers to process the job
	fmt.Println("Waiting 2 seconds for job processing...")
	time.Sleep(2 * time.Second)
}

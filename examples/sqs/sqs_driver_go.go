package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/saravanasai/goqueue"
	"github.com/saravanasai/goqueue/config"
	"github.com/saravanasai/goqueue/examples/jobs"
)

func main() {
	// AWS SQS configuration - replace with your actual values
	queueURL := "https://sqs.ap-south-1.amazonaws.com/xxxxsadasdasdxx/email-queue" // SQS queue URL
	region := "ap-south-1"                                                         // AWS region
	accessKeyID := ""                                                              // AWS access key ID
	secretAccessKey := ""                                                          // AWS secret access key

	// Create a queue with SQS backend
	cfg := config.NewSQSConfig(queueURL, region, accessKeyID, secretAccessKey)

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

	fmt.Printf("Dispatching delayed email job to: %s\n", emailJob.To)
	if err := q.DispatchWithDelay(emailJob, 1*time.Minute); err != nil {
		log.Fatalf("Failed to dispatch job: %v", err)
	}
	fmt.Println("Email job delayed dispatched successfully")

	// Wait to allow workers to process the job
	fmt.Println("Waiting 2 seconds for job processing...")
	time.Sleep(2 * time.Second)

	var wg sync.WaitGroup

	wg.Add(1)
	wg.Wait()

	// Graceful shutdown
	// fmt.Println("Shutting down queue...")
	// shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	// defer cancel()
	// q.Shutdown(shutdownCtx)
	// fmt.Println("Queue shutdown complete")
}

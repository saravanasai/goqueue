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
	// Redis connection parameters - replace with your actual values
	redisAddr := "localhost:6379" // Redis server address
	redisPassword := ""           // Redis password (if any)
	redisDB := 0                  // Redis database number

	// Create a queue with Redis backend
	cfg := config.NewRedisConfig(redisAddr, redisPassword, redisDB)
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
	job := &jobs.EmailJob{
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

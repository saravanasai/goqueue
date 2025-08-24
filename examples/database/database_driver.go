package main

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/saravanasai/goqueue"
	"github.com/saravanasai/goqueue/config"
	"github.com/saravanasai/goqueue/examples/jobs"
)

func main() {

	cfg := config.NewPostgresConfig("postgresql://postgres:root@db.wwuofuykolqrrtgkjyru.supabase.co:5432/postgres")

	q, err := goqueue.NewQueueWithDefaults("emails", cfg)
	if err != nil {
		log.Fatalf("Failed to create queue: %v", err)
	}

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

	// Start worker pool with 2 concurrent workers
	ctx := context.Background()
	err = q.StartWorkers(ctx, 1)
	if err != nil {
		log.Fatalf("Failed to start workers: %v", err)
	}

	var wg sync.WaitGroup

	wg.Add(1)
	wg.Wait()
}

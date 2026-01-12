package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/danish-a1/goqueue"
	"github.com/danish-a1/goqueue/config"
	"github.com/danish-a1/goqueue/examples/jobs"
	"github.com/danish-a1/goqueue/internal/logger"
	"github.com/danish-a1/goqueue/middleware"
)

func main() {

	// cfg := config.NewPostgresConfig("postgresql://postgres:root@db.wwuofuykolqrrtgkjyru.supabase.co:5432/postgres")

	cfg := config.NewMySQLConfig("root:root@tcp(localhost:3306)/wizpense?parseTime=true")

	cfg = cfg.WithMetricsCallback(func(metrics config.JobMetrics) {
		// Record metrics in your monitoring system (Prometheus, etc.)
		fmt.Printf("Job: %s, Queue: %s, Duration: %v, Error: %v\n",
			metrics.JobID, metrics.QueueName, metrics.Duration, metrics.Error)
	})

	cfg = cfg.WithMaxRetryAttempts(5)
	cfg = cfg.WithExponentialBackoff(true)
	cfg = cfg.WithRetryDelay(1 * time.Minute)
	logger := logger.NewZapLogger()
	loggingMiddleware := middleware.LoggingMiddleware(logger)
	cfg = cfg.WithMiddleware(loggingMiddleware)

	q, err := goqueue.NewQueueWithDefaults("emails", cfg)
	if err != nil {
		log.Fatalf("Failed to create queue: %v", err)
	}

	// Dispatch a job
	job := &jobs.EmailJob{
		To:      "user@example.com",
		Subject: "Welcome to GoQueue!",
	}

	failJob := &jobs.FailJob{
		To:      "fail@example.com",
		Subject: "Failed on GoQueue!",
	}

	q.Dispatch(failJob)

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

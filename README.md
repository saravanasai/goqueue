# GoQueue

A lightweight, high-performance job queue library for Go applications with support for multiple backends and built-in metrics.

## Features

- **Multiple Backends**: In-memory (development) and Redis (production)
- **Concurrency Control**: Configurable worker limits and job concurrency
- **Metrics Support**: Optional callback-based metrics collection
- **Middleware Support**: Chainable middleware for job processing customization
- **Lightweight**: Zero external dependencies for in-memory backend
- **Thread Safe**: Concurrent job processing with semaphore-based flow control

## Installation

```bash
go get github.com/saravanasai/goqueue
```

## Quick Start

### 1. Define Your Job

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"

    "github.com/saravanasai/goqueue"
    "github.com/saravanasai/goqueue/config"
)

// EmailJob implements the goqueue.Job interface
type EmailJob struct {
    To      string `json:"to"`
    Subject string `json:"subject"`
    Body    string `json:"body"`
}

func (e EmailJob) Process(ctx context.Context) error {
    // Simulate email sending
    fmt.Printf("Sending email to %s: %s\n", e.To, e.Subject)
    time.Sleep(100 * time.Millisecond)
    return nil
}

// Register the job type
func init() {
    goqueue.RegisterJob("EmailJob", func() goqueue.Job {
        return &EmailJob{}
    })
}
```

### 2. Basic Usage

```go
func main() {
    ctx := context.Background()

    // Configure queue (in-memory for development)
    cfg := config.NewInMemoryConfig()

    // Create queue
    q, err := goqueue.NewQueueWithDefaults("email-queue", cfg)
    if err != nil {
        log.Fatal(err)
    }

    // Start workers
    goqueue.StartWorker(q, ctx, 2)

    // Dispatch jobs (single)
    for i := 0; i < 5; i++ {
        job := EmailJob{
            To:      fmt.Sprintf("user%d@example.com", i),
            Subject: "Welcome!",
            Body:    "Thank you for signing up",
        }
        if err := goqueue.Dispatch(q, job); err != nil {
            log.Printf("Failed to dispatch job: %v", err)
        }
    }

    // Dispatch jobs (batch)
    batch := []goqueue.Job{
        &EmailJob{To: "userA@example.com", Subject: "Batch Welcome!", Body: "Hello A"},
        &EmailJob{To: "userB@example.com", Subject: "Batch Welcome!", Body: "Hello B"},
        &EmailJob{To: "userC@example.com", Subject: "Batch Welcome!", Body: "Hello C"},
    }
    if err := goqueue.DispatchBatch(q, batch); err != nil {
        log.Printf("Failed to dispatch batch jobs: %v", err)
    }

    // Let jobs process
    time.Sleep(2 * time.Second)
}
```

## Configuration

### Job Timeout Configuration

You can set a default job timeout for all jobs using the config:

```go
cfg := config.NewInMemoryConfig().
    WithJobTimeout(30 * time.Second) // Set default job timeout to 30 seconds

// For Redis
cfg := config.NewRedisConfig("localhost:6379", "", 0).
    WithJobTimeout(30 * time.Second)
```

You can also set a timeout for individual jobs:

```go
jobCtx := job.JobContext{Job: &EmailJob{...}}
jobCtx.SetTimeout(10 * time.Second) // Set timeout for this job only
```

If a job exceeds its timeout, it will be cancelled, logged as a timeout error, and retried or sent to DLQ as configured.

### Basic Configuration

```go
// In-Memory Backend (Development)
cfg := config.NewInMemoryConfig()

// Redis Backend (Production)
cfg := config.NewRedisConfig("localhost:6379", "", 0) // addr, password, db
```

### Concurrency Configuration

```go
cfg := config.NewInMemoryConfig().
    WithMaxWorkers(4).        // Max worker goroutines
    WithConcurrencyLimit(10)  // Max concurrent job processing

// For Redis
cfg := config.NewRedisConfig("localhost:6379", "", 0).
    WithMaxWorkers(8).
    WithConcurrencyLimit(50)
```

### Worker Configuration Examples

```go
// For CPU-intensive jobs
cfg.WithMaxWorkers(4).WithConcurrencyLimit(4)

// For I/O-intensive jobs (API calls, database operations)
cfg.WithMaxWorkers(2).WithConcurrencyLimit(20)

// Balanced workload
cfg.WithMaxWorkers(4).WithConcurrencyLimit(10)
```

## Simple Metrics Monitoring

### Basic Metrics Callback

```go
cfg := config.NewInMemoryConfig().
    WithMetricsCallback(func(metrics config.JobMetrics) {
        if metrics.Error != nil {
            log.Printf("Job %s failed in %v: %v",
                metrics.JobID, metrics.Duration, metrics.Error)
        } else {
            log.Printf("Job %s completed in %v",
                metrics.JobID, metrics.Duration)
        }
    })
```

### Dead Letter Queue (DLQ) Configuration

```go
// Using the built-in Redis DLQ adapter
redisDLQ := dlq.NewRedisDLQ(redisClient, logger)
cfg := config.NewRedisConfig("localhost:6379", "", 0).
    WithDLQAdapter(redisDLQ)

// Custom DLQ implementation
type MyCustomDLQ struct {
    // your implementation
}

func (d *MyCustomDLQ) Push(ctx context.Context, job *job.JobContext, err error) error {
    // your implementation
    return nil
}

cfg := config.NewRedisConfig("localhost:6379", "", 0).
    WithDLQAdapter(&MyCustomDLQ{})
```

### Middleware Configuration

GoQueue supports middleware for customizing job processing behavior. Middleware can be used for logging, conditional execution, and more. The middleware chain executes in the order they are added, with each middleware wrapping the next one in the chain.

```go
// Using built-in middleware with a logger
cfg := config.NewRedisConfig("localhost:6379", "", 0).
    WithMiddleware(middleware.LoggingMiddleware(logger))

// Custom middleware example
func MyCustomMiddleware() middleware.Middleware {
    return func(next middleware.HandlerFunc) middleware.HandlerFunc {
        return func(ctx context.Context, jobCtx *job.JobContext) error {
            // Pre-processing logic
            fmt.Printf("Processing job: %s\n", jobCtx.JobID)

            err := next(ctx, jobCtx)

            // Post-processing logic
            if err != nil {
                fmt.Printf("Job failed: %s\n", err)
            }
            return err
        }
    }
}

// Using multiple middleware
cfg.WithMiddlewares(
    middleware.LoggingMiddleware(logger),
    MyCustomMiddleware(),
)

// Using conditional skip middleware
cfg.WithMiddleware(middleware.ConditionalSkipMiddleware(func(jobCtx *job.JobContext) bool {
    // Skip processing for jobs older than 1 hour
    return time.Since(jobCtx.EnqueuedAt) > time.Hour
}))
```

Built-in middleware includes:

- `LoggingMiddleware`: Logs job execution details including start, completion, and errors
- `ConditionalSkipMiddleware`: Skips job processing based on custom conditions

## Performance Benchmarks

Based on testing with AWS t2.micro instance (1 vCPU, 1GB RAM) running Redis 6.x:

### Redis Backend

- **Simple Jobs (< 1ms processing)**: ~1,000 jobs/second
- **I/O Jobs (10-100ms processing)**: ~100-500 jobs/second
- **CPU Jobs (100ms+ processing)**: ~50-100 jobs/second

### In-Memory Backend

- **Simple Jobs (< 1ms processing)**: ~5,000 jobs/second
- **I/O Jobs (10-100ms processing)**: ~500-1,000 jobs/second
- **CPU Jobs (100ms+ processing)**: ~100-200 jobs/second

Note: These numbers are approximate and will vary based on:

- Instance type and resources
- Network latency (for Redis)
- Job complexity and processing time
- Concurrent worker configuration
- Queue size and load patterns

<!-- Badges -->

![Build Status](https://github.com/saravanasai/goqueue/actions/workflows/ci.yml/badge.svg)
[![Go Reference](https://pkg.go.dev/badge/github.com/saravanasai/goqueue.svg)](https://pkg.go.dev/github.com/saravanasai/goqueue)
![Coverage](https://img.shields.io/badge/coverage-100%25-brightgreen.svg)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

## Contributing

Contributions, issues, and feature requests are welcome!

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/my-feature`)
3. Commit your changes (`git commit -am 'Add new feature'`)
4. Push to the branch (`git push origin feature/my-feature`)
5. Open a pull request

Please read the [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) and follow the guidelines. For major changes, open an issue first to discuss what you would like to change.

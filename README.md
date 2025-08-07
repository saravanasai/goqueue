# GoQueue

A lightweight, high-performance job queue library for Go applications with support for multiple backends and built-in metrics.

## Features

- **Multiple Backends**: In-memory (development) and Redis (production)
- **Concurrency Control**: Configurable worker limits and job concurrency
- **Metrics Support**: Optional callback-based metrics collection
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

    // Dispatch jobs
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

    // Let jobs process
    time.Sleep(2 * time.Second)
}
```

## Configuration

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
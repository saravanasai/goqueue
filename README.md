# GoQueue

<div align="center">
  <img src="./assets/logo.png" alt="GoQueue Logo" width="180"/>
  <p><em>Reliable Job Processing for Go Applications</em></p>
  
  <p>
    <a href="https://pkg.go.dev/github.com/saravanasai/goqueue"><img src="https://pkg.go.dev/badge/github.com/saravanasai/goqueue.svg" alt="Go Reference"></a>
    <a href="LICENSE"><img src="https://img.shields.io/badge/License-MIT-yellow.svg" alt="License: MIT"></a>
    <img src="https://img.shields.io/badge/Go-%3E=1.18-blue" alt="Go Version">
    <img src="https://img.shields.io/badge/Version-0.0.1-green" alt="Version">
  </p>
</div>

GoQueue is a flexible background job processing library for Go applications, designed to handle workloads of any scale with multiple backend options.

## Features

- **Multiple Backends**: In-memory (development), Redis (production), PostgreSQL/MySQL , AWS SQS (cloud)
- **Job Management**: automatic retries with backoff, Dead Letter Queue
- **Concurrency Control**: Configurable worker pools, graceful shutdown
- **Observability**: Metrics collection, logging support
- **Extensibility**: Middleware pipeline for job customization
- **Code Quality**: Configured with golangci-lint for high code quality standards

For a deeper understanding of the system design, check out the [Architecture Documentation](docs/architecture.md).

## Why GoQueue?

GoQueue provides the most comprehensive feature set with excellent developer experience!

## Alternatives

Here are some popular alternatives and related frameworks for job queues and background processing in Go:

- **Asynq**: Redis-based job queue with a polished UI and CLI. Good developer experience, but recent commit activity is low.
- **River**: Transactional queue built on Postgres, offering strong consistency and transactional guarantees.
- **Temporal**: Workflow engine for complex orchestration and stateful jobs. Powerful, but has a steeper learning curve and heavier setup.
- **NATS**: High-performance messaging system. Lacks built-in job scheduling, but is widely used for event-driven architectures.
- **Machinery**: Older Go task queue library supporting multiple backends. Less active development and limited middleware support.
- **RabbitMQ / Kafka**: General-purpose message brokers. Robust and scalable, but require more infrastructure and lack job-specific features out of the box.

### Comparison Table

| Feature              | GoQueue | Asynq | River | Temporal | NATS | Machinery | RabbitMQ/Kafka |
| -------------------- | ------- | ----- | ----- | -------- | ---- | --------- | -------------- |
| Multiple Backends    | ✅      | ❌    | ❌    | ❌       | ❌   | ✅        | ✅             |
| Clean API            | ✅      | ✅    | ✅    | ⚠️       | ⚠️   | ❌        | ⚠️             |
| Middleware Support   | ✅      | ❌    | ✅    | ⚠️       | ❌   | ❌        | ❌             |
| Test Coverage        | ✅      | ✅    | ✅    | ✅       | ⚠️   | ⚠️        | ⚠️             |
| AWS SQS Support      | ✅      | ❌    | ❌    | ❌       | ❌   | ❌        | ❌             |
| Non-blocking Retries | ✅      | ✅    | ✅    | ✅       | ⚠️   | ⚠️        | ⚠️             |
| UI/CLI Tools         | ⚠️      | ✅    | ✅    | ✅       | ❌   | ❌        | ⚠️             |
| Scheduling           | ✅      | ✅    | ✅    | ✅       | ❌   | ⚠️        | ⚠️             |
| SQL                  | ✅      | ❌    | ✅    | ⚠️       | ❌   | ❌        | ⚠️             |

## Installation

```bash
go get github.com/saravanasai/goqueue
```

## Quick Start

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

// Define your job
type EmailJob struct {
    To      string `json:"to"`
    Subject string `json:"subject"`
}

// Implement the Job interface
func (e EmailJob) Process(ctx context.Context) error {
    fmt.Printf("Sending email to %s: %s\n", e.To, e.Subject)
    return nil
}

// Register job type for serialization
func init() {
    goqueue.RegisterJob("EmailJob", func() goqueue.Job {
        return &EmailJob{}
    })
}

func main() {
    // Create a queue with in-memory backend
    cfg := config.NewInMemoryConfig()
    q, err := goqueue.NewQueueWithDefaults("email-queue", cfg)
    if err != nil {
        log.Fatal(err)
    }

    // Start worker pool with 2 concurrent workers
    ctx := context.Background()
    q.StartWorkers(ctx, 2)

    // Dispatch a job
    job := EmailJob{
        To:      "user@example.com",
        Subject: "Welcome to GoQueue!",
    }
    if err := q.Dispatch(job); err != nil {
        log.Printf("Failed to dispatch job: %v", err)
    }

    // Wait to see results (in production, workers would run continuously)
    time.Sleep(2 * time.Second)

    // Graceful shutdown
    shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    q.Shutdown(shutdownCtx)
}
```

## Database Driver Setup

GoQueue supports database backends for teams that prefer SQL-based persistence and transactional guarantees. Both PostgreSQL and MySQL are supported.

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/saravanasai/goqueue"
    "github.com/saravanasai/goqueue/config"
)

func main() {
    // Create a queue with PostgreSQL backend
    cfg := config.NewPostgresConfig("postgresql://user:password@localhost:5432/dbname")

    // Configure metrics callback (optional)
    cfg = cfg.WithMetricsCallback(func(metrics config.JobMetrics) {
        fmt.Printf("Job: %s, Queue: %s, Duration: %v, Error: %v\n",
            metrics.JobID, metrics.QueueName, metrics.Duration, metrics.Error)
    })

    // Create the queue
    q, err := goqueue.NewQueueWithDefaults("emails", cfg)
    if err != nil {
        log.Fatalf("Failed to create queue: %v", err)
    }

    // Dispatch jobs and start workers as normal...
}
```

The database driver automatically handles creating tables and managing failed jobs through a Dead Letter Queue (DLQ).

## Backend Options

### In-Memory (Development)

```go
cfg := config.NewInMemoryConfig()
queue, err := goqueue.NewQueueWithDefaults("emails", cfg)
```

### PostgreSQL/MySQL (Transactional)

```go
// PostgreSQL
cfg := config.NewPostgresConfig("postgresql://user:password@localhost:5432/dbname")
queue, err := goqueue.NewQueueWithDefaults("emails", cfg)

// MySQL
cfg := config.NewMySQLConfig("root:password@tcp(localhost:3306)/dbname")
queue, err := goqueue.NewQueueWithDefaults("emails", cfg)
```

### Redis (Production)

```go
cfg := config.NewRedisConfig(
    "localhost:6379", // Redis server address
    "",               // Username (if any)
    "",               // Password (if any)
    0,                // Database number
)
queue, err := goqueue.NewQueueWithDefaults("emails", cfg)
```

### AWS SQS (Cloud)

```go
cfg := config.NewSQSConfig(
    "https://sqs.us-west-2.amazonaws.com/123456789012/my-queue", // Queue URL
    "us-west-2",                                                 // AWS Region
    "xxx",                                                       // Access key
    "xxxx",                                                      // Secret key
)
queue, err := goqueue.NewQueueWithDefaults("notifications", cfg)
```

## Configuration Options

```go
// Basic configuration
cfg := config.NewRedisConfig("localhost:6379", "", "", 0)

// Worker options
cfg = cfg.WithMaxWorkers(5)               // Number of worker goroutines
cfg = cfg.WithConcurrencyLimit(10)        // Maximum concurrent jobs

// Retry behavior
cfg = cfg.WithMaxRetryAttempts(3)         // Max retry attempts for failed jobs
cfg = cfg.WithRetryDelay(5 * time.Second) // Base delay between retries
cfg = cfg.WithExponentialBackoff(true)    // Use increasing delays between retries

// Metrics collection
cfg = cfg.WithMetricsCallback(func(metrics config.JobMetrics) {
    log.Printf("Job %s completed in %v with error: %v",
        metrics.JobID, metrics.Duration, metrics.Error)
})
```

## Advanced Features

### Delayed Jobs

You can schedule jobs to run in the future using `DispatchWithDelay`:

```go
// Schedule a job to run 5 minutes from now
delay := 5 * time.Minute
if err := q.DispatchWithDelay(job, delay); err != nil {
    log.Printf("Failed to schedule job: %v", err)
}
```

> **Note**: When using the Amazon SQS driver, the maximum supported delay is 15 minutes.

### Job Middleware

Middleware allows you to add cross-cutting functionality to job processing. GoQueue uses a pipeline pattern where each middleware wraps the next one.

```go
// Available built-in middleware
loggingMiddleware := middleware.LoggingMiddleware(logger) // Structured logging
skipCondition := middleware.ConditionalSkipMiddleware(func(jobCtx *job.JobContext) bool {
    // Skip processing based on job attributes
    return jobCtx.EnqueuedAt.Before(time.Now().Add(-24 * time.Hour))
})

// Custom middleware
rateLimiter := func(next middleware.HandlerFunc) middleware.HandlerFunc {
    return func(ctx context.Context, jobCtx *job.JobContext) error {
        // Implement rate limiting logic here
        fmt.Println("Rate limiting job execution")
        return next(ctx, jobCtx)
    }
}

// Apply middleware through configuration (order matters - first added, first executed)
cfg = cfg.WithMiddleware(loggingMiddleware)
cfg = cfg.WithMiddleware(skipCondition)
cfg = cfg.WithMiddleware(rateLimiter)

// Or add multiple middlewares at once
cfg = cfg.WithMiddlewares(loggingMiddleware, skipCondition, rateLimiter)
```

### Dead Letter Queue (DLQ)

When jobs fail repeatedly after exhausting retry attempts, they can be sent to a Dead Letter Queue for analysis and handling.

```go
// Custom handler for failed jobs
type CustomDLQ struct{}

func (d *CustomDLQ) Push(ctx context.Context, jobCtx *job.JobContext, err error) error {
    // Store job data and error in database
    fmt.Printf("Job %s failed after %d attempts: %v\n",
        jobCtx.JobID, jobCtx.Attempts, err)

    // Send alerts or notifications
    // alertService.Send(fmt.Sprintf("Critical job failure: %s", jobCtx.JobID))

    return nil
}

// Register custom DLQ
cfg = cfg.WithDLQAdapter(&CustomDLQ{})
```

### Metrics and Observability

GoQueue provides built-in support for collecting metrics and monitoring job processing:

```go
// Register metrics callback
cfg = cfg.WithMetricsCallback(func(metrics config.JobMetrics) {
    // Record metrics in your monitoring system (Prometheus, etc.)
    fmt.Printf("Job: %s, Queue: %s, Duration: %v, Error: %v\n",
        metrics.JobID, metrics.QueueName, metrics.Duration, metrics.Error)

    // Track success/failure rates
    if metrics.Error != nil {
        failureCounter.Inc()
    } else {
        successCounter.Inc()
    }
})
```

## License

[MIT](LICENSE)

## Roadmap

Upcoming features planned for GoQueue:

- **NATS Driver**: NATS as backend option.
- **Event Bus**: For better metric collection.
- **Transaction interface** : For better transactional jobs

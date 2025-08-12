<div align="center">
  <img src="./assets/logo.png" alt="GoQueue Logo" width="180"/>
  <h1>GoQueue</h1>
  <p><em>A lightweight, high-performance job queue library for Go applications</em></p>
  
  <p>
    <a href="https://github.com/saravanasai/goqueue/actions/workflows/ci.yml">
      <img src="https://github.com/saravanasai/goqueue/actions/workflows/ci.yml/badge.svg" alt="Build Status">
    </a>
    <a href="https://pkg.go.dev/github.com/saravanasai/goqueue">
      <img src="https://pkg.go.dev/badge/github.com/saravanasai/goqueue.svg" alt="Go Reference">
    </a>
    <a href="LICENSE">
      <img src="https://img.shields.io/badge/License-MIT-yellow.svg" alt="License: MIT">
    </a>
    <img src="https://img.shields.io/badge/coverage-100%25-brightgreen.svg" alt="Coverage">
  </p>
</div>

<hr style="margin-bottom: 20px;">

## Features

- **Multiple Backends**: In-memory (development), Redis (production), and AWS SQS (cloud-native)
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

// AWS SQS Backend (Cloud-Native)
cfg := config.NewSQSConfig(
    "https://sqs.us-west-2.amazonaws.com/123456789012/my-queue",  // queueURL
    "us-west-2",                                                  // region
    "AKIAIOSFODNN7EXAMPLE",                                       // accessKeyID (optional, can use instance profile)
    "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"                   // secretAccessKey (optional)
)

// AWS SQS FIFO Queue Backend
cfg := config.NewSQSFifoConfig(
    "https://sqs.us-west-2.amazonaws.com/123456789012/my-queue.fifo",  // queueURL (must end with .fifo)
    "us-west-2",                                                       // region
    "AKIAIOSFODNN7EXAMPLE",                                            // accessKeyID (optional)
    "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",                       // secretAccessKey (optional)
    "default-group"                                                    // messageGroupID (required for FIFO queues)
)
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

### AWS SQS Backend

- **Simple Jobs (< 1ms processing)**: ~50-100 jobs/second
- **I/O Jobs (10-100ms processing)**: ~50-80 jobs/second
- **CPU Jobs (100ms+ processing)**: ~20-50 jobs/second

### In-Memory Backend

- **Simple Jobs (< 1ms processing)**: ~5,000 jobs/second
- **I/O Jobs (10-100ms processing)**: ~500-1,000 jobs/second
- **CPU Jobs (100ms+ processing)**: ~100-200 jobs/second

Note: These numbers are approximate and will vary based on:

- Instance type and resources
- Network latency (for Redis/SQS)
- Job complexity and processing time
- Concurrent worker configuration
- Queue size and load patterns
- AWS SQS visibility timeout and batch size settings

## AWS SQS Configuration

When using the SQS backend, you'll need the following AWS permissions:

- `sqs:SendMessage` - For enqueueing jobs
- `sqs:ReceiveMessage` - For worker to fetch jobs
- `sqs:DeleteMessage` - For acknowledging completed jobs
- `sqs:GetQueueAttributes` - For health checks
- `sqs:SendMessageBatch` - For batch job enqueueing

Example IAM policy:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "sqs:SendMessage",
                "sqs:ReceiveMessage",
                "sqs:DeleteMessage",
                "sqs:GetQueueAttributes",
                "sqs:SendMessageBatch"
            ],
            "Resource": [
                "arn:aws:sqs:region:account-id:queue-name"
            ]
        }
    ]
}
```

You can also use AWS environment variables or instance profiles for authentication:

```
AWS_REGION=us-west-2
AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
```

## Using AWS SQS FIFO Queues

AWS SQS FIFO (First-In-First-Out) queues provide exactly-once processing and message ordering guarantees. GoQueue supports FIFO queues with a dedicated configuration function.

### Configuration

```go
// Create a FIFO queue configuration with required MessageGroupID
cfg := config.NewSQSFifoConfig(
    "https://sqs.us-west-2.amazonaws.com/123456789012/my-queue.fifo",  // queueURL (must end with .fifo)
    "us-west-2",                                                       // region
    "AKIAIOSFODNN7EXAMPLE",                                            // accessKeyID (optional)
    "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",                       // secretAccessKey (optional)
    "order-processing"                                                 // messageGroupID
)

// Optional: Customize the MessageDeduplicationID
sqsCfg := cfg.DriverConfig.(config.SQSConfig)
sqsCfg.MessageDeduplicationID = "custom-dedup-id"  // If not set, job ID is used
cfg.DriverConfig = sqsCfg

// Create and use the queue as normal
q, err := goqueue.NewQueueWithDefaults("orders", cfg)
if err != nil {
    log.Fatal(err)
}
```

### MessageGroupID and MessageDeduplicationID

- **MessageGroupID**: Required for FIFO queues. Messages with the same MessageGroupID are processed in the order they are sent. If not specified, "default" is used.

- **MessageDeduplicationID**: Optional for FIFO queues. Ensures message uniqueness within a 5-minute deduplication interval. If not specified, the job ID is used, ensuring each message is unique.

### Size Limit Validation

GoQueue validates that job payloads do not exceed the AWS SQS message size limit of 256KB. If a job exceeds this limit, an error is returned:

```go
// This will return an error if jobData exceeds 256KB
err := goqueue.Dispatch(q, jobWithLargePayload)
if err != nil {
    // Handle the error - implement your own S3 offloading logic if needed
    log.Printf("Job too large for SQS: %v", err)
}
```

Note: For large payloads (>256KB), you'll need to implement your own storage solution (like S3) and store a reference in the job.

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

### Running Tests

To run all tests in the project:

```bash
go test ./...
```

To run tests for a specific package:

```bash
go test ./worker
go test ./queue
go test ./adapter/memory
```

To run tests with verbose output:

```bash
go test -v ./...
```

For Redis adapter tests, you'll need a running Redis server:

```bash
# Start Redis in Docker
docker run --name redis-test -p 6379:6379 -d redis

# Run Redis tests
go test ./adapter/redis
```

For SQS adapter tests, you'll need proper AWS credentials configured.

Please read the [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) and follow the guidelines. For major changes, open an issue first to discuss what you would like to change.

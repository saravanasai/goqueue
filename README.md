<div align="center">
  <img src="./assets/logo.png" alt="GoQueue Logo" width="180"/>
  <h1>GoQueue</h1>
  <p><em>A robust, production-ready job queue system for Go</em></p>
  
  <p>
    <a href="https://pkg.go.dev/github.com/saravanasai/goqueue">
      <img src="https://pkg.go.dev/badge/github.com/saravanasai/goqueue.svg" alt="Go Reference">
    </a>
    <a href="LICENSE">
      <img src="https://img.shields.io/badge/License-MIT-yellow.svg" alt="License: MIT">
    </a>
    <img src="https://img.shields.io/badge/Go-%3E=1.18-blue" alt="Go Version">
  </p>
</div>

<hr>

GoQueue is a job processing library designed for performance and reliability in production environments. It provides a flexible solution for background processing needs in Go applications of any scale. The architecture allows for seamless transitions between development and production by supporting multiple backend options - use the lightweight in-memory queue during development, then switch to Redis or AWS SQS in production with minimal code changes.

## Key Features

GoQueue provides a comprehensive set of features to handle asynchronous job processing in Go applications:

- **Flexible Storage Backends**:

  - Memory queue for lightweight, in-process job storage ideal for development environments
  - Redis backend for reliable, persistent queue operations in production
  - AWS SQS integration for fully-managed cloud infrastructure (supports both standard and FIFO queues)

- **Advanced Processing Controls**:

  - Configurable concurrency limits to optimize resource utilization
  - Intelligent worker pool management
  - Sophisticated error handling with customizable retry strategies
  - Automatic job serialization and deserialization

- **Monitoring & Observability**:

  - Built-in metrics collection for performance analysis
  - Event hooks for integration with logging and monitoring systems

- **Extensible Architecture**:
  - Middleware support for pre/post job processing
  - Pluggable design for custom storage adapters
  - Thread-safe implementation for concurrent environments

## Installation

Install GoQueue using the standard Go package management:

```bash
go get github.com/saravanasai/goqueue
```

**Note:** GoQueue requires Go 1.18 or later.

## Getting Started

<details open>
<summary><b>Basic Implementation</b></summary>

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

// Define your job type
type EmailJob struct {
    To      string `json:"to"`
    Subject string `json:"subject"`
    Body    string `json:"body"`
}

// Implement the job interface
func (e EmailJob) Process(ctx context.Context) error {
    fmt.Printf("Sending email to %s: %s\n", e.To, e.Subject)
    time.Sleep(100 * time.Millisecond) // Simulate work
    return nil
}

// Register job type for serialization
func init() {
    goqueue.RegisterJob("EmailJob", func() goqueue.Job {
        return &EmailJob{}
    })
}

func main() {
    // 1. Create a queue with in-memory backend
    cfg := config.NewInMemoryConfig()
    q, err := goqueue.NewQueueWithDefaults("email-queue", cfg)
    if err != nil {
        log.Fatal(err)
    }

    // 2. Start worker pool with 2 concurrent workers
    ctx := context.Background()
    goqueue.StartWorker(q, ctx, 2)

    // 3. Dispatch individual jobs
    for i := 0; i < 3; i++ {
        job := EmailJob{
            To:      fmt.Sprintf("user%d@example.com", i),
            Subject: "Welcome to GoQueue!",
            Body:    "Thank you for signing up",
        }
        if err := goqueue.Dispatch(q, job); err != nil {
            log.Printf("Failed to dispatch job: %v", err)
        }
    }

    // 4. Dispatch batch jobs for better performance
    batch := []goqueue.Job{
        &EmailJob{To: "batch1@example.com", Subject: "Batch Processing", Body: "Hello 1"},
        &EmailJob{To: "batch2@example.com", Subject: "Batch Processing", Body: "Hello 2"},
    }
    if err := goqueue.DispatchBatch(q, batch); err != nil {
        log.Printf("Failed to dispatch batch: %v", err)
    }

    // Wait to see results (in production, workers would run continuously)
    time.Sleep(2 * time.Second)
}
```

</details>

<details>
<summary><b>Production Setup</b></summary>

```go
// Redis backend (for production use)
redisCfg := config.NewRedisConfig(
    "localhost:6379", // Redis server address
    "",               // Password (if any)
    0,                // Database number
)
redisQueue, err := goqueue.NewQueueWithDefaults("emails", redisCfg)

// AWS SQS backend (for cloud deployments)
sqsCfg := config.NewSQSConfig(
    "https://sqs.us-west-2.amazonaws.com/123456789012/my-queue", // Queue URL
    "us-west-2",                                                 // AWS Region
    "",                                                          // Access key (optional)
    "",                                                          // Secret key (optional)
)
sqsQueue, err := goqueue.NewQueueWithDefaults("notifications", sqsCfg)
```

</details>

<details>
<summary><b>Advanced Worker Configuration</b></summary>

```go
// Create worker with advanced options
workerOpts := &goqueue.WorkerOptions{
    Concurrency:      10,     // Number of concurrent jobs
    PollInterval:     2 * time.Second,  // How often to check for new jobs
    MaxRetries:       3,      // Number of retry attempts on failure
    BackoffStrategy:  goqueue.ExponentialBackoff, // Retry with increasing delays
    FailureCallback: func(job goqueue.Job, err error) {
        log.Printf("Job %s failed: %v", job.ID(), err)
    },
}

worker := goqueue.NewWorkerWithOptions(queue, workerOpts)
go worker.Start()

// Graceful shutdown
// ... application running ...
worker.Stop() // Wait for in-progress jobs to complete
```

</details>

## ⚙️ Configuration

GoQueue provides flexible configuration options to adapt to different environments and requirements.

### Choosing a Backend

<details>
<summary><b>🧠 In-Memory Backend</b> (for development/testing)</summary>

```go
// Simple in-memory queue (data is lost when application restarts)
cfg := config.NewInMemoryConfig()
```

**Great for:**

- Local development
- Testing environments
- Small applications with non-critical jobs
- Proof of concept projects

**Limitations:**

- No persistence across restarts
- Cannot be shared between multiple processes
- Not suitable for production workloads
</details>

<details>
<summary><b>🔶 Redis Backend</b> (for production)</summary>

```go
// Redis-backed queue for persistence and reliability
cfg := config.NewRedisConfig(
    "localhost:6379",  // Redis address
    "password123",     // Password (empty string if none)
    0                  // Redis database number
)

// Advanced Redis configuration
redisCfg := config.NewRedisConfig("localhost:6379", "", 0)
redisCfg.MaxRetries = 3                    // Connection retry attempts
redisCfg.DialTimeout = 5 * time.Second     // Connection timeout
redisCfg.ReadTimeout = 3 * time.Second     // Read timeout
redisCfg.WriteTimeout = 3 * time.Second    // Write timeout
redisCfg.PoolSize = 10                     // Connection pool size
```

**Great for:**

- Production deployments
- Persistent job storage
- Horizontal scaling with multiple workers
- High-throughput processing
- Services requiring persistence

**Features:**

- Fast and reliable
- Job persistence across restarts
- Shared queue across multiple services
- Dead letter queue support
- Mature and battle-tested
</details>

<details>
<summary><b>☁️ AWS SQS Backend</b> (for cloud-native apps)</summary>

```go
// Standard SQS Queue
cfg := config.NewSQSConfig(
    "https://sqs.us-west-2.amazonaws.com/123456789012/my-queue",  // Queue URL
    "us-west-2",                                                  // AWS region
    "AKIAIOSFODNN7EXAMPLE",                                       // Access key (optional)
    "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"                   // Secret key (optional)
)

// FIFO Queue (ordered processing)
cfg := config.NewSQSFifoConfig(
    "https://sqs.us-west-2.amazonaws.com/123456789012/my-queue.fifo",  // URL (.fifo suffix)
    "us-west-2",                                                       // AWS region
    "AKIAIOSFODNN7EXAMPLE",                                            // Access key (optional)
    "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",                       // Secret key (optional)
    "default-group"                                                    // Message group ID
)

// Advanced SQS configuration
sqsCfg := config.NewSQSConfig("https://sqs.url...", "us-west-2", "", "")
sqsCfg.VisibilityTimeout = 30           // Visibility timeout in seconds
sqsCfg.WaitTimeSeconds = 20             // Long polling wait time
sqsCfg.MaxNumberOfMessages = 10         // Max batch size for receives
```

**Great for:**

- Cloud-native applications
- Serverless architectures
- Auto-scaling worker environments
- Applications requiring high availability

**Features:**

- Fully managed by AWS
- Auto-scaling and high availability
- Standard and FIFO queue support
- Transparent integration with AWS ecosystem
- No maintenance overhead
</details>

### Worker Configuration

<details>
<summary><b>🛠️ Worker Options</b></summary>

```go
// Create custom worker options
opts := &goqueue.WorkerOptions{
    Concurrency:     5,                  // Number of concurrent jobs
    PollInterval:    500 * time.Millisecond, // How often to check for new jobs
    MaxRetries:      3,                  // Max retry attempts for failed jobs
    BackoffStrategy: goqueue.LinearBackoff, // Retry delay strategy
    RetryDelay:      5 * time.Second,    // Base delay between retries

    // Optional callbacks
    FailureCallback: func(job goqueue.Job, err error) {
        log.Printf("Job %s failed: %v", job.ID(), err)
        // Send alerts, log to monitoring system, etc.
    },
    SuccessCallback: func(job goqueue.Job) {
        log.Printf("Job %s completed successfully", job.ID())
        // Update metrics, record completion, etc.
    },
}

// Create worker with custom options
worker := goqueue.NewWorkerWithOptions(queue, opts)

// Start and stop the worker
go worker.Start()
// ... application running ...
worker.Stop() // Graceful shutdown
```

**Available Backoff Strategies:**

- `NoBackoff`: Always retry immediately
- `ConstantBackoff`: Fixed delay between retries
- `LinearBackoff`: Delay increases linearly (delay \* retry count)
- `ExponentialBackoff`: Delay doubles with each retry attempt
</details>

### Performance Tuning

<details>
<summary><b>⚙️ Concurrency Settings</b></summary>

```go
// Configure worker pools and concurrency limits
cfg := config.NewRedisConfig("localhost:6379", "", 0).
    WithMaxWorkers(8).        // Number of worker goroutines (CPU-bound tasks)
    WithConcurrencyLimit(20)  // Maximum concurrent jobs (I/O-bound tasks)
```

**Recommended configurations:**

- **CPU-intensive jobs**: Set both values close to the number of CPU cores
- **I/O-intensive jobs**: Lower worker count, higher concurrency limit
- **Mixed workloads**: Balance between the two approaches

</details>

<details>
<summary><b>⏱️ Timeout Configuration</b></summary>

```go
// Set a default timeout for all jobs
cfg := config.NewInMemoryConfig().
    WithJobTimeout(30 * time.Second)

// Override timeout for specific jobs
jobCtx := job.JobContext{Job: &EmailJob{...}}
jobCtx.SetTimeout(10 * time.Second)
```

When a job exceeds its timeout:

- The context is cancelled
- An error is logged
- The job is retried or sent to DLQ based on your configuration

</details>
```

### Advanced Features

<details>
<summary><b>📊 Metrics and Monitoring</b></summary>

```go
// Add metrics collection to track job performance
cfg := config.NewRedisConfig("localhost:6379", "", 0).
    WithMetricsCallback(func(metrics config.JobMetrics) {
        // Log performance metrics
        if metrics.Error != nil {
            log.Printf("[ERROR] Job %s failed in %v: %v",
                metrics.JobID, metrics.Duration, metrics.Error)
        } else {
            log.Printf("[INFO] Job %s completed in %v",
                metrics.JobID, metrics.Duration)
        }

        // You could send these metrics to your monitoring system:
        // - Prometheus, Datadog, CloudWatch, etc.
        // myMetricsClient.RecordJobExecution(metrics)
    })
```

The metrics callback provides:

- Job ID and queue name
- Processing duration
- Error information (if any)
- Timestamp of execution

</details>

<details>
<summary><b>💀 Dead Letter Queue (DLQ) Configuration</b></summary>

When jobs fail repeatedly, they should be moved to a Dead Letter Queue for later inspection:

```go
// Using the built-in Redis DLQ adapter
redisDLQ := dlq.NewRedisDLQ(redisClient, logger)
cfg := config.NewRedisConfig("localhost:6379", "", 0).
    WithDLQAdapter(redisDLQ)
```

You can also implement your own DLQ adapter:

```go
// Custom DLQ implementation
type MyCustomDLQ struct {
    // Your fields here
}

func (d *MyCustomDLQ) Push(ctx context.Context, job *job.JobContext, err error) error {
    // Your custom DLQ logic:
    // - Store in a database
    // - Send to an external system
    // - Notify via webhook
    return nil
}

// Use your custom DLQ
cfg := config.NewRedisConfig("localhost:6379", "", 0).
    WithDLQAdapter(&MyCustomDLQ{})
```

</details>

<details>
<summary><b>🔄 Middleware Pipeline</b></summary>

Middleware allows you to intercept and modify job processing. Common use cases include:

- Logging and metrics collection
- Transaction management
- Permission checks
- Conditional execution

```go
// Using built-in middleware
cfg := config.NewRedisConfig("localhost:6379", "", 0).
    WithMiddleware(middleware.LoggingMiddleware(logger))

// Creating your own middleware
func MyCustomMiddleware() middleware.Middleware {
    return func(next middleware.HandlerFunc) middleware.HandlerFunc {
        return func(ctx context.Context, jobCtx *job.JobContext) error {
            // Pre-processing logic (runs before job execution)
            fmt.Printf("Starting job: %s\n", jobCtx.JobID)

            // Call the next middleware in the chain
            err := next(ctx, jobCtx)

            // Post-processing logic (runs after job execution)
            if err != nil {
                fmt.Printf("Job failed: %s\n", err)
            } else {
                fmt.Printf("Job succeeded: %s\n", jobCtx.JobID)
            }

            return err
        }
    }
}

// Combine multiple middleware (executed in order)
cfg.WithMiddlewares(
    middleware.LoggingMiddleware(logger),
    MyCustomMiddleware(),
    middleware.ConditionalSkipMiddleware(func(jobCtx *job.JobContext) bool {
        // Skip processing for jobs older than 1 hour
        return time.Since(jobCtx.EnqueuedAt) > time.Hour
    }),
)
```

Built-in middleware:

| Middleware                  | Purpose                                              |
| --------------------------- | ---------------------------------------------------- |
| `LoggingMiddleware`         | Records job execution events with timing information |
| `ConditionalSkipMiddleware` | Bypasses job processing based on custom conditions   |
| `RetryMiddleware`           | Implements custom retry logic for failed jobs        |

</details>

## Performance

<details>
<summary><b>📊 Benchmark Results</b></summary>

The following benchmarks were measured on AWS t2.micro (1 vCPU, 1GB RAM) with Redis 6.x:

| Backend       | Job Type  | Processing Time | Throughput          |
| ------------- | --------- | --------------- | ------------------- |
| **Redis**     | Simple    | < 1ms           | ~1,000 jobs/sec     |
|               | I/O-bound | 10-100ms        | ~100-500 jobs/sec   |
|               | CPU-bound | 100ms+          | ~50-100 jobs/sec    |
| **AWS SQS**   | Simple    | < 1ms           | ~50-100 jobs/sec    |
|               | I/O-bound | 10-100ms        | ~50-80 jobs/sec     |
|               | CPU-bound | 100ms+          | ~20-50 jobs/sec     |
| **In-Memory** | Simple    | < 1ms           | ~5,000 jobs/sec     |
|               | I/O-bound | 10-100ms        | ~500-1,000 jobs/sec |
|               | CPU-bound | 100ms+          | ~100-200 jobs/sec   |

Performance will vary based on:

- Hardware resources and network conditions
- Job complexity and processing patterns
- Worker and concurrency settings
- Queue size and batch processing configuration

</details>

<details>
<summary><b>⚡ Performance Tips</b></summary>

**For High Throughput:**

1. **Use batch operations** when possible (`DispatchBatch` instead of multiple `Dispatch` calls)
2. **Optimize concurrency settings** for your workload type
3. **Consider job payload size** - smaller payloads process faster
4. **Balance worker count** with your system resources

**For Redis Backend:**

- Keep connections pooled and reuse them
- Consider using Redis Cluster for horizontal scaling

**For AWS SQS Backend:**

- Use long polling (enabled by default in GoQueue)
- Set appropriate visibility timeouts based on job duration
- For FIFO queues, batch similar messages with the same MessageGroupId

</details>

## AWS SQS Integration

<details>
<summary><b>🔑 Required AWS Permissions</b></summary>

When using the SQS backend, your IAM user/role needs these permissions:

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
      "Resource": ["arn:aws:sqs:region:account-id:queue-name"]
    }
  ]
}
```

You can use environment variables for credentials:

```
AWS_REGION=us-west-2
AWS_ACCESS_KEY_ID=YOUR_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY=YOUR_SECRET_ACCESS_KEY
```

Or let GoQueue use your instance profile, ECS task role, or other AWS credential sources automatically.

</details>

## Using AWS SQS FIFO Queues

<details>
<summary><b>🔄 FIFO Queue Overview</b></summary>

Amazon SQS FIFO (First-In-First-Out) queues guarantee:

- **Exactly-once processing**: No duplicates
- **Message ordering**: Messages are processed in the exact order they were sent (within a message group)
- **High throughput**: Up to 3,000 messages per second with batching

GoQueue seamlessly supports FIFO queues while maintaining the same developer experience.

</details>

<details>
<summary><b>⚙️ FIFO Queue Configuration</b></summary>

```go
// Use the dedicated FIFO queue configuration builder
cfg := config.NewSQSFifoConfig(
    "https://sqs.us-west-2.amazonaws.com/123456789012/my-queue.fifo",  // Queue URL (must end with .fifo)
    "us-west-2",                                                       // AWS Region
    "ACCESS_KEY_ID",                                                   // Optional credentials
    "SECRET_ACCESS_KEY",                                               // Optional credentials
    "order-processing"                                                 // MessageGroupID
)

// Optional: Set a custom deduplication ID strategy
sqsCfg := cfg.DriverConfig.(config.SQSConfig)
sqsCfg.MessageDeduplicationID = "custom-dedup-id"  // If not set, job ID is used
cfg.DriverConfig = sqsCfg

// Create and use the queue just like any other queue
q, err := goqueue.NewQueueWithDefaults("orders", cfg)
if err != nil {
    log.Fatal(err)
}

// Dispatch jobs normally - FIFO attributes are handled automatically
err = goqueue.Dispatch(q, &OrderProcessingJob{OrderID: "12345"})
```

</details>

<details>
<summary><b>🔍 Key FIFO Concepts</b></summary>

### MessageGroupID

The `MessageGroupID` parameter controls which messages are processed sequentially:

- Messages with the **same** MessageGroupID are processed in strict order
- Messages with **different** MessageGroupIDs can be processed in parallel
- If not specified, GoQueue uses `"default"` as the MessageGroupID

**Usage Tips:**

- Use customer ID, order ID, or session ID as MessageGroupID
- For completely independent messages, use unique MessageGroupIDs
- For strict ordering of all messages, use the same MessageGroupID for all

### MessageDeduplicationID

Controls how SQS identifies duplicate messages:

- Must be unique for each message within a 5-minute window
- If not specified, GoQueue uses the generated job ID

**Usage Tips:**

- For idempotent operations, use a business identifier (e.g., transaction ID)
- For non-idempotent operations, let GoQueue use the default (job ID)
- For high-volume producers, provide application-specific IDs
</details>

<details>
<summary><b>📏 Message Size Limitations</b></summary>

AWS SQS has a hard limit of 256KB per message. GoQueue validates payload sizes:

```go
// This will return an error if the payload exceeds 256KB
err := goqueue.Dispatch(q, jobWithLargePayload)
if err != nil {
    if strings.Contains(err.Error(), "exceeds AWS SQS limit of 256KB") {
        // Handle oversized payload
        log.Printf("Job too large: %v", err)
    }
}
```

**For Large Payloads:**

1. Store data externally (S3, DynamoDB, etc.)
2. Put only the reference in the job payload
3. Retrieve the full data during job processing

```go
// Instead of sending large data directly
type LargeJob struct {
    Data []byte // Might exceed 256KB
}

// Send a reference
type LargeJobReference struct {
    S3Bucket string `json:"bucket"`
    S3Key    string `json:"key"`
}
```

</details>

## Contributing

Contributions, issues, and feature requests are welcome!

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/my-feature`)
3. Commit your changes (`git commit -am 'Add new feature'`)
4. Push to the branch (`git push origin feature/my-feature`)
5. Open a pull request

### Running Tests

To run all tests in the project:

## 🤝 Contributing

Contributions are welcome! Here's how you can help:

<details>
<summary><b>Development Setup</b></summary>

1. Fork the repository
2. Clone your fork: `git clone https://github.com/your-username/goqueue.git`
3. Create your feature branch: `git checkout -b my-new-feature`
4. Make your changes and add tests
5. Run tests: `go test ./...`
6. Commit your changes: `git commit -am 'Add some feature'`
7. Push to the branch: `git push origin my-new-feature`
8. Submit a pull request
</details>

<details>
<summary><b>Running Tests</b></summary>

```bash
# Run all tests
go test ./...

# Run tests for a specific package
go test ./worker
go test ./queue
go test ./adapter/memory

# Run tests with verbose output
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

</details>

<details>
<summary><b>Code Style</b></summary>

- Follow Go best practices and standard formatting
- Run `go fmt` and `golint` before committing
- Write comprehensive tests for new features
- Update documentation for API changes
- Keep backward compatibility when possible
</details>

Please read the [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) and follow the guidelines. For major changes, open an issue first to discuss what you would like to change.

## 📄 License

GoQueue is available under the [MIT License](LICENSE).

---

<div align="center">
  <p>Built with ❤️ for the Go community</p>
</div>

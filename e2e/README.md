# E2E Tests

End-to-end tests for GoQueue drivers. Test complete queue functionality with realistic job processing.

## Test Commands by Driver

### Memory Driver

```bash
# Run memory e2e test
go test -v ./e2e/ -run TestSimple

# Run memory concurrent test
go test -v ./e2e/ -run TestMemoryQueueConcurrentDispatch
```

### Redis Driver

```bash
# Run Redis e2e tests
go test -v ./e2e/ -run TestRedis

# Run specific Redis test
go test -v ./e2e/ -run TestRedisQueueConcurrentDispatch

# Run Redis metrics test
go test -v ./e2e/ -run TestRedisQueueMetrics
```

### SQS Driver

**Prerequisites**: Set the following environment variables before running SQS tests:

```bash
export SQS_TEST_QUEUE_URL="https://sqs.ap-south-1.amazonaws.com/625858709968/email-queue"
export SQS_TEST_REGION="ap-south-1"
export SQS_TEST_ACCESS_KEY_ID="your-access-key-id"
export SQS_TEST_SECRET_ACCESS_KEY="your-secret-access-key"
```

**Test Commands**:

```bash
# Run SQS e2e tests (requires AWS credentials)
go test -v ./e2e/ -run TestSQS

# Run specific SQS test
go test -v ./e2e/ -run TestSQSQueueIntegration

# Run SQS concurrent test
go test -v ./e2e/ -run TestSQSQueueConcurrentDispatch

# Run SQS health check test
go test -v ./e2e/ -run TestSQSQueueHealthCheck
```

### All Drivers

```bash
# Run all e2e tests
go test -v ./e2e/

# Run tests excluding SQS (for CI environments without AWS access)
go test -v ./e2e/ -short
```

## Test Features by Driver

| Feature               | Memory | Redis  | SQS |
| --------------------- | ------ | ------ | --- |
| Job Processing        | ✅     | ✅     | ✅  |
| Worker Management     | ✅     | ✅     | ✅  |
| Metrics Collection    | ✅     | ✅     | ✅  |
| Health Monitoring     | ✅     | ✅     | ✅  |
| Concurrent Operations | ✅     | ✅     | ✅  |
| Graceful Shutdown     | ✅     | ✅     | ✅  |

\*\*Redis tests now include comprehensive metrics testing with proper callback validation

## SQS Test Configuration

The SQS tests use AWS credentials from environment variables and connect to a live SQS queue.

### Environment Variables

The following environment variables must be set to run SQS tests:

- `SQS_TEST_QUEUE_URL`: The full URL of your SQS queue
- `SQS_TEST_REGION`: AWS region where the queue is located
- `SQS_TEST_ACCESS_KEY_ID`: Your AWS access key ID
- `SQS_TEST_SECRET_ACCESS_KEY`: Your AWS secret access key

### Security

- **Never commit AWS credentials to version control**
- Tests are automatically skipped if environment variables are not set
- Use `-short` flag to skip SQS tests in CI environments

**Note**: SQS tests are skipped when running with `-short` flag to avoid requiring AWS credentials in CI environments.

## Test Design Patterns

### Job Registration

All test jobs must be registered with the GoQueue registry:

```go
func init() {
    goqueue.RegisterJob("JobTypeName", func() goqueue.Job {
        return &JobTypeName{}
    })
}
```

### Metrics Tracking

Tests use metrics callbacks to track job completion:

```go
cfg := config.NewSQSConfig(...).WithMetricsCallback(func(metrics config.JobMetrics) {
    // Track job completion
})
```

### Rate Limiting (SQS)

SQS tests implement rate limiting to respect AWS service limits:

- Controlled concurrent dispatching with semaphores
- Delays between job dispatches
- Extended timeouts for job processing

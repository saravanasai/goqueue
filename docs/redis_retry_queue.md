# Redis Retry Queue Implementation

This document explains the non-blocking retry mechanism implemented for the Redis driver in goqueue.

## Overview

The Redis driver now implements a sophisticated retry mechanism that does not block worker threads. Instead of using `time.Sleep()` to delay retries, failed jobs are pushed to a dedicated retry queue in Redis with their next retry timestamp, allowing workers to continue processing other jobs.

## Architecture

### Retry Queue Structure

- **Main Queue**: `queues:{queue-name}` - Contains jobs ready for immediate processing
- **Retry Queue**: `retry:{queue-name}` - Redis sorted set where jobs await retry
- **Processing Queue**: `processing:{queue-name}` - Temporary storage for jobs being processed
- **Job Index**: `job_index:{queue-name}` - Hash map for job metadata lookup

### Key Components

1. **Retry Poller**: Background goroutine that moves jobs from retry queue to main queue
2. **Lua Scripts**: Atomic operations to prevent race conditions
3. **Job Metadata**: Enhanced job context with retry count tracking

## Implementation Details

### Retry Logic Flow

1. **Job Fails**: When a job fails but has remaining retry attempts:

   - Calculate delay using exponential backoff (if enabled)
   - Increment retry count in job metadata
   - Push job to retry sorted set with `retry_timestamp` as score
   - Clean up current processing state
   - Continue processing other jobs immediately

2. **Retry Poller**: Runs every 1 second:

   - Scans all retry queues for jobs where `score <= current_time`
   - Atomically moves ready jobs from retry queue to main queue
   - Uses Lua script to prevent duplicate processing

3. **Max Retries Exceeded**:
   - Jobs that exceed `MaxRetryAttempts` are sent to Dead Letter Queue (DLQ)
   - Jobs are acknowledged and removed from processing

### Exponential Backoff

When enabled, retry delays follow the pattern:

- Attempt 1: `RetryDelay * 1` (e.g., 200ms)
- Attempt 2: `RetryDelay * 2` (e.g., 400ms)
- Attempt 3: `RetryDelay * 4` (e.g., 800ms)
- And so on...

Formula: `delay = RetryDelay * 2^(retry_count)`

### Atomic Operations

The implementation uses Lua scripts to ensure atomicity:

```lua
-- Move jobs from retry queue to main queue
local retry_key = KEYS[1]
local main_queue = KEYS[2]
local current_time = tonumber(ARGV[1])

local jobs = redis.call('ZRANGEBYSCORE', retry_key, '-inf', current_time, 'LIMIT', 0, 10)
if #jobs == 0 then return 0 end

local moved = 0
for _, job in ipairs(jobs) do
    local removed = redis.call('ZREM', retry_key, job)
    if removed == 1 then
        redis.call('LPUSH', main_queue, job)
        moved = moved + 1
    end
end
return moved
```

### Job Metadata Structure

```go
type RedisQueuedJob struct {
    Job        json.RawMessage `json:"job"`        // Serialized job data
    ID         string          `json:"id"`         // Unique job identifier
    JobName    string          `json:"job_name"`   // Job type for deserialization
    EnqueuedAt time.Time       `json:"enqueued_at"` // Original enqueue timestamp
    RetryCount int             `json:"retry_count"` // Number of retry attempts
}

type JobContext struct {
    Job        Job           // Actual job implementation
    JobID      string        // Unique identifier
    QueueName  string        // Queue name
    EnqueuedAt time.Time     // Original enqueue time
    Timeout    time.Duration // Job-specific timeout
    RetryCount int           // Current retry count
}
```

## Performance Benefits

### Non-Blocking Workers

- **Before**: Workers blocked on `time.Sleep()` during retries
- **After**: Workers immediately available for new jobs
- **Result**: Higher throughput and better resource utilization

### Efficient Redis Operations

- **ZRANGEBYSCORE**: Efficiently finds jobs ready for retry
- **Lua Scripts**: Atomic operations prevent race conditions
- **Limited Batch Size**: Processes max 10 jobs per poller cycle to avoid memory spikes

### Scalability

- **Multiple Workers**: All workers can process jobs without conflicts
- **Distributed Processing**: Retry mechanism works across multiple application instances
- **Fault Tolerance**: Retry poller handles Redis connection failures gracefully

## Configuration

```go
// Enable exponential backoff with custom settings
cfg := config.NewRedisConfig("localhost:6379", "", 0).
    WithMaxRetryAttempts(5).
    WithRetryDelay(1 * time.Second).
    WithExponentialBackoff(true).
    WithDLQAdapter(myDLQAdapter)

queue, err := goqueue.NewQueueWithDefaults("my-queue", cfg)
```

## Error Handling

### Connection Failures

- Retry poller gracefully handles Redis connection errors
- No logging spam during shutdown or network issues
- Workers continue processing available jobs

### Retry Exhaustion

- Jobs exceeding max retries are sent to configured DLQ
- If no DLQ is configured, jobs are discarded with warning
- Proper cleanup ensures no memory leaks

### Race Conditions

- Lua scripts ensure atomic operations
- Job index prevents duplicate processing
- Processing queue cleanup handles worker crashes

## Testing

The implementation includes comprehensive E2E tests:

1. **Basic Retry**: Verifies jobs are retried correct number of times
2. **Exponential Backoff**: Validates increasing delay between retries
3. **DLQ Integration**: Ensures failed jobs go to dead letter queue
4. **Multi-Worker**: Tests concurrent processing without conflicts
5. **Crash Recovery**: Verifies retry mechanism survives worker failures

## Migration from Blocking Retries

For existing users, the change is transparent:

- **Memory/SQS Drivers**: Continue using blocking retries with `time.Sleep()`
- **Redis Driver**: Automatically uses non-blocking retry queue
- **Configuration**: No changes required to existing configurations
- **Backward Compatibility**: All existing features continue to work

## Monitoring and Observability

### Logs

- Retry queue operations logged at INFO level
- Failed job details logged at ERROR level
- Connection issues filtered to reduce noise

### Metrics

- Retry queue depth can be monitored via Redis commands
- Job processing times improved due to non-blocking retries
- DLQ metrics available through configured adapters

### Redis Commands for Monitoring

```bash
# Check retry queue depth
ZCARD retry:my-queue

# View jobs awaiting retry
ZRANGE retry:my-queue 0 -1 WITHSCORES

# Check main queue length
LLEN my-queue

# Monitor processing queue
LLEN processing:my-queue
```

## Best Practices

1. **Retry Limits**: Set reasonable `MaxRetryAttempts` to avoid infinite loops
2. **Delay Configuration**: Use exponential backoff for transient failures
3. **DLQ Setup**: Always configure a dead letter queue for failed jobs
4. **Monitoring**: Monitor retry queue depths and DLQ accumulation
5. **Resource Planning**: Account for retry storage in Redis memory planning

## Future Enhancements

Potential improvements for future versions:

- Configurable retry poller interval
- Priority-based retry scheduling
- Retry queue analytics and metrics
- Batch processing optimizations
- Custom retry strategies per job type

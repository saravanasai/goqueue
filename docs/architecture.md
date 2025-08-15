# GoQueue Architecture

## Goal

Provide a simple, production‑oriented background job system for Go with pluggable backends (In‑Memory, Redis, AWS SQS), clean dev experience, and predictable semantics (retries, DLQ, middleware, metrics, graceful shutdown).

## High‑Level Overview

Core idea: Applications produce Jobs and enqueue them via a Queue. Workers consume jobs, run user‑defined Handlers, and report results. The queue storage and transport are abstracted behind Drivers so you can swap In‑Memory, Redis, or AWS SQS without changing application code.

```
Producer → Queue API → Driver (In‑Mem | Redis | SQS) → Worker Pool → Dispatcher → Handler → Result
```

## Key Properties

- **Pluggable drivers** behind a common interface.
- **Job registry** for (de)serialization of typed jobs.
- **Middleware pipeline** for cross‑cutting concerns (logging, rate‑limit, tracing…).
- **Retries + backoff** and optional Dead‑Letter Queue (DLQ) adapter.
- **Metrics callback hook** for observability.
- **Graceful shutdown** & concurrency control.

## Packages & Responsibilities

| Package       | Responsibility                                                      |
| ------------- | ------------------------------------------------------------------- |
| `/adapter`    | Optional integrations (e.g., Redis DLQ adapter)                     |
| `/config`     | User‑facing configuration builders & options                        |
| `/dispatcher` | Job type registry + dispatch to user handler                        |
| `/internal`   | Shared internals (helpers, backoff, clocks, errors)                 |
| `/job`        | Job model, envelope, (de)serialization utilities                    |
| `/middleware` | Middleware interfaces + built‑ins (logging, conditional skip, etc.) |
| `/queue`      | Core queue interfaces + driver implementations                      |
| `/worker`     | Worker loop, pool management, concurrency, shutdown                 |

## Retry Flow

GoQueue implements a unified retry mechanism across all drivers while leveraging each backend's native capabilities for optimal performance and reliability.

### Unified Retry Logic

All drivers follow the same logical retry flow:

1. **Job Failure**: When a job fails and has remaining retry attempts:

   - Calculate retry delay using exponential backoff (if enabled)
   - Increment retry count in job metadata
   - Schedule job for retry using driver-specific mechanism
   - Workers remain available for other jobs (non-blocking)

2. **Retry Scheduling**:

   - **Redis**: Uses internal retry queue with sorted set (timestamp-based)
   - **SQS**: Uses native `ChangeMessageVisibility` API
   - **Memory**: Uses blocking `time.Sleep()` (fallback for development)

3. **Exponential Backoff**: Same formula across all drivers:

   ```
   delay = RetryDelay * 2^(retry_count)
   ```

4. **Max Retries Exceeded**:
   - Jobs that exceed `MaxRetryAttempts` are sent to Dead Letter Queue (DLQ)
   - Jobs are acknowledged and removed from processing

### Driver-Specific Implementations

#### Redis Driver

- **Retry Queue**: `retry:{queue-name}` (Redis sorted set)
- **Retry Poller**: Background goroutine that moves jobs from retry queue to main queue
- **Non-blocking**: Workers never sleep, immediately available for new jobs
- **Atomic Operations**: Lua scripts prevent race conditions
- **Persistence**: Retry state survives worker restarts

```lua
-- Move jobs from retry queue to main queue when ready
local retry_key = KEYS[1]
local main_queue = KEYS[2]
local current_time = tonumber(ARGV[1])

local jobs = redis.call('ZRANGEBYSCORE', retry_key, '-inf', current_time, 'LIMIT', 0, 10)
-- Atomically move ready jobs to main queue
```

#### SQS Driver

- **Visibility Timeout**: Uses `ChangeMessageVisibility` API for retry delays
- **Non-blocking**: Workers don't sleep, message becomes invisible until retry time
- **Auto-redelivery**: SQS automatically redelivers message after visibility timeout
- **Attempt Tracking**: Retry count stored in message attributes
- **FIFO Support**: Preserves `MessageGroupId` and `MessageDeduplicationId` for ordered queues

```go
// Change message visibility to implement retry delay
_, err := client.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
    QueueUrl:          queueURL,
    ReceiptHandle:     receiptHandle,
    VisibilityTimeout: int32(delay.Seconds()), // Up to 12 hours
})
```

#### Memory Driver

- **Blocking Retry**: Uses `time.Sleep()` in worker goroutine
- **Simple Implementation**: Suitable for development and testing
- **No Persistence**: Retry state lost on application restart

### Retry Consistency Guarantees

1. **Same Retry Timing**: All drivers produce identical retry schedules
2. **Attempt Tracking**: Retry counts are preserved across worker restarts
3. **DLQ Behavior**: Failed jobs go to DLQ after max attempts (all drivers)
4. **Configuration**: Same retry settings apply regardless of driver
5. **Exponential Backoff**: Identical backoff calculation across drivers

### Error Handling

#### Connection Failures

- **Redis**: Retry poller handles connection errors gracefully
- **SQS**: Visibility timeout changes are resilient to temporary failures
- **Memory**: No network dependency

#### Worker Crashes

- **Redis**: Jobs in retry queue are preserved, poller continues
- **SQS**: Messages become visible again after timeout, auto-redelivered
- **Memory**: Jobs lost (in-memory storage limitation)

#### Retry Exhaustion

- **All Drivers**: Jobs exceeding max retries sent to configured DLQ
- **Graceful Degradation**: Jobs discarded with warning if no DLQ configured

### Performance Benefits

1. **Non-blocking Workers**: Redis and SQS workers never sleep during retries
2. **Optimal Resource Usage**: Workers immediately available for new jobs
3. **Native API Usage**: Leverages backend-specific features for efficiency
4. **Horizontal Scaling**: Retry mechanisms work across multiple worker instances

### Migration Compatibility

Applications can switch between Redis and SQS drivers without code changes:

- **Same API**: Retry configuration and behavior identical
- **Same Timings**: Exponential backoff produces identical schedules
- **Same Semantics**: DLQ behavior and job lifecycle consistent
- **No Code Changes**: Only driver configuration needs updating

This unified approach ensures reliable, predictable retry behavior regardless of the underlying queue technology.

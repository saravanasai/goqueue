# NATS JetStream Driver for GoQueue

This document provides details about the NATS JetStream driver implementation for the GoQueue project.

## Overview

The NATS JetStream driver allows GoQueue to use [NATS JetStream](https://docs.nats.io/jetstream) as a backend for job queuing. NATS is a lightweight, high-performance messaging system, and JetStream is its built-in persistence layer, providing durable storage, at-least-once delivery, and native message scheduling — making it well suited for teams already running NATS infrastructure who want to avoid adding a separate broker just for background jobs.

## Key Features

- Native delayed/scheduled job dispatch via JetStream's message scheduler (server 2.12+), requiring no external timer or polling mechanism
- Work-queue retention policy: messages are automatically deleted from the stream once acknowledged, rather than accumulating indefinitely
- Automatic crash recovery: an unacked message is redelivered by the server itself if a worker dies mid-processing, with no additional application-level bookkeeping required
- Configurable delivery cap (`MaxDeliver`) coordinated with GoQueue's own retry logic, so JetStream's server-side redelivery limit never cuts off an in-progress application-level retry
- Long-poll pull consumers (20s default wait) for efficient message retrieval without busy-polling an empty queue
- Async batch publishing for efficient bulk dispatch
- Health checking via cached connection status

## Configuration

### Basic Configuration

```go
// Basic configuration
cfg := config.NewNATSConfig(
    "nats://localhost:4222", // NATS server URL
    "email-queue",           // JetStream stream name jobs will be stored under
)
```

### Advanced Configuration

```go
natsCfg := cfg.DriverConfig.(config.NATSConfig)
natsCfg.MaxDeliver = 10                    // JetStream's own redelivery cap; must exceed MaxRetryAttempts
natsCfg.AckWait = 45 * time.Second         // How long JetStream waits for an ack before redelivering
natsCfg.MaxMessages = 5                    // Messages fetched per Pop call
natsCfg.RequestTimeout = 15 * time.Second  // Timeout for synchronous JetStream calls (Publish, consumer setup)
cfg.DriverConfig = natsCfg
```

**Note on `MaxDeliver`:** if left unset (`0`), GoQueue automatically derives a safe value (`MaxRetryAttempts + 2`) at store creation time. This exists because a Nak (used internally by `RetryJobWithMetadata`) counts against JetStream's own delivery counter — if `MaxDeliver` were lower than or equal to `MaxRetryAttempts`, JetStream could stop redelivering a job before GoQueue's own retry logic considers it exhausted.

## Implementation Details

### Message Format

Jobs are wrapped in a `NATSQueuedJob` envelope before publishing:

- `Job`: the serialized job payload (raw JSON)
- `ID`: a unique job identifier
- `JobName`: the registered job type name, used to reconstruct the concrete type on `Pop`
- `EnqueuedAt`: timestamp the job was queued
- `RetryCount`: informational only — see the retry-count note below

### Subject Layout

Each `Queue` instance maps to one JetStream stream, using a wildcard subject (`<StreamName>.>`) so a single stream can serve arbitrary queue names passed to `Push`/`Pop`:

- Real job traffic: `<StreamName>.<queueName>`
- Metrics: `<StreamName>.<queueName>-metrics`
- Scheduled (delayed) jobs, before they fire: `<StreamName>.schedules.<queueName>.<jobID>`

### Retention: Work Queue Policy

The stream is created with `Retention: WorkQueuePolicy`, meaning a message is deleted from the stream once acknowledged — matching job-queue semantics (process once, discard). This differs from JetStream's default `LimitsPolicy`, which would retain messages until age/size/count limits are hit regardless of ack state.

### Delayed Jobs (JetStream Message Scheduler)

Delayed dispatch (`DispatchWithDelay`) is implemented using JetStream's native message scheduler (server 2.12+): the job is published to a per-job schedule subject with two headers —

- `Nats-Schedule: @at <RFC3339 timestamp>` — when to fire
- `Nats-Schedule-Target: <real subject>` — where to deliver it once the time passes

JetStream holds the message and republishes it to the target subject automatically once the scheduled time arrives; no polling, sweeper, or external timer is required by the driver itself.

### Retry Count Tracking

`RetryJobWithMetadata` uses JetStream's `NakWithDelay`, which redelivers the *same* message rather than reconstructing a new one — meaning the `RetryCount` field inside the JSON envelope is never rewritten after the initial `Push`. To report an accurate retry count, `Pop` instead reads JetStream's server-tracked `NumDelivered` value from the message's metadata (`NumDelivered - 1`), falling back to the JSON field only if metadata is unavailable.

### Acknowledgment

When a job completes successfully, `Ack` calls the message's native `Ack()` method. Combined with `WorkQueuePolicy` retention, this both stops redelivery and removes the message from the stream.

### Crash Recovery

If a worker pops a job but crashes before acking or naking it, JetStream's `AckWait` timeout automatically makes the message eligible for redelivery — no additional tracking is needed at the application level, unlike backends without a native visibility/ack-wait mechanism.

## Required NATS Server Version

- **NATS Server 2.9+** — minimum for JetStream support in general
- **NATS Server 2.12+** — required for the message scheduler feature (`AllowMsgSchedules`), which backs delayed/scheduled job dispatch
- **NATS Server 2.14+** — adds recurring/cron-style schedules to the same mechanism (not currently used by this driver, which only issues one-time `@at` schedules)

## Performance Considerations

- Pull consumers long-poll for 20 seconds by default when a queue is empty, avoiding busy-polling
- `PushBatch` publishes jobs asynchronously and waits for all acks together, rather than one round trip per message
- Default max payload size is 1MB (matching JetStream's out-of-the-box `max_payload` server setting); larger payloads are rejected client-side with a clear error before being sent
- Unlike SQS, NATS maintains a persistent connection rather than a per-call HTTP round trip, so publishing does not carry per-message connection overhead
- Consumers are cached per queue name after first use, avoiding redundant consumer-creation calls on every `Pop`

## Error Handling

The driver handles various NATS-specific conditions:

- Connection failures (wrapped and logged at store construction)
- Publish/fetch timeouts, bounded by a configurable `RequestTimeout` on all synchronous JetStream calls — without this, a slow or unreachable server could otherwise block a worker indefinitely
- Empty-queue conditions (`jetstream.ErrNoMessages` and `nats.ErrTimeout`) are treated as a normal empty result, not a failure
- Message payload size limit (1MB default) validation, matching the pattern used by the SQS driver's 256KB check
- Cached health status, updated whenever a publish or fetch operation fails

## Testing

The driver includes:

- **Unit tests** (`nats_store_test.go`) — one behavior per test, using a mocked `jetstream.JetStream`/`Consumer`/`Msg` (built via interface embedding + method overrides, since the real interfaces expose more methods than this driver uses)
- **Workflow tests** (`nats_store_integration_test.go`) — mocked, multi-step scenarios chaining several `Store` methods together (e.g. `Push` → `Pop` → `Ack`), following the same convention as the SQS driver's integration test file. These do not require a real NATS server.
- **Manual verification tests** (not included in this PR) — a separate, locally-run file gated behind a `manual_verify` build tag, used during development to confirm behavior (scheduled delivery timing, work-queue deletion on ack, retry-count incrementing) against a real NATS server. Not part of the committed test suite.

To run the mocked unit and workflow tests:

```bash
go test -v ./adapter/nats/...
```

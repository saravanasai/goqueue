# AWS SQS Driver for GoQueue

This document provides details about the AWS SQS driver implementation for the GoQueue project.

## Overview

The AWS SQS driver allows GoQueue to use Amazon Simple Queue Service (SQS) as a backend for job queuing. SQS is a fully managed message queuing service that enables you to decouple and scale microservices, distributed systems, and serverless applications.

## Key Features

- Long polling (WaitTimeSeconds = 20) for efficient message retrieval
- Configurable MaxMessages (1-10) for batch receiving
- Configurable VisibilityTimeout for message processing
- Support for message attributes to store job metadata
- Dead Letter Queue (DLQ) support for failed jobs
- Automatic credential loading from environment or instance profiles
- Health checking for SQS connection

## Configuration

```go
// Basic configuration
cfg := config.NewSQSConfig(
    "https://sqs.us-west-2.amazonaws.com/123456789012/my-queue", // queueURL
    "us-west-2",                                                 // region
    "AKIAIOSFODNN7EXAMPLE",                                      // accessKeyID (optional)
    "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"                  // secretAccessKey (optional)
)

// With DLQ support
cfg := sqs.NewSQSConfigWithDLQ(
    "https://sqs.us-west-2.amazonaws.com/123456789012/my-queue", // queueURL
    "https://sqs.us-west-2.amazonaws.com/123456789012/my-dlq",   // dlqURL
    "us-west-2",                                                 // region
    "",                                                          // accessKeyID (use env or instance profile)
    "",                                                          // secretAccessKey (use env or instance profile)
    logger                                                       // logger instance
)

// Configure MaxMessages and VisibilityTimeout
sqsCfg := cfg.DriverConfig.(config.SQSConfig)
sqsCfg.MaxMessages = 5                            // 1-10 messages per batch
sqsCfg.VisibilityTimeout = 2 * time.Minute        // 2 minute timeout
cfg.DriverConfig = sqsCfg
```

## Implementation Details

### Message Format

SQS messages include:

- Message body: JSON-serialized job data
- Message attributes: Metadata like JobID, QueueName, etc.

### Acknowledgment

When a job completes successfully, the message is deleted from SQS using the receipt handle.

### Dead Letter Queue

Failed jobs (after max retries) are sent to a Dead Letter Queue if configured. The DLQ message includes:

- Original job data
- Error information
- Metadata (job ID, queue name, timestamp)

### Authentication

The driver supports:

1. Explicit credentials in the config
2. Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
3. EC2 instance profiles
4. ECS task roles
5. Other standard AWS credential sources

## Required AWS Permissions

The SQS driver requires the following AWS permissions:

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
        "arn:aws:sqs:region:account-id:queue-name",
        "arn:aws:sqs:region:account-id:dlq-name"
      ]
    }
  ]
}
```

## Performance Considerations

- SQS has a limit of 300 TPS for API operations
- Long polling reduces empty receives and API calls
- Batch operations (SendMessageBatch) are more efficient for high-throughput scenarios
- Message size limit is 256KB
- Consider region selection for lower latency

## Error Handling

The driver handles various AWS-specific errors:

- Authentication failures
- Network issues
- Service unavailability
- Message format errors
- Visibility timeout expiration

## Testing

The driver includes:

- Unit tests with mocked SQS client
- Integration test that can be run against a real SQS queue

To run integration tests:

```bash
export SQS_INTEGRATION_TEST=true
export SQS_QUEUE_URL=https://sqs.us-west-2.amazonaws.com/123456789012/my-queue
export AWS_REGION=us-west-2
export AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
export AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
go test -v ./adapter/sqs/...
```

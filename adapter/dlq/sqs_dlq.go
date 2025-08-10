// Package dlq provides the Dead Letter Queue (DLQ) implementation for various backends.
// This file contains the AWS SQS implementation for DLQ.
package dlq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/saravanasai/goqueue/internal/logger"
	"github.com/saravanasai/goqueue/job"
)

// SQSDLQ implements the DLQAdapter interface for AWS SQS.
type SQSDLQ struct {
	// client is the AWS SQS client
	client *sqs.Client
	// dlqURL is the URL of the SQS Dead Letter Queue
	dlqURL string
	// logger for errors and debugging
	logger logger.Logger
}

// sqsDLQEntry represents an entry in the Dead Letter Queue
type sqsDLQEntry struct {
	Job       json.RawMessage `json:"job"`
	JobID     string          `json:"job_id"`
	QueueName string          `json:"queue_name"`
	Error     string          `json:"error"`
	FailedAt  time.Time       `json:"failed_at"`
}

// NewSQSDLQ creates a new SQS Dead Letter Queue adapter
func NewSQSDLQ(client *sqs.Client, dlqURL string, logger logger.Logger) *SQSDLQ {
	return &SQSDLQ{
		client: client,
		dlqURL: dlqURL,
		logger: logger,
	}
}

// Push implements the DLQAdapter interface by sending a failed job to the SQS Dead Letter Queue
func (d *SQSDLQ) Push(ctx context.Context, job *job.JobContext, err error) error {
	// Serialize the job
	jobData, marshalErr := json.Marshal(job.Job)
	if marshalErr != nil {
		d.logger.Error("Failed to marshal job for DLQ", "error", marshalErr)
		return fmt.Errorf("failed to marshal job for DLQ: %w", marshalErr)
	}

	// Create DLQ entry
	entry := sqsDLQEntry{
		Job:       jobData,
		JobID:     job.JobID,
		QueueName: job.QueueName,
		Error:     err.Error(),
		FailedAt:  time.Now(),
	}

	// Serialize the entry
	entryData, marshalErr := json.Marshal(entry)
	if marshalErr != nil {
		d.logger.Error("Failed to marshal DLQ entry", "error", marshalErr)
		return fmt.Errorf("failed to marshal DLQ entry: %w", marshalErr)
	}

	// Send to SQS DLQ
	_, sendErr := d.client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    aws.String(d.dlqURL),
		MessageBody: aws.String(string(entryData)),
		MessageAttributes: map[string]types.MessageAttributeValue{
			"JobID": {
				DataType:    aws.String("String"),
				StringValue: aws.String(job.JobID),
			},
			"QueueName": {
				DataType:    aws.String("String"),
				StringValue: aws.String(job.QueueName),
			},
			"ErrorType": {
				DataType:    aws.String("String"),
				StringValue: aws.String(fmt.Sprintf("%T", err)),
			},
		},
	})

	if sendErr != nil {
		d.logger.Error("Failed to push to SQS DLQ", "error", sendErr)
		return fmt.Errorf("failed to push to SQS DLQ: %w", sendErr)
	}

	d.logger.Info("Job pushed to SQS DLQ", "jobID", job.JobID, "queue", job.QueueName)
	return nil
}

package dlq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/danish-a1/goqueue/internal/logger"
	"github.com/danish-a1/goqueue/job"
)

const dlqPrefix = "dlq:"

type RedisDLQ struct {
	client *redis.Client
	logger logger.Logger
}

type dlqEntry struct {
	Job       json.RawMessage `json:"job"`
	JobID     string          `json:"job_id"`
	QueueName string          `json:"queue_name"`
	Error     string          `json:"error"`
	FailedAt  time.Time       `json:"failed_at"`
}

func NewRedisDLQ(client *redis.Client, logger logger.Logger) *RedisDLQ {
	return &RedisDLQ{
		client: client,
		logger: logger,
	}
}

func (r *RedisDLQ) Push(ctx context.Context, job *job.JobContext, err error) error {
	// Serialize the job
	jobData, err := json.Marshal(job.Job)
	if err != nil {
		r.logger.Error("Failed to marshal job for DLQ", "error", err)
		return fmt.Errorf("failed to marshal job for DLQ: %w", err)
	}

	// Create DLQ entry
	entry := dlqEntry{
		Job:       jobData,
		JobID:     job.JobID,
		QueueName: job.QueueName,
		Error:     err.Error(),
		FailedAt:  time.Now(),
	}

	// Serialize the entry
	entryData, err := json.Marshal(entry)
	if err != nil {
		r.logger.Error("Failed to marshal DLQ entry", "error", err)
		return fmt.Errorf("failed to marshal DLQ entry: %w", err)
	}

	// Push to Redis DLQ list
	dlqKey := dlqPrefix + job.QueueName
	if err := r.client.LPush(ctx, dlqKey, entryData).Err(); err != nil {
		r.logger.Error("Failed to push to DLQ", "error", err)
		return fmt.Errorf("failed to push to DLQ: %w", err)
	}

	r.logger.Info("Job pushed to DLQ", "jobID", job.JobID, "queue", job.QueueName)
	return nil
}

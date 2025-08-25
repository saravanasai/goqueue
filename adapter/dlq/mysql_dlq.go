package dlq

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/saravanasai/goqueue/adapter/utils"
	"github.com/saravanasai/goqueue/internal/logger"
	"github.com/saravanasai/goqueue/job"
)

// MySQLDLQ implements the DLQ interface using a MySQL database
type MySQLDLQ struct {
	db     *sql.DB
	logger logger.Logger
}

// NewMySQLDLQ creates a new MySQL Dead Letter Queue
func NewMySQLDLQ(db *sql.DB, logger logger.Logger) *MySQLDLQ {
	return &MySQLDLQ{
		db:     db,
		logger: logger,
	}
}

// Push adds a failed job to the dead letter queue
func (m *MySQLDLQ) Push(ctx context.Context, job *job.JobContext, err error) error {
	// Generate an ID for the record
	id, err2 := uuid.NewRandom()
	if err2 != nil {
		m.logger.Error("Failed to generate ID for DLQ", "error", err2)
		return fmt.Errorf("failed to generate ID for DLQ: %w", err2)
	}

	// Serialize the job
	jobData, err2 := json.Marshal(job.Job)
	if err2 != nil {
		m.logger.Error("Failed to marshal job for DLQ", "error", err2)
		return fmt.Errorf("failed to marshal job for DLQ: %w", err2)
	}

	// Get job name
	jobName := utils.GetJobName(job.Job)
	if jobName == "" {
		// Try to determine job name from the job struct if not provided
		jobName = fmt.Sprintf("%T", job.Job)
	}

	// Insert into failed_jobs table - MySQL uses ? as placeholders
	query := `
        INSERT INTO failed_jobs (
            id, uuid, queue_name, job_name, payload, exception, attempts, error
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `

	errorMsg := ""
	if err != nil {
		errorMsg = err.Error()
	}

	_, err2 = m.db.ExecContext(
		ctx,
		query,
		id.String(),
		job.JobID,
		job.QueueName,
		jobName,
		jobData,
		errorMsg,
		job.RetryCount,
		errorMsg,
	)

	if err2 != nil {
		m.logger.Error("Failed to insert job into DLQ", "error", err2)
		return fmt.Errorf("failed to insert job into DLQ: %w", err2)
	}

	return nil
}

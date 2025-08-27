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
	// Start a transaction to ensure atomicity
	tx, err2 := m.db.BeginTx(ctx, nil)
	if err2 != nil {
		m.logger.Error("Failed to begin transaction for DLQ", "error", err2)
		return fmt.Errorf("failed to begin transaction for DLQ: %w", err2)
	}
	defer func() {
		if err2 != nil {
			tx.Rollback()
		}
	}()

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
	insertQuery := `
        INSERT INTO failed_jobs (
            id, uuid, queue_name, job_name, payload, exception, attempts, error
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `

	errorMsg := ""
	if err != nil {
		errorMsg = err.Error()
	}
	job.RetryCount = job.RetryCount + 1
	_, err2 = tx.ExecContext(
		ctx,
		insertQuery,
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

	// Now delete the job from the jobs table
	deleteQuery := `
        DELETE FROM jobs
        WHERE id = ?
    `

	_, err2 = tx.ExecContext(ctx, deleteQuery, job.JobID)
	if err2 != nil {
		m.logger.Error("Failed to delete job after moving to DLQ", "error", err2, "jobID", job.JobID)
		return fmt.Errorf("failed to delete job after moving to DLQ: %w", err2)
	}

	// Commit the transaction
	if err2 = tx.Commit(); err2 != nil {
		m.logger.Error("Failed to commit transaction for DLQ", "error", err2)
		return fmt.Errorf("failed to commit transaction for DLQ: %w", err2)
	}

	return nil
}

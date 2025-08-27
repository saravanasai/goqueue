package dlq

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/saravanasai/goqueue/adapter/utils"
	"github.com/saravanasai/goqueue/internal/logger"
	"github.com/saravanasai/goqueue/job"
)

// PostgresDLQ implements the DLQ interface using a PostgreSQL database
type PostgresDLQ struct {
	db     *sql.DB
	logger logger.Logger
}

// NewPostgresDLQ creates a new PostgreSQL Dead Letter Queue
func NewPostgresDLQ(db *sql.DB, logger logger.Logger) *PostgresDLQ {
	return &PostgresDLQ{
		db:     db,
		logger: logger,
	}
}

// Push adds a failed job to the dead letter queue and deletes it from the jobs table
func (p *PostgresDLQ) Push(ctx context.Context, job *job.JobContext, err error) error {
	// Start a transaction to ensure atomicity
	tx, err2 := p.db.BeginTx(ctx, nil)
	if err2 != nil {
		p.logger.Error("Failed to begin transaction for DLQ", "error", err2)
		return fmt.Errorf("failed to begin transaction for DLQ: %w", err2)
	}
	defer func() {
		if err2 != nil {
			tx.Rollback()
		}
	}()

	// Serialize the job
	jobData, err2 := json.Marshal(job.Job)
	if err2 != nil {
		p.logger.Error("Failed to marshal job for DLQ", "error", err2)
		return fmt.Errorf("failed to marshal job for DLQ: %w", err2)
	}

	// Get job name
	jobName := utils.GetJobName(job.Job)
	if jobName == "" {
		jobName = fmt.Sprintf("%T", job.Job)
	}

	// Insert into failed_jobs table
	insertQuery := `
        INSERT INTO failed_jobs (
            uuid, queue_name, job_name, payload, exception, attempts, error
        ) VALUES ($1, $2, $3, $4, $5, $6, $7)
    `

	errorMsg := ""
	if err != nil {
		errorMsg = err.Error()
	}

	job.RetryCount = job.RetryCount + 1

	_, err2 = tx.ExecContext(
		ctx,
		insertQuery,
		job.JobID,
		job.QueueName,
		jobName,
		jobData,
		errorMsg,
		job.RetryCount,
		errorMsg,
	)

	if err2 != nil {
		p.logger.Error("Failed to insert job into DLQ", "error", err2)
		return fmt.Errorf("failed to insert job into DLQ: %w", err2)
	}

	// Now delete the job from the jobs table
	deleteQuery := `
        DELETE FROM jobs
        WHERE id = $1
    `

	_, err2 = tx.ExecContext(ctx, deleteQuery, job.JobID)
	if err2 != nil {
		p.logger.Error("Failed to delete job after moving to DLQ", "error", err2, "jobID", job.JobID)
		return fmt.Errorf("failed to delete job after moving to DLQ: %w", err2)
	}

	// Commit the transaction
	if err2 = tx.Commit(); err2 != nil {
		p.logger.Error("Failed to commit transaction for DLQ", "error", err2)
		return fmt.Errorf("failed to commit transaction for DLQ: %w", err2)
	}

	return nil
}

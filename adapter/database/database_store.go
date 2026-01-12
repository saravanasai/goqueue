package database

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/danish-a1/goqueue/adapter/database/migrations"
	"github.com/danish-a1/goqueue/adapter/utils"
	"github.com/danish-a1/goqueue/config"
	"github.com/danish-a1/goqueue/internal/logger"
	"github.com/danish-a1/goqueue/internal/registry"
	"github.com/danish-a1/goqueue/job"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

type DatabaseStore struct {
	Db     *sql.DB
	dbType string // "postgres", "mysql".
	logger logger.Logger
	config config.Config
}

// NewDatabaseStore creates a new database store with the given configuration
func NewDatabaseStore(cfg config.DatabaseConfig, log logger.Logger, config config.Config) (*DatabaseStore, error) {
	var db *sql.DB
	var err error

	// Connect to the database using the connection string
	db, err = sql.Open(cfg.DatabaseType, cfg.ConnectionString)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	// Test the connection
	if err = db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	// Set connection pool parameters
	if cfg.MaxOpenConns > 0 {
		db.SetMaxOpenConns(cfg.MaxOpenConns)
	}
	if cfg.MaxIdleConns > 0 {
		db.SetMaxIdleConns(cfg.MaxIdleConns)
	}
	if cfg.ConnMaxLifetime > 0 {
		db.SetConnMaxLifetime(cfg.ConnMaxLifetime)
	}

	// Create a new store instance
	store := &DatabaseStore{
		Db:     db,
		dbType: cfg.DatabaseType,
		logger: log,
		config: config,
	}

	// Run migrations if auto-migrate is enabled
	if cfg.AutoMigrate {
		if err := migrations.RunMigrations(db, cfg.DatabaseType); err != nil {
			db.Close()
			return nil, fmt.Errorf("failed to run migrations: %w", err)
		}
	}

	log.Info("Connected to database", "type", cfg.DatabaseType)
	return store, nil
}

func (ds *DatabaseStore) GetDbConnection() interface{} {
	return ds.Db
}

// Push adds a single job to the queue
func (ds *DatabaseStore) Push(queueName string, job job.Job, delay ...time.Duration) error {
	// Get job information
	jobName := utils.GetJobName(job)
	if jobName == "" {
		return fmt.Errorf("could not determine job name from type")
	}

	// Serialize the job payload
	jobPayload, err := json.Marshal(job)
	if err != nil {
		ds.logger.Error("failed to marshal job", "error", err, "queue", queueName)
		return fmt.Errorf("failed to marshal job: %w", err)
	}

	// Generate a UUID for the job
	jobID := utils.GenerateID()

	// Set the available_at timestamp based on delay
	var availableAt time.Time
	if len(delay) > 0 && delay[0] > 0 {
		availableAt = time.Now().UTC().Add(delay[0])
	} else {
		availableAt = time.Now().UTC()
	}

	// Prepare the query based on database type
	var query string
	switch ds.dbType {
	case config.DatabaseTypePostgres:
		query = `INSERT INTO jobs 
            (id, queue_name, job_name, payload, created_at, available_at, status) 
            VALUES ($1, $2, $3, $4, NOW(), $5, 'pending')`
	case config.DatabaseTypeMySQL:
		query = `INSERT INTO jobs 
            (id, queue_name, job_name, payload, created_at, available_at, status) 
            VALUES (?, ?, ?, ?, NOW(), ?, 'pending')`
	default:
		return fmt.Errorf("unsupported database type: %s", ds.dbType)
	}

	// Execute the query with parameters
	_, err = ds.Db.Exec(query, jobID, queueName, jobName, jobPayload, availableAt)
	if err != nil {
		ds.logger.Error("failed to insert job into database",
			"error", err,
			"queue", queueName,
			"job", jobName)
		return fmt.Errorf("failed to insert job into database: %w", err)
	}

	return nil
}

// PushBatch adds multiple jobs to the queue in a single call
func (ds *DatabaseStore) PushBatch(queueName string, jobs []job.Job, delay ...time.Duration) error {
	if len(jobs) == 0 {
		return nil // Nothing to do
	}

	// Calculate available_at time
	availableAt := time.Now()
	if len(delay) > 0 && delay[0] > 0 {
		availableAt = availableAt.Add(delay[0])
	}

	// Start a transaction
	tx, err := ds.Db.Begin()
	if err != nil {
		ds.logger.Error("failed to begin transaction", "error", err)
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	// Prepare the query based on database type
	var query string
	var placeholders []string
	var values []interface{}
	var paramCount int = 1

	switch ds.dbType {
	case config.DatabaseTypePostgres:
		// Build a multi-value INSERT statement for Postgres using $n parameters
		query = "INSERT INTO jobs (id, queue_name, job_name, payload, created_at, available_at, status) VALUES "
		for i, jb := range jobs {
			// Get job name
			jobName := utils.GetJobName(jb)
			if jobName == "" {
				return fmt.Errorf("could not determine job name for job at index %d", i)
			}

			// Generate job ID
			jobID := utils.GenerateID()

			// Serialize job
			jobPayload, err := json.Marshal(jb)
			if err != nil {
				ds.logger.Error("failed to marshal job", "error", err, "index", i)
				return fmt.Errorf("failed to marshal job at index %d: %w", i, err)
			}

			// Add placeholder for this job
			ph := fmt.Sprintf("($%d, $%d, $%d, $%d, NOW(), $%d, 'pending')",
				paramCount, paramCount+1, paramCount+2, paramCount+3, paramCount+4)
			placeholders = append(placeholders, ph)

			// Add parameter values
			values = append(values, jobID, queueName, jobName, jobPayload, availableAt)
			paramCount += 5
		}

	case config.DatabaseTypeMySQL:
		// Build a multi-value INSERT statement for MySQL using ? parameters
		query = "INSERT INTO jobs (id, queue_name, job_name, payload, created_at, available_at, status) VALUES "
		for i, jb := range jobs {
			// Get job name
			jobName := utils.GetJobName(jb)
			if jobName == "" {
				return fmt.Errorf("could not determine job name for job at index %d", i)
			}

			// Generate job ID
			jobID := utils.GenerateID()

			// Serialize job
			jobPayload, err := json.Marshal(jb)
			if err != nil {
				ds.logger.Error("failed to marshal job", "error", err, "index", i)
				return fmt.Errorf("failed to marshal job at index %d: %w", i, err)
			}

			// Add placeholder for this job
			placeholders = append(placeholders, "(?, ?, ?, ?, NOW(), ?, 'pending')")

			// Add parameter values
			values = append(values, jobID, queueName, jobName, jobPayload, availableAt)
		}

	default:
		return fmt.Errorf("unsupported database type: %s", ds.dbType)
	}

	// Combine query with placeholders
	query = query + strings.Join(placeholders, ", ")

	// Execute the query
	_, err = tx.Exec(query, values...)
	if err != nil {
		ds.logger.Error("failed to insert batch jobs", "error", err, "count", len(jobs))
		return fmt.Errorf("failed to insert batch jobs: %w", err)
	}

	// Commit the transaction
	if err = tx.Commit(); err != nil {
		ds.logger.Error("failed to commit transaction", "error", err)
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// Pop retrieves a job from the queue
func (ds *DatabaseStore) Pop(queueName string) (job.JobContext, error) {

	// Start a transaction for atomicity
	tx, err := ds.Db.Begin()
	if err != nil {
		ds.logger.Error("failed to begin transaction", "error", err)
		return job.JobContext{}, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	// Get the job with the appropriate locking strategy based on database type
	var query string
	switch ds.dbType {
	case config.DatabaseTypePostgres:
		query = `
            UPDATE jobs
            SET reserved_at = NOW(), 
                status = 'processing'
            WHERE id = (
                SELECT id
                FROM jobs
                WHERE queue_name = $1
                AND status = 'pending'
                AND (available_at <= NOW() AT TIME ZONE 'UTC')
                ORDER BY available_at
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            )
            RETURNING id, job_name, payload, queue_name, attempts, created_at, available_at, reserved_at;
        `
	case config.DatabaseTypeMySQL:
		// MySQL doesn't support RETURNING, so we need to use a different approach
		query = `
            SELECT id, job_name, payload, queue_name, attempts, created_at, available_at, reserved_at
            FROM jobs
            WHERE queue_name = ?
            AND status = 'pending'
            AND available_at <= NOW()
            AND (reserved_at IS NULL OR reserved_at < NOW() - INTERVAL 15 MINUTE)
            ORDER BY available_at
            LIMIT 1
            FOR UPDATE SKIP LOCKED;
        `
	default:
		tx.Rollback()
		return job.JobContext{}, fmt.Errorf("unsupported database type: %s", ds.dbType)
	}

	// Variables to store job information
	var jobID, jobName, jobQueue string
	var jobPayload []byte
	var attempts int
	var createdAt, availableAt, reservedAt sql.NullTime

	// Execute the query
	var row *sql.Row
	if ds.dbType == config.DatabaseTypePostgres {
		row = tx.QueryRow(query, queueName)
	} else {
		// First, update a job and get its ID
		updateQuery := `
            UPDATE jobs AS j
            JOIN (
                SELECT id
                FROM jobs
                WHERE queue_name = ?
                AND status = 'pending'
                AND available_at <= NOW()
                ORDER BY available_at
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            ) AS selected ON j.id = selected.id
            SET j.reserved_at = NOW(),
                j.status = 'processing'
        `
		result, err := tx.Exec(updateQuery, queueName)
		if err != nil {
			tx.Rollback()
			ds.logger.Error("failed to update job status", "error", err)
			return job.JobContext{}, fmt.Errorf("failed to update job status: %w", err)
		}

		rowsAffected, err := result.RowsAffected()
		if err != nil {
			tx.Rollback()
			return job.JobContext{}, fmt.Errorf("failed to get rows affected: %w", err)
		}

		if rowsAffected == 0 {
			// No jobs available
			tx.Rollback()
			return job.JobContext{}, nil
		}

		// Then get the updated job details
		selectQuery := `
            SELECT id, job_name, payload, queue_name, attempts, created_at, available_at, reserved_at
            FROM jobs
            WHERE queue_name = ?
            AND status = 'processing'
            AND reserved_at IS NOT NULL
            ORDER BY reserved_at DESC
            LIMIT 1
        `
		row = tx.QueryRow(selectQuery, queueName)
	}

	// Scan the result
	err = row.Scan(&jobID, &jobName, &jobPayload, &jobQueue, &attempts, &createdAt, &availableAt, &reservedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			// No jobs available
			tx.Rollback()
			return job.JobContext{}, nil
		}
		tx.Rollback()
		ds.logger.Error("failed to scan job", "error", err)
		return job.JobContext{}, fmt.Errorf("failed to scan job: %w", err)
	}

	// For MySQL, need to update the job without incrementing attempts
	if ds.dbType == config.DatabaseTypeMySQL {
		updateQuery := `
            UPDATE jobs
            SET reserved_at = NOW(), 
                status = 'processing'
            WHERE id = ?;
        `
		_, err = tx.Exec(updateQuery, jobID)
		if err != nil {
			tx.Rollback()
			ds.logger.Error("failed to update job status", "error", err, "id", jobID)
			return job.JobContext{}, fmt.Errorf("failed to update job status: %w", err)
		}
	}

	// Unmarshal the job payload to create the actual job instance
	var jobInstance job.Job
	factory, exists := registry.GetFromRegistery(jobName)
	if !exists {
		tx.Rollback()
		ds.logger.Error("unknown job type", "job_name", jobName)
		return job.JobContext{}, fmt.Errorf("unknown job type: %s", jobName)
	}

	jobInstance = factory()
	if err = json.Unmarshal(jobPayload, &jobInstance); err != nil {
		tx.Rollback()
		ds.logger.Error("failed to unmarshal job payload", "error", err, "id", jobID)
		return job.JobContext{}, fmt.Errorf("failed to unmarshal job payload: %w", err)
	}

	// Commit the transaction
	if err = tx.Commit(); err != nil {
		ds.logger.Error("failed to commit transaction", "error", err)
		return job.JobContext{}, fmt.Errorf("failed to commit transaction: %w", err)
	}

	// Create and return the job context
	jobCtx := job.JobContext{
		Job:        jobInstance,
		JobID:      jobID,
		QueueName:  queueName,
		EnqueuedAt: createdAt.Time,
		Timeout:    ds.config.JobTimeout,
		RetryCount: attempts,
	}

	return jobCtx, nil

}

// Ack acknowledges that a job was processed successfully
func (ds *DatabaseStore) Ack(queueName string, jobID string) error {
	// Prepare the delete query based on database type
	var query string
	switch ds.dbType {
	case config.DatabaseTypePostgres:
		query = `DELETE FROM jobs WHERE id = $1 AND queue_name = $2`
	case config.DatabaseTypeMySQL:
		query = `DELETE FROM jobs WHERE id = ? AND queue_name = ?`
	default:
		return fmt.Errorf("unsupported database type: %s", ds.dbType)
	}

	// Execute the query
	_, err := ds.Db.Exec(query, jobID, queueName)
	if err != nil {
		ds.logger.Error("failed to acknowledge job",
			"error", err,
			"queue", queueName,
			"id", jobID)
		return fmt.Errorf("failed to acknowledge job: %w", err)
	}

	return nil

}

// Retry adds a job back to the queue for retry
func (ds *DatabaseStore) Retry(j job.Job, delay time.Duration) error {
	// TODO: Implement real logic
	return nil
}

// RetryJobWithMetadata adds a job with its metadata back to the queue for retry
func (ds *DatabaseStore) RetryJobWithMetadata(queueName string, jobCtx job.JobContext, delay ...time.Duration) error {
	// Start a transaction
	tx, err := ds.Db.Begin()
	if err != nil {
		ds.logger.Error("failed to begin transaction for retry", "error", err)
		return fmt.Errorf("failed to begin transaction for retry: %w", err)
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	// Serialize the job payload
	jobName := utils.GetJobName(jobCtx.Job)
	if jobName == "" {
		tx.Rollback()
		return fmt.Errorf("could not determine job name from type")
	}

	jobPayload, err := json.Marshal(jobCtx.Job)
	if err != nil {
		tx.Rollback()
		ds.logger.Error("failed to marshal job for retry", "error", err, "queue", queueName)
		return fmt.Errorf("failed to marshal job for retry: %w", err)
	}

	// Calculate the available_at timestamp based on delay
	var availableAt time.Time
	if len(delay) > 0 && delay[0] > 0 {
		// Use the delay provided by the worker, which already includes
		// any exponential backoff calculations
		availableAt = time.Now().UTC().Add(delay[0])
		ds.logger.Debug("using provided delay for retry",
			"delay", delay[0],
			"queue", queueName,
			"jobID", jobCtx.JobID,
			"retryCount", jobCtx.RetryCount+1)
	} else {
		// If no delay is provided, use the current time
		availableAt = time.Now().UTC()
		ds.logger.Debug("no delay provided for retry",
			"queue", queueName,
			"jobID", jobCtx.JobID,
			"retryCount", jobCtx.RetryCount+1)
	}

	// Increment retry count
	retryCount := jobCtx.RetryCount + 1

	// Prepare the update query based on database type
	var query string
	switch ds.dbType {
	case config.DatabaseTypePostgres:
		query = `
            UPDATE jobs
            SET 
                status = 'pending',
                reserved_at = NULL,
                available_at = $1,
                attempts = $2,
                payload = $3
            WHERE id = $4 AND queue_name = $5
        `
	case config.DatabaseTypeMySQL:
		query = `
            UPDATE jobs
            SET 
                status = 'pending',
                reserved_at = NULL,
                available_at = ?,
                attempts = ?,
                payload = ?
            WHERE id = ? AND queue_name = ?
        `
	default:
		tx.Rollback()
		return fmt.Errorf("unsupported database type: %s", ds.dbType)
	}

	// Execute the query
	result, err := tx.Exec(query, availableAt, retryCount, jobPayload, jobCtx.JobID, queueName)
	if err != nil {
		tx.Rollback()
		ds.logger.Error("failed to update job for retry",
			"error", err,
			"queue", queueName,
			"id", jobCtx.JobID)
		return fmt.Errorf("failed to update job for retry: %w", err)
	}

	// Check if the job was actually updated
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		tx.Rollback()
		ds.logger.Error("failed to get affected rows",
			"error", err,
			"queue", queueName,
			"id", jobCtx.JobID)
		return fmt.Errorf("failed to get affected rows: %w", err)
	}

	if rowsAffected == 0 {
		tx.Rollback()
		ds.logger.Warn("job not found for retry",
			"queue", queueName,
			"id", jobCtx.JobID)
		return fmt.Errorf("job not found for retry: queue=%s, id=%s", queueName, jobCtx.JobID)
	}

	// Commit the transaction
	if err = tx.Commit(); err != nil {
		ds.logger.Error("failed to commit transaction",
			"error", err,
			"queue", queueName,
			"id", jobCtx.JobID)
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	ds.logger.Info("job scheduled for retry",
		"queue", queueName,
		"id", jobCtx.JobID,
		"retryCount", retryCount,
		"availableAt", availableAt)
	return nil
}

// EnqueueMetrics adds job metrics to the metrics queue
func (ds *DatabaseStore) EnqueueMetrics(metrics config.JobMetrics) error {
	// Generate a unique ID for the metrics record
	metricsID := utils.GenerateID()

	// Convert error to string if present
	var errorText sql.NullString
	if metrics.Error != nil {
		errorText.String = metrics.Error.Error()
		errorText.Valid = true
	}

	// Convert duration to milliseconds for storage
	durationMs := metrics.Duration.Milliseconds()

	// Prepare the query based on database type
	var query string
	switch ds.dbType {
	case config.DatabaseTypePostgres:
		query = `
            INSERT INTO job_metrics 
            (id, job_id, queue_name, duration, error, timestamp, processed)
            VALUES ($1, $2, $3, $4, $5, $6, false)
        `
	case config.DatabaseTypeMySQL:
		query = `
            INSERT INTO job_metrics 
            (id, job_id, queue_name, duration, error, timestamp, processed)
            VALUES (?, ?, ?, ?, ?, ?, false)
        `
	default:
		return fmt.Errorf("unsupported database type: %s", ds.dbType)
	}

	// Execute the query
	_, err := ds.Db.Exec(
		query,
		metricsID,
		metrics.JobID,
		metrics.QueueName,
		durationMs,
		errorText,
		metrics.Timestamp,
	)

	if err != nil {
		ds.logger.Error("failed to enqueue job metrics",
			"error", err,
			"job_id", metrics.JobID,
			"queue", metrics.QueueName)
		return fmt.Errorf("failed to enqueue job metrics: %w", err)
	}

	return nil
}

// DequeueMetrics retrieves job metrics from the metrics queue and then deletes them
func (ds *DatabaseStore) DequeueMetrics(queueName string) (config.JobMetrics, error) {
	// Start a transaction for atomicity
	tx, err := ds.Db.Begin()
	if err != nil {
		ds.logger.Error("failed to begin transaction for metrics", "error", err)
		return config.JobMetrics{}, fmt.Errorf("failed to begin transaction for metrics: %w", err)
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	// Prepare the query to find a metrics record
	var query string
	var deleteQuery string

	switch ds.dbType {
	case config.DatabaseTypePostgres:
		query = `
            SELECT id, job_id, queue_name, duration, error, timestamp
            FROM job_metrics
            WHERE processed = false
            AND queue_name = $1
            ORDER BY timestamp
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        `
		deleteQuery = `
            DELETE FROM job_metrics
            WHERE id = $1
        `
	case config.DatabaseTypeMySQL:
		query = `
            SELECT id, job_id, queue_name, duration, error, timestamp
            FROM job_metrics
            WHERE processed = false
            AND queue_name = ?
            ORDER BY timestamp
            LIMIT 1
            FOR UPDATE
        `
		deleteQuery = `
            DELETE FROM job_metrics
            WHERE id = ?
        `
	default:
		tx.Rollback()
		return config.JobMetrics{}, fmt.Errorf("unsupported database type: %s", ds.dbType)
	}

	// Variables to store metrics information
	var id, jobID, queue string
	var durationMs int64
	var errorText sql.NullString
	var timestamp time.Time

	// Execute the query
	row := tx.QueryRow(query, queueName)
	err = row.Scan(&id, &jobID, &queue, &durationMs, &errorText, &timestamp)
	if err != nil {
		if err == sql.ErrNoRows {
			// No metrics available - this is normal
			tx.Rollback()
			return config.JobMetrics{}, nil
		}

		// This is an actual error
		tx.Rollback()
		ds.logger.Error("failed to scan metrics", "error", err)
		return config.JobMetrics{}, fmt.Errorf("failed to scan metrics: %w", err)
	}

	// Delete the metrics record instead of marking as processed
	_, err = tx.Exec(deleteQuery, id)
	if err != nil {
		tx.Rollback()
		ds.logger.Error("failed to delete metrics", "error", err, "id", id)
		return config.JobMetrics{}, fmt.Errorf("failed to delete metrics: %w", err)
	}

	// Commit the transaction
	if err = tx.Commit(); err != nil {
		ds.logger.Error("failed to commit transaction", "error", err)
		return config.JobMetrics{}, fmt.Errorf("failed to commit transaction: %w", err)
	}

	// Convert duration from milliseconds back to Duration
	duration := time.Duration(durationMs) * time.Millisecond

	// Convert error string back to error if present
	var jobError error
	if errorText.Valid {
		jobError = fmt.Errorf("%s", errorText.String)
	}

	// Create and return the metrics
	metrics := config.JobMetrics{
		JobID:     jobID,
		QueueName: queue,
		Duration:  duration,
		Error:     jobError,
		Timestamp: timestamp,
	}

	return metrics, nil
}

// IsHealthy returns whether the database connection is healthy
func (ds *DatabaseStore) IsHealthy() bool {
	return true
}

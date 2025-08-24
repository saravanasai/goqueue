package database

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"github.com/saravanasai/goqueue/adapter/database/migrations"
	"github.com/saravanasai/goqueue/adapter/utils"
	"github.com/saravanasai/goqueue/config"
	"github.com/saravanasai/goqueue/internal/logger"
	"github.com/saravanasai/goqueue/internal/registry"
	"github.com/saravanasai/goqueue/job"
)

type DatabaseStore struct {
	db     *sql.DB
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
		db:     db,
		dbType: cfg.DatabaseType,
		logger: log,
		config: config,
	}

	// Run migrations if auto-migrate is enabled
	if cfg.AutoMigrate {
		if err := migrations.RunMigrations(db, cfg.DatabaseType, cfg.MigrationsTable); err != nil {
			db.Close()
			return nil, fmt.Errorf("failed to run migrations: %w", err)
		}
	}

	log.Info("Connected to database", "type", cfg.DatabaseType)
	return store, nil
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
	_, err = ds.db.Exec(query, jobID, queueName, jobName, jobPayload, availableAt)
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
	tx, err := ds.db.Begin()
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
	tx, err := ds.db.Begin()
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
		// For MySQL, need to use a two-step process
		row = tx.QueryRow(query, queueName)
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
	_, err := ds.db.Exec(query, jobID, queueName)
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
func (ds *DatabaseStore) RetryJobWithMetadata(queueName string, job job.JobContext, delay ...time.Duration) error {
	// TODO: Implement real logic
	return nil
}

// EnqueueMetrics adds job metrics to the metrics queue
func (ds *DatabaseStore) EnqueueMetrics(metrics config.JobMetrics) error {
	// TODO: Implement real logic
	return nil
}

// DequeueMetrics retrieves job metrics from the metrics queue
func (ds *DatabaseStore) DequeueMetrics(queueName string) (config.JobMetrics, error) {
	// TODO: Implement real logic
	return config.JobMetrics{}, nil
}

// IsHealthy returns whether the database connection is healthy
func (ds *DatabaseStore) IsHealthy() bool {
	// TODO: Implement real logic
	return true
}

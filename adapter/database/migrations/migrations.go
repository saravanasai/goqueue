package migrations

import (
	"database/sql"
	"fmt"

	"github.com/saravanasai/goqueue/config"
)

// Migration represents a database migration
type Migration struct {
	TableName   string
	Description string
	SQL         map[string]string // Key is database type ("postgres", "mysql")
}

// GetMigrations returns all migrations
func GetMigrations() []Migration {
	return []Migration{
		{
			TableName:   "jobs",
			Description: "Create jobs table",
			SQL: map[string]string{
				"postgres": `CREATE TABLE jobs (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    queue_name TEXT NOT NULL,
                    job_name TEXT NOT NULL,
                    payload JSONB NOT NULL,
                    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    available_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    reserved_at TIMESTAMP NULL,
                    attempts INT NOT NULL DEFAULT 0,
                    max_tries INT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    last_error TEXT,
                    timeout INT NULL,
                    backoff INT NULL
                );
                CREATE INDEX idx_jobs_queue_reserved_avail ON jobs (queue_name, reserved_at, available_at);`,
				"mysql": `CREATE TABLE jobs (
                    id CHAR(36) PRIMARY KEY,
                    queue_name VARCHAR(255) NOT NULL,
                    job_name VARCHAR(255) NOT NULL,
                    payload JSON NOT NULL,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    available_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    reserved_at TIMESTAMP NULL,
                    attempts INT NOT NULL DEFAULT 0,
                    max_tries INT NULL,
                    status VARCHAR(50) NOT NULL DEFAULT 'pending',
                    last_error TEXT,
                    timeout INT NULL,
                    backoff INT NULL,
                    INDEX idx_jobs_queue_reserved_avail (queue_name, reserved_at, available_at)
                )`,
			},
		},
		{
			TableName:   "failed_jobs",
			Description: "Create dead letter queue table",
			SQL: map[string]string{
				"postgres": `CREATE TABLE failed_jobs (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    uuid UUID NOT NULL UNIQUE,
                    queue_name TEXT NOT NULL,
                    job_name TEXT NOT NULL,
                    payload JSONB NOT NULL,
                    exception TEXT NOT NULL,
                    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    failed_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    attempts INT NOT NULL DEFAULT 0,
                    error TEXT
                )`,
				"mysql": `CREATE TABLE failed_jobs (
                    id CHAR(36) PRIMARY KEY,
                    uuid CHAR(36) NOT NULL UNIQUE,
                    queue_name VARCHAR(255) NOT NULL,
                    job_name VARCHAR(255) NOT NULL,
                    payload JSON NOT NULL,
                    exception TEXT NOT NULL,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    failed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    attempts INT NOT NULL DEFAULT 0,
                    error TEXT
                )`,
			},
		},
		{
			TableName:   "job_metrics",
			Description: "Create job metrics table",
			SQL: map[string]string{
				"postgres": `CREATE TABLE job_metrics (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    job_id TEXT NOT NULL,
                    queue_name TEXT NOT NULL,
                    duration BIGINT NOT NULL,
                    error TEXT,
                    timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                    processed BOOLEAN NOT NULL DEFAULT FALSE
                );
                CREATE INDEX idx_job_metrics_processed ON job_metrics (processed);`,
				"mysql": `CREATE TABLE job_metrics (
                    id CHAR(36) PRIMARY KEY,
                    job_id VARCHAR(255) NOT NULL,
                    queue_name VARCHAR(255) NOT NULL,
                    duration BIGINT NOT NULL,
                    error TEXT,
                    timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    processed BOOLEAN NOT NULL DEFAULT FALSE,
                    INDEX idx_job_metrics_processed (processed)
                )`,
			},
		},
	}
}

func RunMigrations(db *sql.DB, dbType string) error {
	migrations := GetMigrations()

	for _, migration := range migrations {
		var tableExists bool
		var tableName string = migration.TableName

		// Check if table exists based on database type
		var checkTableSQL string
		switch dbType {
		case config.DatabaseTypePostgres:
			checkTableSQL = `SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = $1
            )`
		case config.DatabaseTypeMySQL:
			checkTableSQL = `SELECT EXISTS (
                SELECT * FROM information_schema.tables 
                WHERE table_schema = DATABASE()
                AND table_name = ?
            )`
		default:
			return fmt.Errorf("unsupported database type: %s", dbType)
		}

		// Check if table exists
		if err := db.QueryRow(checkTableSQL, tableName).Scan(&tableExists); err != nil {
			return fmt.Errorf("failed to check if table %s exists: %w", tableName, err)
		}

		// If table doesn't exist, create it
		if !tableExists {
			sql, ok := migration.SQL[dbType]
			if !ok {
				return fmt.Errorf("migration for table %s does not support database type: %s",
					tableName, dbType)
			}

			if _, err := db.Exec(sql); err != nil {
				return fmt.Errorf("failed to create table %s: %w", tableName, err)
			}
			fmt.Printf("Created table %s\n", tableName)
		} else {
			fmt.Printf("Table %s already exists, skipping\n", tableName)
		}
	}

	return nil
}

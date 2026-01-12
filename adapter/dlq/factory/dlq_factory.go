package factory

import (
	"database/sql"

	"github.com/danish-a1/goqueue/adapter/dlq"
	"github.com/danish-a1/goqueue/config"
	"github.com/danish-a1/goqueue/internal/logger"
	"github.com/redis/go-redis/v9"
)

// NewDatabaseDLQ creates a new database-backed DLQ based on the database type
func NewDatabaseDLQ(db *sql.DB, dbType string, logger logger.Logger) dlq.DLQAdapter {
	switch dbType {
	case config.DatabaseTypePostgres:
		return dlq.NewPostgresDLQ(db, logger)
	case config.DatabaseTypeMySQL:
		return dlq.NewMySQLDLQ(db, logger)
	default:
		// Default to Postgres if unknown
		return dlq.NewPostgresDLQ(db, logger)
	}
}

func NewRedisDLQ(client *redis.Client, logger logger.Logger) dlq.DLQAdapter {
	return dlq.NewRedisDLQ(client, logger)
}

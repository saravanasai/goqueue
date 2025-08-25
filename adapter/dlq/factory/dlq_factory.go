package factory

import (
	"database/sql"

	"github.com/redis/go-redis/v9"
	"github.com/saravanasai/goqueue/adapter/dlq"
	"github.com/saravanasai/goqueue/config"
	"github.com/saravanasai/goqueue/internal/logger"
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

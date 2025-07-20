package config

import (
	"errors"
	"time"
)

const (
	DriverMemory   = "memory"
	DriverRedis    = "redis"
	DriverDatabase = "database"
)

type Config struct {
	Driver          string        // "memory", "redis", "database"
	QueueName       string        // default queue name
	RetryCount      int           // retries on failure
	RetryDelay      time.Duration // delay between retries
	ShutdownTimeout time.Duration // max time to wait during graceful shutdown
	DriverConfig    any           // e.g. *RedisConfig, *SQLiteConfig
}

type InMemoryConfig struct{}

func NewInMemoryConfig() *InMemoryConfig {

	return &InMemoryConfig{}
}

func (c Config) Validate() error {
	switch c.Driver {
	case DriverMemory:
		return nil
	default:
		return errors.New("unsupported driver: " + c.Driver)
	}
}

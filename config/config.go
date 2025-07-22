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

type DriverConfig interface {
	Type() string
}

type Config struct {
	Driver          string        // "memory", "redis", "database"
	QueueName       string        // default queue name
	RetryCount      int           // retries on failure
	RetryDelay      time.Duration // delay between retries
	ShutdownTimeout time.Duration // max time to wait during graceful shutdown
	DriverConfig    DriverConfig  // e.g. *RedisConfig, *SQLiteConfig
	NumWorkers      int
}

type RedisConfig struct {
	Addr     string
	Password string
	Db       int
}

func (r RedisConfig) Type() string {
	return "redis"
}

func NewInMemoryConfig() Config {
	return Config{
		Driver:          DriverMemory,
		QueueName:       "default",
		RetryCount:      3,
		RetryDelay:      time.Second,
		ShutdownTimeout: 5 * time.Second,
		DriverConfig:    nil,
		NumWorkers:      2,
	}
}

func NewRedisConfig(address string, password string, db int) Config {
	return Config{
		Driver:          DriverRedis,
		QueueName:       "default",
		RetryCount:      3,
		RetryDelay:      time.Second,
		ShutdownTimeout: 5 * time.Second,
		DriverConfig:    RedisConfig{Addr: address, Password: password, Db: db},
		NumWorkers:      2,
	}
}

func (c Config) Validate() error {
	switch c.Driver {
	case DriverMemory:
		return nil
	case DriverRedis:
		return nil
	default:
		return errors.New("unsupported driver: " + c.Driver)
	}
}

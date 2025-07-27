package config

import (
	"errors"
	"runtime"
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

type JobMetrics struct {
	QueueName string
	JobID     string
	Duration  time.Duration
	Error     error
	Timestamp time.Time
}

// Clean callback signature with struct
type MetricsCallback func(metrics JobMetrics)

type Config struct {
	Driver           string
	DriverConfig     DriverConfig
	MaxWorkers       int
	ConcurrencyLimit int
	OnJobComplete    MetricsCallback
}

type RedisConfig struct {
	Addr     string
	Password string
	Db       int
}

func (r RedisConfig) Type() string {
	return "redis"
}

func sensibleDefaultMaxWorkers() int {

	return runtime.NumCPU() * 2
}

func sensibleDefaultConcurrencyLimit() int {
	return runtime.NumCPU() * 4
}

func NewInMemoryConfig() Config {
	return Config{
		Driver:           DriverMemory,
		DriverConfig:     nil,
		MaxWorkers:       sensibleDefaultMaxWorkers(),
		ConcurrencyLimit: sensibleDefaultConcurrencyLimit(),
		OnJobComplete:    nil,
	}
}

func NewRedisConfig(address string, password string, db int) Config {
	return Config{
		Driver: DriverRedis,
		DriverConfig: RedisConfig{
			Addr:     address,
			Password: password,
			Db:       db,
		},
		MaxWorkers:       sensibleDefaultMaxWorkers(),
		ConcurrencyLimit: sensibleDefaultConcurrencyLimit(),
		OnJobComplete:    nil,
	}
}

func (c Config) WithMaxWorkers(maxWorkers int) Config {
	c.MaxWorkers = maxWorkers
	return c
}

func (c Config) WithConcurrencyLimit(limit int) Config {
	c.ConcurrencyLimit = limit
	return c
}

func (c Config) WithMetricsCallback(callback MetricsCallback) Config {
	c.OnJobComplete = callback
	return c
}

func (c Config) Validate() error {

	if c.MaxWorkers <= 0 {
		return errors.New("MaxWorkers must be greater than 0")
	}

	if c.ConcurrencyLimit <= 0 {
		return errors.New("ConcurrencyLimit must be greater than 0")
	}

	switch c.Driver {
	case DriverMemory:
		return nil
	case DriverRedis:
		return nil
	default:
		return errors.New("unsupported driver: " + c.Driver)
	}
}

// Package config provides configuration types and options for setting up and customizing
// queue behavior, including driver selection, worker limits, retry policies, and middleware.
package config

import (
	"errors"
	"runtime"
	"time"

	"github.com/saravanasai/goqueue/adapter/dlq"
	"github.com/saravanasai/goqueue/internal/logger"
	"github.com/saravanasai/goqueue/middleware"
)

// Driver type constants for supported queue backends
const (
	// DriverMemory uses in-memory storage, suitable for development and testing
	DriverMemory = "memory"
	// DriverRedis uses Redis as the backend storage
	DriverRedis = "redis"
	// DriverSQS uses AWS SQS as the backend storage
	DriverSQS = "sqs"
	// DriverDatabase is reserved for future database backend support
	DriverDatabase = "database"
)

// DriverConfig defines the interface that all driver-specific configurations must implement.
type DriverConfig interface {
	// Type returns the driver type identifier (e.g., "redis", "memory")
	Type() string
}

// JobMetrics contains metrics data for a completed job.
// This is passed to the MetricsCallback when a job completes processing.
type JobMetrics struct {
	// QueueName is the name of the queue the job was processed in
	QueueName string
	// JobID is the unique identifier of the processed job
	JobID string
	// Duration is how long the job took to process
	Duration time.Duration
	// Error contains any error that occurred during processing
	Error error
	// Timestamp is when the metrics were collected
	Timestamp time.Time
}

// MetricsCallback is a function type for handling job completion metrics.
// It is called after a job completes processing, whether successful or not.
type MetricsCallback func(metrics JobMetrics)

// Config holds all configuration options for a queue instance.
// Use the With* methods to set specific options in a fluent manner.
type Config struct {
	// Driver specifies the queue backend ("memory" or "redis")
	Driver string
	// DriverConfig contains driver-specific configuration
	DriverConfig DriverConfig
	// MaxWorkers is the maximum number of concurrent worker goroutines
	MaxWorkers int
	// ConcurrencyLimit is the maximum number of jobs that can be processed simultaneously
	ConcurrencyLimit int
	// OnJobComplete is called when a job finishes processing
	OnJobComplete MetricsCallback
	// StatsEnabled enables collection of queue statistics
	StatsEnabled bool
	// MaxRetryAttempts is the number of times to retry a failed job
	MaxRetryAttempts int
	// RetryDelay is the base delay between retry attempts
	RetryDelay time.Duration
	// ExponentialBackoff enables exponential increase of retry delays
	ExponentialBackoff bool
	// DLQAdapter handles failed jobs after max retries
	DLQAdapter dlq.DLQAdapter
	// Middlewares is the chain of job processing middleware
	Middlewares []middleware.Middleware
	// JobTimeout is the default timeout for job execution (can be overridden per job)
	JobTimeout time.Duration
}

// RedisConfig contains configuration options specific to the Redis driver.
type RedisConfig struct {
	// Addr is the Redis server address (e.g., "localhost:6379")
	Addr string
	// Password is the Redis server password (optional)
	Password string
	// Db is the Redis database number to use
	Db int
}

// Type implements the DriverConfig interface.
func (r RedisConfig) Type() string {
	return "redis"
}

// SQSConfig contains configuration options specific to the AWS SQS driver.
type SQSConfig struct {
	// QueueURL is the URL of the SQS queue
	QueueURL string
	// Region is the AWS region where the SQS queue is located
	Region string
	// AccessKeyID is the AWS access key ID for authentication
	AccessKeyID string
	// SecretAccessKey is the AWS secret access key for authentication
	SecretAccessKey string
	// MaxMessages is the maximum number of messages to retrieve in a single batch (1-10)
	MaxMessages int
	// VisibilityTimeout is the duration that messages are hidden from subsequent retrieve requests
	VisibilityTimeout time.Duration
	// IsFifo indicates whether the SQS queue is a FIFO queue
	IsFifo bool
	// MessageGroupID is the FIFO queue message group ID (required for FIFO queues)
	// If not specified, a default value of "default" will be used for FIFO queues
	MessageGroupID string
	// MessageDeduplicationID is used for FIFO queues to prevent duplicate messages (optional)
	// If not specified, a unique ID will be generated automatically for each message
	MessageDeduplicationID string
}

// Type implements the DriverConfig interface.
func (s SQSConfig) Type() string {
	return "sqs"
}

// sensibleDefaultMaxWorkers returns a reasonable default for MaxWorkers
// based on the number of CPU cores.
func sensibleDefaultMaxWorkers() int {
	return runtime.NumCPU() * 2
}

// sensibleDefaultConcurrencyLimit returns a reasonable default for ConcurrencyLimit
// based on the number of CPU cores.
func sensibleDefaultConcurrencyLimit() int {
	return runtime.NumCPU() * 4
}

// NewInMemoryConfig creates a new Config instance with in-memory driver and sensible defaults.
// This is suitable for development and testing environments.
func NewInMemoryConfig() Config {
	return Config{
		Driver:             DriverMemory,
		DriverConfig:       nil,
		MaxWorkers:         sensibleDefaultMaxWorkers(),
		ConcurrencyLimit:   sensibleDefaultConcurrencyLimit(),
		OnJobComplete:      nil,
		MaxRetryAttempts:   3,
		RetryDelay:         2 * time.Second,
		ExponentialBackoff: false,
	}
}

// NewRedisConfig creates a new Config instance with Redis driver and sensible defaults.
// This is suitable for production environments.
func NewRedisConfig(address string, password string, db int) Config {
	return Config{
		Driver: DriverRedis,
		DriverConfig: RedisConfig{
			Addr:     address,
			Password: password,
			Db:       db,
		},
		MaxWorkers:         sensibleDefaultMaxWorkers(),
		ConcurrencyLimit:   sensibleDefaultConcurrencyLimit(),
		OnJobComplete:      nil,
		MaxRetryAttempts:   3,
		RetryDelay:         2 * time.Second,
		ExponentialBackoff: false,
	}
}

// NewSQSConfig creates a new Config instance with AWS SQS driver and sensible defaults.
// This is suitable for production environments with AWS SQS standard queues.
func NewSQSConfig(queueURL, region, accessKeyID, secretAccessKey string) Config {
	return Config{
		Driver: DriverSQS,
		DriverConfig: SQSConfig{
			QueueURL:          queueURL,
			Region:            region,
			AccessKeyID:       accessKeyID,
			SecretAccessKey:   secretAccessKey,
			MaxMessages:       1,                // Default to 1 message at a time
			VisibilityTimeout: 30 * time.Second, // Default visibility timeout
			IsFifo:            false,
		},
		MaxWorkers:         sensibleDefaultMaxWorkers(),
		ConcurrencyLimit:   sensibleDefaultConcurrencyLimit(),
		OnJobComplete:      nil,
		MaxRetryAttempts:   3,
		RetryDelay:         2 * time.Second,
		ExponentialBackoff: false,
	}
}

// NewSQSFifoConfig creates a new Config instance with AWS SQS FIFO queue driver.
// This is suitable for production environments requiring exactly-once processing
// and message ordering guarantees.
//
// The queueURL must be for a FIFO queue, which should end with .fifo
// messageGroupID is required for FIFO queues and defines which group of messages should be processed in order.
// If messageGroupID is empty, "default" will be used.
func NewSQSFifoConfig(queueURL, region, accessKeyID, secretAccessKey, messageGroupID string) Config {
	// If messageGroupID is not provided, use a default value
	if messageGroupID == "" {
		messageGroupID = "default"
	}

	return Config{
		Driver: DriverSQS,
		DriverConfig: SQSConfig{
			QueueURL:          queueURL,
			Region:            region,
			AccessKeyID:       accessKeyID,
			SecretAccessKey:   secretAccessKey,
			MaxMessages:       1,                // Default to 1 message at a time
			VisibilityTimeout: 30 * time.Second, // Default visibility timeout
			IsFifo:            true,
			MessageGroupID:    messageGroupID,
		},
		MaxWorkers:         sensibleDefaultMaxWorkers(),
		ConcurrencyLimit:   sensibleDefaultConcurrencyLimit(),
		OnJobComplete:      nil,
		MaxRetryAttempts:   3,
		RetryDelay:         2 * time.Second,
		ExponentialBackoff: false,
	}
}

// WithStats enables or disables queue statistics collection.
func (c Config) WithStats(enabled bool) Config {
	c.StatsEnabled = enabled
	return c
}

// WithMaxWorkers sets the maximum number of concurrent worker goroutines.
func (c Config) WithMaxWorkers(maxWorkers int) Config {
	c.MaxWorkers = maxWorkers
	return c
}

// WithConcurrencyLimit sets the maximum number of jobs that can be processed simultaneously.
func (c Config) WithConcurrencyLimit(limit int) Config {
	c.ConcurrencyLimit = limit
	return c
}

// WithMetricsCallback sets the callback function for job completion metrics.
func (c Config) WithMetricsCallback(callback MetricsCallback) Config {
	c.OnJobComplete = callback
	return c
}

// WithMaxRetryAttempts sets the number of times to retry a failed job.
func (c Config) WithMaxRetryAttempts(attempts int) Config {
	c.MaxRetryAttempts = attempts
	return c
}

// WithRetryDelay sets the base delay between retry attempts.
func (c Config) WithRetryDelay(delay time.Duration) Config {
	c.RetryDelay = delay
	return c
}

// WithExponentialBackoff enables or disables exponential increase of retry delays.
func (c Config) WithExponentialBackoff(enabled bool) Config {
	c.ExponentialBackoff = enabled
	return c
}

// WithDLQAdapter sets the Dead Letter Queue adapter for handling failed jobs.
func (c Config) WithDLQAdapter(adapter dlq.DLQAdapter) Config {
	c.DLQAdapter = adapter
	return c
}

// WithMiddleware adds a middleware to the processing chain.
// Middlewares are executed in the order they are added.
func (c Config) WithMiddleware(m middleware.Middleware) Config {
	c.Middlewares = append(c.Middlewares, m)
	return c
}

// WithMiddlewares adds multiple middlewares to the processing chain.
// Middlewares are executed in the order they are added.
func (c Config) WithMiddlewares(middlewares ...middleware.Middleware) Config {
	c.Middlewares = append(c.Middlewares, middlewares...)
	return c
}

// WithJobTimeout sets the default timeout for job execution.
func (c Config) WithJobTimeout(timeout time.Duration) Config {
	c.JobTimeout = timeout
	return c
}

// Validate checks if the configuration is valid.
// It returns an error if any required fields are missing or invalid.
func (c Config) Validate(logger logger.Logger) error {
	if c.MaxWorkers <= 0 {
		logger.Error("MaxWorkers must be greater than 0", "MaxWorkers", c.MaxWorkers)
		return errors.New("MaxWorkers must be greater than 0")
	}

	if c.ConcurrencyLimit <= 0 {
		logger.Error("ConcurrencyLimit must be greater than 0", "ConcurrencyLimit", c.ConcurrencyLimit)
		return errors.New("ConcurrencyLimit must be greater than 0")
	}

	switch c.Driver {
	case DriverMemory:
		return nil
	case DriverRedis:
		return nil
	case DriverSQS:
		return nil
	default:
		logger.Error("unsupported driver", "Driver", c.Driver)
		return errors.New("unsupported driver: " + c.Driver)
	}
}

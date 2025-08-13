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
//
// The in-memory driver stores all jobs in memory and does not provide persistence
// across application restarts. Jobs will be lost if the application terminates.
//
// Returns:
//   - A Config instance configured with the in-memory driver
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
//
// The Redis driver provides persistence and can be used in distributed environments
// where multiple application instances need to share the same job queue.
//
// Parameters:
//   - address: Redis server address (e.g., "localhost:6379")
//   - password: Redis server password, or empty string if no password
//   - db: Redis database number to use
//
// Returns:
//   - A Config instance configured with the Redis driver
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
//
// The SQS driver provides fully managed message queuing with high availability and durability.
// It's appropriate for cloud-based applications that need reliable message processing.
//
// Parameters:
//   - queueURL: The URL of the SQS queue (from AWS console or API)
//   - region: AWS region where the queue is located (e.g., "us-west-2")
//   - accessKeyID: AWS access key ID for authentication, or empty to use environment/instance profile
//   - secretAccessKey: AWS secret access key for authentication, or empty to use environment/instance profile
//
// Returns:
//   - A Config instance configured with the SQS driver for standard queues
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
// FIFO (First-In-First-Out) queues provide additional guarantees compared to standard SQS queues:
// - Messages are processed in the exact order they are sent
// - Messages are delivered exactly once with no duplicates
//
// Parameters:
//   - queueURL: The URL of the SQS FIFO queue (must end with .fifo)
//   - region: AWS region where the queue is located (e.g., "us-west-2")
//   - accessKeyID: AWS access key ID for authentication, or empty to use environment/instance profile
//   - secretAccessKey: AWS secret access key for authentication, or empty to use environment/instance profile
//   - messageGroupID: FIFO queue message group ID (required), defines which messages are processed in order
//
// Returns:
//   - A Config instance configured with the SQS driver for FIFO queues
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
//
// When enabled, the queue will collect metrics about job processing rates,
// queue sizes, and health indicators. This is useful for monitoring
// and debugging, but has a small performance cost.
//
// Parameters:
//   - enabled: Whether to enable statistics collection
//
// Returns:
//   - Updated Config with statistics collection setting
func (c Config) WithStats(enabled bool) Config {
	c.StatsEnabled = enabled
	return c
}

// WithMaxWorkers sets the maximum number of concurrent worker goroutines.
//
// This controls how many worker goroutines will be spawned to process jobs.
// Each worker can process one job at a time. The optimal number depends on
// your workload and available resources.
//
// Parameters:
//   - maxWorkers: Maximum number of worker goroutines to spawn
//
// Returns:
//   - Updated Config with the max workers setting
func (c Config) WithMaxWorkers(maxWorkers int) Config {
	c.MaxWorkers = maxWorkers
	return c
}

// WithConcurrencyLimit sets the maximum number of jobs that can be processed simultaneously.
//
// This controls the total number of jobs that can be in-process at once,
// which may be different from the number of workers. This is useful for
// rate limiting and preventing system overload.
//
// Parameters:
//   - limit: Maximum number of concurrent jobs
//
// Returns:
//   - Updated Config with the concurrency limit setting
func (c Config) WithConcurrencyLimit(limit int) Config {
	c.ConcurrencyLimit = limit
	return c
}

// WithMetricsCallback sets the callback function for job completion metrics.
//
// This function will be called after every job completes, with metrics
// including job ID, duration, error status, and timestamps. This is useful
// for monitoring, alerting, and performance tracking.
//
// Parameters:
//   - callback: Function to call with job metrics on completion
//
// Returns:
//   - Updated Config with the metrics callback set
func (c Config) WithMetricsCallback(callback MetricsCallback) Config {
	c.OnJobComplete = callback
	return c
}

// WithMaxRetryAttempts sets the number of times to retry a failed job.
//
// When a job fails with an error, it will be retried up to this many times
// before being considered permanently failed and potentially sent to the DLQ.
// Set to 0 to disable retries.
//
// Parameters:
//   - attempts: Maximum number of retry attempts
//
// Returns:
//   - Updated Config with the max retry attempts setting
func (c Config) WithMaxRetryAttempts(attempts int) Config {
	c.MaxRetryAttempts = attempts
	return c
}

// WithRetryDelay sets the base delay between retry attempts.
//
// This is the initial delay between retry attempts. If exponential backoff
// is enabled, this delay will increase with each retry attempt.
//
// Parameters:
//   - delay: Base time to wait between retry attempts
//
// Returns:
//   - Updated Config with the retry delay setting
func (c Config) WithRetryDelay(delay time.Duration) Config {
	c.RetryDelay = delay
	return c
}

// WithExponentialBackoff enables or disables exponential increase of retry delays.
//
// When enabled, the delay between retry attempts will increase exponentially
// based on the retry count. This helps to prevent overwhelming the system with
// retry attempts when there are persistent issues.
//
// Parameters:
//   - enabled: Whether to use exponential backoff for retries
//
// Returns:
//   - Updated Config with the exponential backoff setting
func (c Config) WithExponentialBackoff(enabled bool) Config {
	c.ExponentialBackoff = enabled
	return c
}

// WithDLQAdapter sets the Dead Letter Queue adapter for handling failed jobs.
//
// A Dead Letter Queue (DLQ) is used to store jobs that have failed after exceeding
// their retry attempts. This allows for later analysis, debugging, or manual reprocessing.
//
// Parameters:
//   - adapter: An implementation of the dlq.DLQAdapter interface
//
// Returns:
//   - Updated Config with the DLQ adapter set
func (c Config) WithDLQAdapter(adapter dlq.DLQAdapter) Config {
	c.DLQAdapter = adapter
	return c
}

// WithMiddleware adds a middleware to the processing chain.
// Middlewares are executed in the order they are added.
//
// Middleware allows you to add cross-cutting functionality to job processing,
// such as logging, metrics collection, validation, or rate limiting.
//
// Parameters:
//   - m: The middleware function to add
//
// Returns:
//   - Updated Config with the middleware added
func (c Config) WithMiddleware(m middleware.Middleware) Config {
	c.Middlewares = append(c.Middlewares, m)
	return c
}

// WithMiddlewares adds multiple middlewares to the processing chain.
// Middlewares are executed in the order they are added.
//
// This is a convenience method for adding multiple middlewares at once.
//
// Parameters:
//   - middlewares: One or more middleware functions to add
//
// Returns:
//   - Updated Config with all the middlewares added
func (c Config) WithMiddlewares(middlewares ...middleware.Middleware) Config {
	c.Middlewares = append(c.Middlewares, middlewares...)
	return c
}

// WithJobTimeout sets the default timeout for job execution.
//
// This sets the maximum duration that a job can run before it's considered timed out.
// Individual jobs can override this timeout by setting their own timeout in the job context.
//
// Parameters:
//   - timeout: The maximum duration for job execution
//
// Returns:
//   - Updated Config with the job timeout set
func (c Config) WithJobTimeout(timeout time.Duration) Config {
	c.JobTimeout = timeout
	return c
}

// Validate checks if the configuration is valid.
// It returns an error if any required fields are missing or invalid.
//
// This method verifies that the configuration has valid worker counts,
// concurrency limits, and a supported driver type. It logs any validation
// errors found.
//
// Parameters:
//   - logger: Logger to record validation errors
//
// Returns:
//   - nil if the configuration is valid
//   - an error describing the validation failure
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

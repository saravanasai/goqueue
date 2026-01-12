package sqs

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/danish-a1/goqueue/adapter/utils"
	jobConfig "github.com/danish-a1/goqueue/config"
	"github.com/danish-a1/goqueue/internal/logger"
	"github.com/danish-a1/goqueue/internal/registry"
	"github.com/danish-a1/goqueue/job"
)

const (
	// SQS attribute names for metadata
	attrJobID         = "JobID"
	attrQueueName     = "QueueName"
	attrJobName       = "JobName"
	attrEnqueuedAt    = "EnqueuedAt"
	attrRetryCount    = "RetryCount"
	metricQueueSuffix = "-metrics"
)

// SQSQueuedJob represents a job queued in SQS
type SQSQueuedJob struct {
	// Job contains the serialized job data
	Job json.RawMessage `json:"job"`
	// ID is a unique identifier for this queued job
	ID string `json:"id"`
	// JobName is the registered name of the job type, used for deserialization
	JobName string `json:"job_name"`
	// EnqueuedAt is the timestamp when the job was added to the queue
	EnqueuedAt time.Time `json:"enqueued_at"`
	// RetryCount tracks how many times this job has been retried
	RetryCount int `json:"retry_count"`
	// ReceiptHandle is the SQS receipt handle for acknowledgment
	ReceiptHandle string `json:"receipt_handle,omitempty"`
}

// SQSClient defines the interface for SQS client operations we use
// This allows us to easily mock the SQS client for testing
type SQSClient interface {
	SendMessage(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
	ReceiveMessage(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessage(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
	SendMessageBatch(ctx context.Context, params *sqs.SendMessageBatchInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageBatchOutput, error)
	GetQueueAttributes(ctx context.Context, params *sqs.GetQueueAttributesInput, optFns ...func(*sqs.Options)) (*sqs.GetQueueAttributesOutput, error)
	ChangeMessageVisibility(ctx context.Context, params *sqs.ChangeMessageVisibilityInput, optFns ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error)
}

// SQSStore is an implementation of the Store interface using AWS SQS
type SQSStore struct {
	// client is the AWS SQS client
	client SQSClient
	// config contains the queue configuration
	config jobConfig.Config
	// sqsConfig contains SQS-specific configuration
	sqsConfig jobConfig.SQSConfig
	// logger for errors and debugging
	logger logger.Logger
	// healthStatus tracks connection health
	healthStatus bool
	// queueURLs maps queue names to their SQS URLs
	queueURLs map[string]string
	// queueURLsMutex protects queueURLs map from concurrent access
	queueURLsMutex sync.RWMutex
	// jobReceiptHandles maps jobID to receiptHandle for Ack
	jobReceiptHandles map[string]string
}

// NewSQSStore creates a new SQS-backed store implementation
func NewSQSStore(cfg jobConfig.Config, logger logger.Logger) (*SQSStore, error) {
	sqsCfg, ok := cfg.DriverConfig.(jobConfig.SQSConfig)
	if !ok {
		return nil, fmt.Errorf("invalid SQS config provided")
	}

	// Set default values if not provided
	if sqsCfg.MaxMessages <= 0 {
		sqsCfg.MaxMessages = 1
	} else if sqsCfg.MaxMessages > 10 {
		sqsCfg.MaxMessages = 10 // SQS maximum is 10
	}

	if sqsCfg.VisibilityTimeout <= 0 {
		sqsCfg.VisibilityTimeout = 30 * time.Second
	}

	// Initialize AWS configuration
	var awsConfig aws.Config
	var err error

	// If credentials are provided, use them; otherwise, fall back to AWS SDK's default credential chain
	if sqsCfg.AccessKeyID != "" && sqsCfg.SecretAccessKey != "" {
		awsConfig, err = config.LoadDefaultConfig(context.Background(),
			config.WithRegion(sqsCfg.Region),
			config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
				sqsCfg.AccessKeyID, sqsCfg.SecretAccessKey, "",
			)),
		)
	} else {
		awsConfig, err = config.LoadDefaultConfig(context.Background(),
			config.WithRegion(sqsCfg.Region),
		)
	}

	if err != nil {
		logger.Error("Failed to initialize AWS config", "error", err)
		return nil, fmt.Errorf("failed to initialize AWS config: %w", err)
	}

	// Create SQS client
	client := sqs.NewFromConfig(awsConfig)

	// Validate queue URL by trying to get queue attributes
	_, err = client.GetQueueAttributes(context.Background(), &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(sqsCfg.QueueURL),
	})

	if err != nil {
		logger.Error("Failed to validate SQS queue", "error", err, "queueURL", sqsCfg.QueueURL)
		return nil, fmt.Errorf("failed to validate SQS queue: %w", err)
	}

	store := &SQSStore{
		client:            client,
		config:            cfg,
		sqsConfig:         sqsCfg,
		logger:            logger,
		healthStatus:      true, // Start with healthy status
		queueURLs:         make(map[string]string),
		jobReceiptHandles: make(map[string]string),
	}

	// Store the main queue URL
	store.queueURLs[""] = sqsCfg.QueueURL

	return store, nil
}

// NewSQSStoreWithClient creates a new SQS store with a custom SQS client
// This is primarily used for testing with mock clients
func NewSQSStoreWithClient(client SQSClient, sqsCfg jobConfig.SQSConfig, cfg jobConfig.Config, logger logger.Logger) *SQSStore {
	store := &SQSStore{
		client:            client,
		config:            cfg,
		sqsConfig:         sqsCfg,
		logger:            logger,
		healthStatus:      true,
		queueURLs:         make(map[string]string),
		jobReceiptHandles: make(map[string]string),
	}

	// Store the main queue URL
	store.queueURLs[""] = sqsCfg.QueueURL

	return store
}

// createSQSQueuedJob creates an SQSQueuedJob from a JobContext for retry purposes
func (s *SQSStore) createSQSQueuedJob(jobCtx job.JobContext, retryCount int) (SQSQueuedJob, error) {
	// Marshal the actual job
	jobPayload, err := json.Marshal(jobCtx.Job)
	if err != nil {
		return SQSQueuedJob{}, fmt.Errorf("failed to marshal job: %w", err)
	}

	// Get job name using utils
	jobName := utils.GetJobName(jobCtx.Job)
	if jobName == "" {
		return SQSQueuedJob{}, fmt.Errorf("could not determine job name from type")
	}

	// Get receipt handle from mapping
	receiptHandle, exists := s.jobReceiptHandles[jobCtx.JobID]
	if !exists {
		return SQSQueuedJob{}, fmt.Errorf("receipt handle not found for job ID: %s", jobCtx.JobID)
	}

	return SQSQueuedJob{
		Job:           jobPayload,
		JobName:       jobName,
		ID:            jobCtx.JobID,
		EnqueuedAt:    jobCtx.EnqueuedAt,
		RetryCount:    retryCount,
		ReceiptHandle: receiptHandle,
	}, nil
}

func (s *SQSStore) GetDbConnection() interface{} {
	return nil
}

// Push adds a single job to the queue, with optional delay
func (s *SQSStore) Push(queueName string, jb job.Job, delay ...time.Duration) error {
	// Get job name from type
	jobName := utils.GetJobName(jb)
	if jobName == "" {
		return fmt.Errorf("could not determine job name from type")
	}

	// Marshal the job data
	jobData, err := json.Marshal(jb)
	if err != nil {
		s.logger.Error("Failed to marshal job", "error", err, "queue", queueName)
		return fmt.Errorf("failed to marshal job: %w", err)
	}

	// Check if payload exceeds SQS message size limit (256KB)
	if len(jobData) > 256*1024 {
		s.logger.Error("Job payload exceeds SQS message size limit (256KB)", "size", len(jobData), "queue", queueName)
		return fmt.Errorf("job payload size (%d bytes) exceeds AWS SQS limit of 256KB", len(jobData))
	}

	// Create a job ID
	jobID := utils.GenerateID()

	// Create the SQS message
	sqsJob := SQSQueuedJob{
		Job:        jobData,
		ID:         jobID,
		JobName:    jobName,
		EnqueuedAt: time.Now(),
		RetryCount: 0,
	}

	messageBody, err := json.Marshal(sqsJob)
	if err != nil {
		s.logger.Error("Failed to marshal SQS job", "error", err, "queue", queueName)
		return fmt.Errorf("failed to marshal SQS job: %w", err)
	}

	// Prepare message attributes
	messageAttributes := map[string]types.MessageAttributeValue{
		attrJobID: {
			DataType:    aws.String("String"),
			StringValue: aws.String(jobID),
		},
		attrQueueName: {
			DataType:    aws.String("String"),
			StringValue: aws.String(queueName),
		},
		attrJobName: {
			DataType:    aws.String("String"),
			StringValue: aws.String(jobName),
		},
		attrEnqueuedAt: {
			DataType:    aws.String("String"),
			StringValue: aws.String(fmt.Sprintf("%d", time.Now().UnixNano())),
		},
		attrRetryCount: {
			DataType:    aws.String("Number"),
			StringValue: aws.String("0"),
		},
	}

	// Create the SendMessageInput
	sendInput := &sqs.SendMessageInput{
		QueueUrl:          aws.String(s.getQueueURL(queueName)),
		MessageBody:       aws.String(string(messageBody)),
		MessageAttributes: messageAttributes,
	}

	// Handle delay parameter (SQS supports up to 15 minutes)
	sendInput.DelaySeconds = calculateDelaySeconds(delay...)

	// Add FIFO queue specific attributes if the queue is FIFO
	sqsCfg, ok := s.config.DriverConfig.(jobConfig.SQSConfig)
	if ok && sqsCfg.IsFifo {
		// For FIFO queues, MessageGroupId is required
		messageGroupID := sqsCfg.MessageGroupID
		if messageGroupID == "" {
			messageGroupID = "default" // Default group if not specified
		}
		sendInput.MessageGroupId = aws.String(messageGroupID)

		// MessageDeduplicationId is optional, but recommended
		// If not provided, generate one based on the job ID
		messageDeduplicationID := sqsCfg.MessageDeduplicationID
		if messageDeduplicationID == "" {
			// Use job ID as deduplication ID if not specified
			messageDeduplicationID = jobID
		}
		sendInput.MessageDeduplicationId = aws.String(messageDeduplicationID)
	}

	// Send the message to SQS
	_, err = s.client.SendMessage(context.Background(), sendInput)

	if err != nil {
		s.healthStatus = false
		s.logger.Error("Failed to send message to SQS", "error", err, "queue", queueName)
		return fmt.Errorf("failed to send message to SQS: %w", err)
	}

	return nil
}

// PushBatch adds multiple jobs to the queue in a single call, with optional delay
func (s *SQSStore) PushBatch(queueName string, jobs []job.Job, delay ...time.Duration) error {
	// SQS SendMessageBatch can only handle up to 10 messages at a time
	batchSize := 10
	for i := 0; i < len(jobs); i += batchSize {
		end := i + batchSize
		if end > len(jobs) {
			end = len(jobs)
		}

		batch := jobs[i:end]
		if err := s.pushBatch(queueName, batch, delay...); err != nil {
			return err
		}
	}
	return nil
}

// pushBatch handles pushing a batch of up to 10 jobs to SQS, with optional delay
func (s *SQSStore) pushBatch(queueName string, jobs []job.Job, delay ...time.Duration) error {
	var entries []types.SendMessageBatchRequestEntry

	// Get SQS configuration for FIFO settings
	sqsCfg, isSQSConfig := s.config.DriverConfig.(jobConfig.SQSConfig)
	isFifo := isSQSConfig && sqsCfg.IsFifo

	// Calculate delay seconds (SQS supports up to 15 minutes)
	delaySeconds := calculateDelaySeconds(delay...)

	for i, jb := range jobs {
		entry, ok, err := s.buildBatchEntry(jb, i, queueName, isFifo, sqsCfg)
		if err != nil {
			s.logger.Error("failed to build batch entry", "error", err, "queue", queueName)
			return err
		}
		if !ok {
			// skipped (no job name or marshal error) continue to next
			continue
		}
		// Set DelaySeconds for each entry if delay is specified
		if delaySeconds > 0 {
			entry.DelaySeconds = delaySeconds
		}
		entries = append(entries, entry)
	}

	if len(entries) == 0 {
		return nil
	}

	// Send the batch to SQS
	resp, err := s.client.SendMessageBatch(context.Background(), &sqs.SendMessageBatchInput{
		QueueUrl: aws.String(s.getQueueURL(queueName)),
		Entries:  entries,
	})

	if err != nil {
		s.healthStatus = false
		s.logger.Error("Failed to send batch messages to SQS", "error", err, "queue", queueName)
		return fmt.Errorf("failed to send batch messages to SQS: %w", err)
	}

	// Log any failed batch entries
	if len(resp.Failed) > 0 {
		for _, failed := range resp.Failed {
			s.logger.Error("Failed to send message in batch",
				"id", *failed.Id,
				"code", *failed.Code,
				"message", *failed.Message,
				"queue", queueName,
			)
		}
		return fmt.Errorf("some messages in the batch failed to send (%d of %d)", len(resp.Failed), len(entries))
	}

	return nil
}

// buildBatchEntry prepares a SendMessageBatchRequestEntry for a job.
// Returns (entry, true, nil) on success, (zero, false, nil) to indicate the job should be skipped,
// or (zero, false, err) to indicate a fatal error (e.g., payload too large).
func (s *SQSStore) buildBatchEntry(jb job.Job, idx int, queueName string, isFifo bool, sqsCfg jobConfig.SQSConfig) (types.SendMessageBatchRequestEntry, bool, error) {
	var empty types.SendMessageBatchRequestEntry

	// Get job name from type
	jobName := utils.GetJobName(jb)
	if jobName == "" {
		s.logger.Error("Could not determine job name from type", "queue", queueName)
		return empty, false, nil
	}

	// Marshal the job data
	jobData, err := json.Marshal(jb)
	if err != nil {
		s.logger.Error("Failed to marshal job", "error", err, "queue", queueName)
		return empty, false, nil
	}

	// Check if payload exceeds SQS message size limit (256KB)
	if len(jobData) > 256*1024 {
		s.logger.Error("Job payload exceeds SQS message size limit (256KB)", "size", len(jobData), "queue", queueName)
		return empty, false, fmt.Errorf("job payload size (%d bytes) exceeds AWS SQS limit of 256KB", len(jobData))
	}

	// Create a job ID
	jobID := utils.GenerateID()

	// Create the SQS message
	sqsJob := SQSQueuedJob{
		Job:        jobData,
		ID:         jobID,
		JobName:    jobName,
		EnqueuedAt: time.Now(),
		RetryCount: 0,
	}

	messageBody, err := json.Marshal(sqsJob)
	if err != nil {
		s.logger.Error("Failed to marshal SQS job", "error", err, "queue", queueName)
		return empty, false, nil
	}

	// Prepare message attributes
	messageAttributes := map[string]types.MessageAttributeValue{
		attrJobID: {
			DataType:    aws.String("String"),
			StringValue: aws.String(jobID),
		},
		attrQueueName: {
			DataType:    aws.String("String"),
			StringValue: aws.String(queueName),
		},
		attrJobName: {
			DataType:    aws.String("String"),
			StringValue: aws.String(jobName),
		},
		attrEnqueuedAt: {
			DataType:    aws.String("String"),
			StringValue: aws.String(fmt.Sprintf("%d", time.Now().UnixNano())),
		},
		attrRetryCount: {
			DataType:    aws.String("Number"),
			StringValue: aws.String("0"),
		},
	}

	entry := types.SendMessageBatchRequestEntry{
		Id:                aws.String(strconv.Itoa(idx)),
		MessageBody:       aws.String(string(messageBody)),
		MessageAttributes: messageAttributes,
	}

	if isFifo {
		messageGroupID := sqsCfg.MessageGroupID
		if messageGroupID == "" {
			messageGroupID = "default"
		}
		entry.MessageGroupId = aws.String(messageGroupID)

		messageDeduplicationID := sqsCfg.MessageDeduplicationID
		if messageDeduplicationID == "" {
			messageDeduplicationID = jobID
		}
		entry.MessageDeduplicationId = aws.String(messageDeduplicationID)
	}

	return entry, true, nil
}

// Pop retrieves a job from the queue
func (s *SQSStore) Pop(queueName string) (job.JobContext, error) {
	// Receive up to MaxMessages messages with a WaitTimeSeconds of 20 (long polling)
	resp, err := s.client.ReceiveMessage(context.Background(), &sqs.ReceiveMessageInput{
		QueueUrl:              aws.String(s.getQueueURL(queueName)),
		MaxNumberOfMessages:   int32(s.sqsConfig.MaxMessages),
		WaitTimeSeconds:       20, // Long polling
		VisibilityTimeout:     int32(s.sqsConfig.VisibilityTimeout.Seconds()),
		MessageAttributeNames: []string{"All"},
	})

	if err != nil {
		s.healthStatus = false
		s.logger.Error("Failed to receive message from SQS", "error", err, "queue", queueName)
		return job.JobContext{}, fmt.Errorf("failed to receive message from SQS: %w", err)
	}

	// If no messages, return empty context
	if len(resp.Messages) == 0 {
		return job.JobContext{}, nil
	}

	// Process the first message
	message := resp.Messages[0]

	// Parse the message body
	var sqsJob SQSQueuedJob
	if err := json.Unmarshal([]byte(*message.Body), &sqsJob); err != nil {
		s.logger.Error("Failed to unmarshal SQS job", "error", err, "queue", queueName)
		return job.JobContext{}, fmt.Errorf("failed to unmarshal SQS job: %w", err)
	}

	// Store the receipt handle for later acknowledgment or retry
	sqsJob.ReceiptHandle = *message.ReceiptHandle
	s.jobReceiptHandles[sqsJob.ID] = *message.ReceiptHandle

	// Extract retry count from message attributes if available
	retryCount := 0
	if message.MessageAttributes != nil {
		if retryAttr, exists := message.MessageAttributes[attrRetryCount]; exists && retryAttr.StringValue != nil {
			if count, err := strconv.Atoi(*retryAttr.StringValue); err == nil {
				retryCount = count
			}
		}
	}

	// Update sqsJob retry count from message attributes (this handles redeliveries)
	sqsJob.RetryCount = retryCount

	// Get the job type constructor
	newJobFunc, ok := registry.GetFromRegistery(sqsJob.JobName)
	if !ok {
		s.logger.Error("No job registered with name", "jobName", sqsJob.JobName, "queue", queueName)
		return job.JobContext{}, fmt.Errorf("no job registered with name: %s", sqsJob.JobName)
	}

	// Create a new job instance and unmarshal the job data
	jobInstance := newJobFunc()
	if jobInstance == nil {
		s.logger.Error("Job constructor returned nil", "jobName", sqsJob.JobName, "queue", queueName)
		return job.JobContext{}, fmt.Errorf("job constructor returned nil for jobName: %s", sqsJob.JobName)
	}
	if err := json.Unmarshal(sqsJob.Job, jobInstance); err != nil {
		s.logger.Error("Failed to decode job into type", "jobName", sqsJob.JobName, "error", err, "queue", queueName)
		return job.JobContext{}, fmt.Errorf("failed to decode job into type %s: %w", sqsJob.JobName, err)
	}

	// Create and return the job context
	return job.JobContext{
		Job:        jobInstance,
		JobID:      sqsJob.ID,
		QueueName:  queueName,
		EnqueuedAt: sqsJob.EnqueuedAt,
		RetryCount: sqsJob.RetryCount,
	}, nil
}

// Ack acknowledges a job has been processed
func (s *SQSStore) Ack(queueName string, jobID string) error {
	// Use the stored receipt handle for this job ID
	receiptHandle, ok := s.jobReceiptHandles[jobID]
	if !ok {
		s.logger.Error("Failed to get receipt handle", "error", "receipt handle not found for job ID", "jobID", jobID, "queue", queueName)
		return fmt.Errorf("receipt handle not found for job ID: %s", jobID)
	}

	// Delete the message from the queue
	_, err := s.client.DeleteMessage(context.Background(), &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(s.getQueueURL(queueName)),
		ReceiptHandle: aws.String(receiptHandle),
	})

	if err != nil {
		s.healthStatus = false
		s.logger.Error("Failed to delete message from SQS", "error", err, "jobID", jobID, "queue", queueName)
		return fmt.Errorf("failed to delete message from SQS: %w", err)
	}

	// Remove the mapping after successful ack
	delete(s.jobReceiptHandles, jobID)

	s.logger.Info("Job acknowledged and removed from SQS", "jobID", jobID, "queue", queueName)
	return nil
}

// Retry sends a job back to the queue with a delay
func (s *SQSStore) Retry(job job.Job, delay time.Duration) error {
	// SQS doesn't support arbitrary delay times, only a fixed delay of up to 15 minutes
	// If a longer delay is needed, you'd need to implement a delay queue pattern

	// This is a stub implementation - use RetryJobWithMetadata instead
	return fmt.Errorf("retry not implemented for SQS driver - use RetryJobWithMetadata")
}

// RetryJobWithMetadata handles job retries using SQS ChangeMessageVisibility
// This provides the same retry behavior as the Redis driver using SQS's native visibility timeout
func (s *SQSStore) RetryJobWithMetadata(queueName string, queuedJob job.JobContext, delay ...time.Duration) error {
	var sqsJob SQSQueuedJob
	var err error

	// Create SQSQueuedJob from JobContext
	sqsJob, err = s.createSQSQueuedJob(queuedJob, queuedJob.RetryCount+1)
	if err != nil {
		return fmt.Errorf("failed to create SQS job for retry: %w", err)
	}

	// Check if receipt handle is available
	if sqsJob.ReceiptHandle == "" {
		return fmt.Errorf("receipt handle not available for retry")
	}

	// Calculate visibility timeout from delay
	// SQS supports visibility timeout up to 12 hours (43200 seconds)
	maxSQSVisibilityTimeout := 12 * time.Hour
	if delay[0] > maxSQSVisibilityTimeout {
		s.logger.Error("Retry delay exceeds SQS maximum visibility timeout",
			"delay", delay,
			"maxTimeout", maxSQSVisibilityTimeout,
			"jobID", sqsJob.ID)
		delay[0] = maxSQSVisibilityTimeout
	}

	// Change the message visibility timeout to implement retry delay
	_, err = s.client.ChangeMessageVisibility(context.Background(), &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          aws.String(s.getQueueURL(queueName)),
		ReceiptHandle:     aws.String(sqsJob.ReceiptHandle),
		VisibilityTimeout: int32(delay[0].Seconds()),
	})

	if err != nil {
		s.healthStatus = false
		s.logger.Error("Failed to change message visibility for retry",
			"error", err,
			"jobID", sqsJob.ID,
			"queue", queueName,
			"delay", delay)
		return fmt.Errorf("failed to change message visibility for retry: %w", err)
	}

	s.logger.Info("Job scheduled for retry using visibility timeout",
		"jobID", sqsJob.ID,
		"queue", queueName,
		"retryCount", sqsJob.RetryCount,
		"delay", delay)

	return nil
}

// EnqueueMetrics adds job metrics to a metrics queue
func (s *SQSStore) EnqueueMetrics(metrics jobConfig.JobMetrics) error {
	// Create a metrics queue name
	metricsQueueName := metrics.QueueName + metricQueueSuffix

	// Serialize metrics to JSON
	jsonData, err := json.Marshal(metrics)
	if err != nil {
		s.logger.Error("Failed to marshal metrics data", "error", err, "queue", metrics.QueueName)
		return fmt.Errorf("failed to marshal metrics data: %w", err)
	}

	// Send metrics to SQS
	_, err = s.client.SendMessage(context.Background(), &sqs.SendMessageInput{
		QueueUrl:    aws.String(s.getQueueURL(metricsQueueName)),
		MessageBody: aws.String(string(jsonData)),
	})

	if err != nil {
		s.healthStatus = false
		s.logger.Error("Failed to enqueue metrics", "error", err, "queue", metrics.QueueName)
		return fmt.Errorf("failed to enqueue metrics: %w", err)
	}

	return nil
}

// DequeueMetrics retrieves job metrics from a metrics queue
func (s *SQSStore) DequeueMetrics(queueName string) (jobConfig.JobMetrics, error) {
	// Create metrics queue name
	metricsQueueName := queueName + metricQueueSuffix

	// Receive a message from the metrics queue
	resp, err := s.client.ReceiveMessage(context.Background(), &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(s.getQueueURL(metricsQueueName)),
		MaxNumberOfMessages: 1,
		WaitTimeSeconds:     0, // Don't block
	})

	if err != nil {
		s.healthStatus = false
		s.logger.Error("Failed to receive metrics from SQS", "error", err, "queue", queueName)
		return jobConfig.JobMetrics{}, fmt.Errorf("failed to receive metrics from SQS: %w", err)
	}

	// If no messages, return empty metrics
	if len(resp.Messages) == 0 {
		return jobConfig.JobMetrics{}, nil
	}

	// Parse the message body
	var metrics jobConfig.JobMetrics
	if err := json.Unmarshal([]byte(*resp.Messages[0].Body), &metrics); err != nil {
		s.logger.Error("Failed to unmarshal metrics", "error", err, "queue", queueName)
		return jobConfig.JobMetrics{}, fmt.Errorf("failed to unmarshal metrics: %w", err)
	}

	// Delete the message
	_, err = s.client.DeleteMessage(context.Background(), &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(s.getQueueURL(metricsQueueName)),
		ReceiptHandle: resp.Messages[0].ReceiptHandle,
	})

	if err != nil {
		s.logger.Error("Failed to delete metrics message", "error", err, "queue", queueName)
		// Don't return error, as we've already got the metrics
	}

	return metrics, nil
}

// IsHealthy returns the last known health status of the SQS connection.
// It does not perform a potentially blocking network call; operational methods
// (e.g., Push/Pop) update s.healthStatus when errors occur.
func (s *SQSStore) IsHealthy() bool {
	return s.healthStatus
}

// getQueueURL returns the SQS queue URL for a given queue name
func (s *SQSStore) getQueueURL(queueName string) string {
	// If it's the main queue or an empty string, return the configured URL
	if queueName == "" {
		return s.sqsConfig.QueueURL
	}

	// Check if we've already resolved this queue name (read lock)
	s.queueURLsMutex.RLock()
	if url, ok := s.queueURLs[queueName]; ok {
		s.queueURLsMutex.RUnlock()
		return url
	}
	s.queueURLsMutex.RUnlock()

	// Otherwise, return the same URL and cache it (write lock)
	s.queueURLsMutex.Lock()
	// Double-check in case another goroutine added it while we were waiting
	if url, ok := s.queueURLs[queueName]; ok {
		s.queueURLsMutex.Unlock()
		return url
	}
	// In a real implementation, you might map queue names to different SQS queues
	s.queueURLs[queueName] = s.sqsConfig.QueueURL
	s.queueURLsMutex.Unlock()

	return s.sqsConfig.QueueURL
}

// calculateDelaySeconds returns the SQS-compatible delay in seconds (max 15 minutes)
func calculateDelaySeconds(delay ...time.Duration) int32 {
	if len(delay) == 0 || delay[0] <= 0 {
		return 0
	}
	maxDelay := 15 * time.Minute
	d := delay[0]
	if d > maxDelay {
		d = maxDelay
	}
	return int32(d.Seconds())
}

// Package sqs provides an AWS SQS implementation of the queue store interface
package sqs

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/saravanasai/goqueue/adapter/utils"
	jobConfig "github.com/saravanasai/goqueue/config"
	"github.com/saravanasai/goqueue/internal/logger"
	"github.com/saravanasai/goqueue/internal/registry"
	"github.com/saravanasai/goqueue/job"
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
		client:       client,
		config:       cfg,
		sqsConfig:    sqsCfg,
		logger:       logger,
		healthStatus: true,
		queueURLs:    make(map[string]string),
	}

	// Store the main queue URL
	store.queueURLs[""] = sqsCfg.QueueURL

	return store
}

// Push adds a single job to the queue
func (s *SQSStore) Push(queueName string, jb job.Job) error {
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

// PushBatch adds multiple jobs to the queue in a single call
func (s *SQSStore) PushBatch(queueName string, jobs []job.Job) error {
	// SQS SendMessageBatch can only handle up to 10 messages at a time
	// so we need to batch them in groups of 10
	batchSize := 10
	for i := 0; i < len(jobs); i += batchSize {
		end := i + batchSize
		if end > len(jobs) {
			end = len(jobs)
		}

		batch := jobs[i:end]
		if err := s.pushBatch(queueName, batch); err != nil {
			return err
		}
	}

	return nil
}

// pushBatch handles pushing a batch of up to 10 jobs to SQS
func (s *SQSStore) pushBatch(queueName string, jobs []job.Job) error {
	var entries []types.SendMessageBatchRequestEntry

	// Get SQS configuration for FIFO settings
	sqsCfg, isSQSConfig := s.config.DriverConfig.(jobConfig.SQSConfig)
	isFifo := isSQSConfig && sqsCfg.IsFifo

	for i, jb := range jobs {
		// Get job name from type
		jobName := utils.GetJobName(jb)
		if jobName == "" {
			s.logger.Error("Could not determine job name from type", "queue", queueName)
			continue
		}

		// Marshal the job data
		jobData, err := json.Marshal(jb)
		if err != nil {
			s.logger.Error("Failed to marshal job", "error", err, "queue", queueName)
			continue
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
			continue
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

		// Create the batch entry
		entry := types.SendMessageBatchRequestEntry{
			Id:                aws.String(strconv.Itoa(i)), // Batch message ID, not job ID
			MessageBody:       aws.String(string(messageBody)),
			MessageAttributes: messageAttributes,
		}

		// Add FIFO queue specific attributes if the queue is FIFO
		if isFifo {
			// For FIFO queues, MessageGroupId is required
			messageGroupID := sqsCfg.MessageGroupID
			if messageGroupID == "" {
				messageGroupID = "default" // Default group if not specified
			}
			entry.MessageGroupId = aws.String(messageGroupID)

			// MessageDeduplicationId is optional, but recommended
			// If not provided, generate one based on the job ID
			messageDeduplicationID := sqsCfg.MessageDeduplicationID
			if messageDeduplicationID == "" {
				// Use job ID as deduplication ID if not specified
				messageDeduplicationID = jobID
			}
			entry.MessageDeduplicationId = aws.String(messageDeduplicationID)
		}

		// Add to batch entries
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

	// Store the receipt handle for later acknowledgment
	sqsJob.ReceiptHandle = *message.ReceiptHandle
	// Set jobReceiptHandles mapping for Ack
	s.jobReceiptHandles[sqsJob.ID] = *message.ReceiptHandle

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

// getReceiptHandle retrieves the SQS receipt handle for a job ID
func (s *SQSStore) getReceiptHandle(queueName string, jobID string) (string, error) {
	// In production code, you'd likely have a local cache/map that stores
	// jobID -> receiptHandle mappings when jobs are received in Pop()

	// For this implementation, we'll do a query to try to find the message
	// This isn't efficient, but works for the demo

	// Use a filter expression to find the message with the matching jobID
	resp, err := s.client.ReceiveMessage(context.Background(), &sqs.ReceiveMessageInput{
		QueueUrl:              aws.String(s.getQueueURL(queueName)),
		MaxNumberOfMessages:   10, // Try to get multiple messages to increase chance of finding it
		WaitTimeSeconds:       0,  // Don't block
		VisibilityTimeout:     int32(s.sqsConfig.VisibilityTimeout.Seconds()),
		MessageAttributeNames: []string{"All"},
		// SQS doesn't support filtering on receive, so we need to check each message
	})

	if err != nil {
		s.healthStatus = false
		s.logger.Error("Failed to query SQS for message", "error", err, "jobID", jobID, "queue", queueName)
		return "", fmt.Errorf("failed to query SQS for message: %w", err)
	}

	// Look through messages to find the one with matching jobID
	for _, msg := range resp.Messages {
		if msgJobID, ok := msg.MessageAttributes[attrJobID]; ok {
			if *msgJobID.StringValue == jobID {
				return *msg.ReceiptHandle, nil
			}
		}
	}

	return "", fmt.Errorf("receipt handle not found for job ID: %s", jobID)
}

// Retry sends a job back to the queue with a delay
func (s *SQSStore) Retry(job job.Job, delay time.Duration) error {
	// SQS doesn't support arbitrary delay times, only a fixed delay of up to 15 minutes
	// If a longer delay is needed, you'd need to implement a delay queue pattern

	// This is a stub implementation
	return fmt.Errorf("retry not implemented for SQS driver")
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

// IsHealthy returns the health status of the SQS connection
func (s *SQSStore) IsHealthy() bool {
	// If previously unhealthy, check connection again
	if !s.healthStatus {
		// Try to get queue attributes to test the connection
		_, err := s.client.GetQueueAttributes(context.Background(), &sqs.GetQueueAttributesInput{
			QueueUrl: aws.String(s.sqsConfig.QueueURL),
		})
		s.healthStatus = (err == nil)
	}
	return s.healthStatus
}

// getQueueURL returns the SQS queue URL for a given queue name
func (s *SQSStore) getQueueURL(queueName string) string {
	// If it's the main queue or an empty string, return the configured URL
	if queueName == "" {
		return s.sqsConfig.QueueURL
	}

	// Check if we've already resolved this queue name
	if url, ok := s.queueURLs[queueName]; ok {
		return url
	}

	// Otherwise, return the same URL (assuming queue names are handled via message attributes)
	// In a real implementation, you might map queue names to different SQS queues
	s.queueURLs[queueName] = s.sqsConfig.QueueURL
	return s.sqsConfig.QueueURL
}

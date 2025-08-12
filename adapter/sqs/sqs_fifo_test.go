package sqs

import (
	"context"
	"strings"
	"testing"
	"time"
	"reflect"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/saravanasai/goqueue/config"
	"github.com/saravanasai/goqueue/internal/logger"
	"github.com/saravanasai/goqueue/job"
)

// mockFifoSQSClient is a mock client for testing FIFO queue functionality
type mockFifoSQSClient struct {
	sentMessages        map[string]*sqs.SendMessageInput
	sentBatchMessages   map[string][]*sqs.SendMessageBatchInput
	expectedGroupID     string
	expectedDedupID     string
	validateGroupID     bool
	validateDedupID     bool
	validateMessageSize bool
}

func newMockFifoSQSClient() *mockFifoSQSClient {
	return &mockFifoSQSClient{
		sentMessages:      make(map[string]*sqs.SendMessageInput),
		sentBatchMessages: make(map[string][]*sqs.SendMessageBatchInput),
	}
}

// GetQueueAttributes mocks the SQS GetQueueAttributes operation
func (m *mockFifoSQSClient) GetQueueAttributes(ctx context.Context, params *sqs.GetQueueAttributesInput, optFns ...func(*sqs.Options)) (*sqs.GetQueueAttributesOutput, error) {
	return &sqs.GetQueueAttributesOutput{
		Attributes: map[string]string{
			"ApproximateNumberOfMessages": "0",
		},
	}, nil
}

func (m *mockFifoSQSClient) SendMessage(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	queueURL := *params.QueueUrl
	m.sentMessages[queueURL] = params

	// Validate MessageGroupId if needed
	if m.validateGroupID && params.MessageGroupId != nil {
		if *params.MessageGroupId != m.expectedGroupID {
			return nil, &sqs.InvalidParameterValueException{
				Message: aws.String("MessageGroupId does not match expected value"),
			}
		}
	}

	// Validate MessageDeduplicationId if needed
	if m.validateDedupID && params.MessageDeduplicationId != nil {
		if *params.MessageDeduplicationId != m.expectedDedupID {
			return nil, &sqs.InvalidParameterValueException{
				Message: aws.String("MessageDeduplicationId does not match expected value"),
			}
		}
	}

	// Validate message size if needed
	if m.validateMessageSize {
		bodySize := len(*params.MessageBody)
		if bodySize > 256*1024 {
			return nil, &sqs.InvalidParameterValueException{
				Message: aws.String("MessageBody size exceeds 256KB"),
			}
		}
	}

	return &sqs.SendMessageOutput{
		MessageId: aws.String("mock-message-id"),
	}, nil
}

func (m *mockFifoSQSClient) SendMessageBatch(ctx context.Context, params *sqs.SendMessageBatchInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageBatchOutput, error) {
	queueURL := *params.QueueUrl
	if _, exists := m.sentBatchMessages[queueURL]; !exists {
		m.sentBatchMessages[queueURL] = []*sqs.SendMessageBatchInput{}
	}
	m.sentBatchMessages[queueURL] = append(m.sentBatchMessages[queueURL], params)

	// Validate MessageGroupId and MessageDeduplicationId if needed
	if m.validateGroupID || m.validateDedupID {
		for _, entry := range params.Entries {
			if m.validateGroupID && entry.MessageGroupId != nil {
				if *entry.MessageGroupId != m.expectedGroupID {
					return &sqs.SendMessageBatchOutput{
						Failed: []types.BatchResultErrorEntry{
							{
								Id:      entry.Id,
								Code:    aws.String("InvalidParameterValue"),
								Message: aws.String("MessageGroupId does not match expected value"),
							},
						},
					}, nil
				}
			}

			if m.validateDedupID && entry.MessageDeduplicationId != nil {
				if *entry.MessageDeduplicationId != m.expectedDedupID {
					return &sqs.SendMessageBatchOutput{
						Failed: []types.BatchResultErrorEntry{
							{
								Id:      entry.Id,
								Code:    aws.String("InvalidParameterValue"),
								Message: aws.String("MessageDeduplicationId does not match expected value"),
							},
						},
					}, nil
				}
			}

			// Validate message size if needed
			if m.validateMessageSize {
				bodySize := len(*entry.MessageBody)
				if bodySize > 256*1024 {
					return &sqs.SendMessageBatchOutput{
						Failed: []types.BatchResultErrorEntry{
							{
								Id:      entry.Id,
								Code:    aws.String("InvalidParameterValue"),
								Message: aws.String("MessageBody size exceeds 256KB"),
							},
						},
					}, nil
				}
			}
		}
	}

	return &sqs.SendMessageBatchOutput{
		Successful: make([]types.SendMessageBatchResultEntry, len(params.Entries)),
	}, nil
}

func (m *mockFifoSQSClient) ReceiveMessage(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	return &sqs.ReceiveMessageOutput{}, nil
}

func (m *mockFifoSQSClient) DeleteMessage(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	return &sqs.DeleteMessageOutput{}, nil
}

// TestJob is a simple job implementation for testing
type TestJob struct {
	Name string `json:"name"`
}

func (j TestJob) Process(ctx context.Context) error {
	return nil
}

// LargeJob is a job with a large payload for testing size limits
type LargeJob struct {
	Data string `json:"data"`
}

func (j LargeJob) Process(ctx context.Context) error {
	return nil
}

// TestSQSFifoPush tests pushing to a FIFO queue
func TestSQSFifoPush(t *testing.T) {
	// Create a mock SQS client
	mockClient := newMockFifoSQSClient()

	// Create a FIFO SQS configuration
	sqsCfg := config.SQSConfig{
		QueueURL:          "https://sqs.test-region.amazonaws.com/123456789012/test-queue.fifo",
		Region:            "test-region",
		AccessKeyID:       "test-key-id",
		SecretAccessKey:   "test-secret-key",
		VisibilityTimeout: 30 * time.Second,
		MaxMessages:       1,
		IsFifo:            true,
		MessageGroupID:    "test-group",
	}

	cfg := config.Config{
		Driver:       config.DriverSQS,
		DriverConfig: sqsCfg,
	}

	// Create a logger
	testLogger := logger.NewZapLogger()

	// Initialize the SQS store with the mock client
	store := NewSQSStoreWithClient(mockClient, sqsCfg, cfg, testLogger)

	// Test job
	testJob := TestJob{Name: "FIFO Test Job"}

	// Push the job
	err := store.Push("test-queue", testJob)
	if err != nil {
		t.Fatalf("Failed to push job to FIFO queue: %v", err)
	}

	// Verify FIFO attributes were set correctly
	sent := mockClient.sentMessages["https://sqs.test-region.amazonaws.com/123456789012/test-queue.fifo"]
	if sent == nil {
		t.Fatal("No message was sent to SQS")
	}

	// Check MessageGroupId
	if sent.MessageGroupId == nil {
		t.Fatal("MessageGroupId was not set for FIFO queue")
	}
	if *sent.MessageGroupId != "test-group" {
		t.Errorf("Expected MessageGroupId to be 'test-group', got %s", *sent.MessageGroupId)
	}

	// Check MessageDeduplicationId is set (it should use job ID which we don't know beforehand)
	if sent.MessageDeduplicationId == nil {
		t.Fatal("MessageDeduplicationId was not set for FIFO queue")
	}
}

// TestSQSFifoPushBatch tests batch pushing to a FIFO queue
func TestSQSFifoPushBatch(t *testing.T) {
	// Create a mock SQS client
	mockClient := newMockFifoSQSClient()

	// Create a FIFO SQS configuration
	sqsCfg := config.SQSConfig{
		QueueURL:          "https://sqs.test-region.amazonaws.com/123456789012/test-queue.fifo",
		Region:            "test-region",
		AccessKeyID:       "test-key-id",
		SecretAccessKey:   "test-secret-key",
		VisibilityTimeout: 30 * time.Second,
		MaxMessages:       1,
		IsFifo:            true,
		MessageGroupID:    "test-group",
	}

	cfg := config.Config{
		Driver:       config.DriverSQS,
		DriverConfig: sqsCfg,
	}

	// Create a logger
	testLogger := logger.NewZapLogger()

	// Initialize the SQS store with the mock client
	store := NewSQSStoreWithClient(mockClient, sqsCfg, cfg, testLogger)

	// Test jobs
	testJobs := []job.Job{
		TestJob{Name: "FIFO Test Job 1"},
		TestJob{Name: "FIFO Test Job 2"},
		TestJob{Name: "FIFO Test Job 3"},
	}

	// Push the jobs as a batch
	err := store.PushBatch("test-queue", testJobs)
	if err != nil {
		t.Fatalf("Failed to push batch jobs to FIFO queue: %v", err)
	}

	// Verify FIFO attributes were set correctly
	batchSent := mockClient.sentBatchMessages["https://sqs.test-region.amazonaws.com/123456789012/test-queue.fifo"]
	if len(batchSent) == 0 {
		t.Fatal("No batch message was sent to SQS")
	}

	// Since the batch size is small, we should have just one batch
	batch := batchSent[0]

	// Check all entries have FIFO attributes
	for i, entry := range batch.Entries {
		// Check MessageGroupId
		if entry.MessageGroupId == nil {
			t.Fatalf("MessageGroupId was not set for entry %d", i)
		}
		if *entry.MessageGroupId != "test-group" {
			t.Errorf("Entry %d: Expected MessageGroupId to be 'test-group', got %s", i, *entry.MessageGroupId)
		}

		// Check MessageDeduplicationId is set
		if entry.MessageDeduplicationId == nil {
			t.Fatalf("MessageDeduplicationId was not set for entry %d", i)
		}
	}
}

// TestSQSMessageSizeLimit tests that messages larger than 256KB are rejected
func TestSQSMessageSizeLimit(t *testing.T) {
	// Create a mock SQS client
	mockClient := newMockFifoSQSClient()
	mockClient.validateMessageSize = true

	// Create a SQS configuration
	sqsCfg := config.SQSConfig{
		QueueURL:          "https://sqs.test-region.amazonaws.com/123456789012/test-queue",
		Region:            "test-region",
		AccessKeyID:       "test-key-id",
		SecretAccessKey:   "test-secret-key",
		VisibilityTimeout: 30 * time.Second,
		MaxMessages:       1,
	}

	cfg := config.Config{
		Driver:       config.DriverSQS,
		DriverConfig: sqsCfg,
	}

	// Create a logger
	testLogger := logger.NewZapLogger()

	// Initialize the SQS store with the mock client
	store := NewSQSStoreWithClient(mockClient, sqsCfg, cfg, testLogger)

	// Generate a string larger than 256KB
	largeData := strings.Repeat("a", 300*1024) // 300KB

	// Test job with large payload
	largeJob := LargeJob{Data: largeData}

	// Push the job - should fail with message size error
	err := store.Push("test-queue", largeJob)
	if err == nil {
		t.Fatal("Expected error for large message, but got nil")
	}

	// Check error message
	if !strings.Contains(err.Error(), "exceeds AWS SQS limit of 256KB") {
		t.Errorf("Expected error message about size limit, got: %v", err)
	}
}

// TestDefaultMessageGroupID tests that a default message group ID is used if not specified
func TestDefaultMessageGroupID(t *testing.T) {
	// Create a mock SQS client
	mockClient := newMockFifoSQSClient()

	// Create a FIFO SQS configuration without a message group ID
	sqsCfg := config.SQSConfig{
		QueueURL:          "https://sqs.test-region.amazonaws.com/123456789012/test-queue.fifo",
		Region:            "test-region",
		AccessKeyID:       "test-key-id",
		SecretAccessKey:   "test-secret-key",
		VisibilityTimeout: 30 * time.Second,
		MaxMessages:       1,
		IsFifo:            true,
		// MessageGroupID is intentionally omitted
	}

	cfg := config.Config{
		Driver:       config.DriverSQS,
		DriverConfig: sqsCfg,
	}

	// Create a logger
	testLogger := logger.NewZapLogger()

	// Initialize the SQS store with the mock client
	store := NewSQSStoreWithClient(mockClient, sqsCfg, cfg, testLogger)

	// Test job
	testJob := TestJob{Name: "FIFO Test Job"}

	// Push the job
	err := store.Push("test-queue", testJob)
	if err != nil {
		t.Fatalf("Failed to push job to FIFO queue: %v", err)
	}

	// Verify FIFO attributes were set correctly
	sent := mockClient.sentMessages["https://sqs.test-region.amazonaws.com/123456789012/test-queue.fifo"]
	if sent == nil {
		t.Fatal("No message was sent to SQS")
	}

	// Check MessageGroupId was set to default
	if sent.MessageGroupId == nil {
		t.Fatal("MessageGroupId was not set for FIFO queue")
	}
	if *sent.MessageGroupId != "default" {
		t.Errorf("Expected MessageGroupId to be 'default', got %s", *sent.MessageGroupId)
	}
}

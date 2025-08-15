package sqs

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/saravanasai/goqueue/internal/logger"
	"github.com/saravanasai/goqueue/job"
)

const (
	testJobID                      = "test-job-id"
	testJobCtxID                   = "test-job-ctx-id"
	testReceiptHandle              = "test-receipt-handle"
	testQueueName                  = "test-queue"
	changeVisibilityNotCalledError = "ChangeMessageVisibility was not called"
)

// TestSQSRetryWithMetadata tests the SQS retry mechanism using ChangeMessageVisibility
func TestSQSRetryWithMetadata(t *testing.T) {
	ensureRegistered()

	var changeVisibilityCalled bool
	var lastVisibilityTimeout int32

	// Create mock client that tracks ChangeMessageVisibility calls
	mock := &mockSQSClient{
		ChangeMessageVisibilityFunc: func(ctx context.Context, params *sqs.ChangeMessageVisibilityInput, optFns ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error) {
			changeVisibilityCalled = true
			lastVisibilityTimeout = params.VisibilityTimeout
			return &sqs.ChangeMessageVisibilityOutput{}, nil
		},
	}

	sqsCfg, cfg := makeConfig()
	store := NewSQSStoreWithClient(mock, sqsCfg, cfg, logger.NewZapLogger())

	// Create an SQS job with receipt handle
	sqsJob := SQSQueuedJob{
		ID:            testJobID,
		JobName:       "TestJob",
		ReceiptHandle: testReceiptHandle,
		RetryCount:    0,
	}

	// Test retry with a 5-second delay
	delay := 5 * time.Second
	err := store.RetryJobWithMetadata(testQueueName, sqsJob, delay)

	if err != nil {
		t.Fatalf("RetryJobWithMetadata failed: %v", err)
	}

	if !changeVisibilityCalled {
		t.Error(changeVisibilityNotCalledError)
	}

	if lastVisibilityTimeout != 5 {
		t.Errorf("Expected visibility timeout of 5 seconds, got %d", lastVisibilityTimeout)
	}
}

// TestSQSRetryWithJobContext tests the SQS retry mechanism using JobContext
func TestSQSRetryWithJobContext(t *testing.T) {
	ensureRegistered()

	var changeVisibilityCalled bool
	var lastVisibilityTimeout int32

	// Create mock client that tracks ChangeMessageVisibility calls
	mock := &mockSQSClient{
		ChangeMessageVisibilityFunc: func(ctx context.Context, params *sqs.ChangeMessageVisibilityInput, optFns ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error) {
			changeVisibilityCalled = true
			lastVisibilityTimeout = params.VisibilityTimeout
			return &sqs.ChangeMessageVisibilityOutput{}, nil
		},
	}

	sqsCfg, cfg := makeConfig()
	store := NewSQSStoreWithClient(mock, sqsCfg, cfg, logger.NewZapLogger())

	// Add receipt handle mapping for the job
	store.jobReceiptHandles[testJobCtxID] = testReceiptHandle

	// Create a JobContext
	testJob := &TestJob{ID: testJobCtxID, Data: "test data"}
	jobCtx := job.JobContext{
		Job:        testJob,
		JobID:      testJobCtxID,
		QueueName:  testQueueName,
		EnqueuedAt: time.Now(),
		RetryCount: 1,
	}

	// Test retry with a 10-second delay
	delay := 10 * time.Second
	err := store.RetryJobWithMetadata(testQueueName, jobCtx, delay)

	if err != nil {
		t.Fatalf("RetryJobWithMetadata with JobContext failed: %v", err)
	}

	if !changeVisibilityCalled {
		t.Error(changeVisibilityNotCalledError)
	}

	if lastVisibilityTimeout != 10 {
		t.Errorf("Expected visibility timeout of 10 seconds, got %d", lastVisibilityTimeout)
	}
}

// TestSQSRetryMaxVisibilityTimeout tests that retry delays exceeding SQS limits are capped
func TestSQSRetryMaxVisibilityTimeout(t *testing.T) {
	ensureRegistered()

	var changeVisibilityCalled bool
	var lastVisibilityTimeout int32

	// Create mock client that tracks ChangeMessageVisibility calls
	mock := &mockSQSClient{
		ChangeMessageVisibilityFunc: func(ctx context.Context, params *sqs.ChangeMessageVisibilityInput, optFns ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error) {
			changeVisibilityCalled = true
			lastVisibilityTimeout = params.VisibilityTimeout
			return &sqs.ChangeMessageVisibilityOutput{}, nil
		},
	}

	sqsCfg, cfg := makeConfig()
	store := NewSQSStoreWithClient(mock, sqsCfg, cfg, logger.NewZapLogger())

	// Create an SQS job with receipt handle
	sqsJob := SQSQueuedJob{
		ID:            testJobID,
		JobName:       "TestJob",
		ReceiptHandle: testReceiptHandle,
		RetryCount:    0,
	}

	// Test retry with a delay exceeding SQS maximum (13 hours)
	delay := 13 * time.Hour
	err := store.RetryJobWithMetadata(testQueueName, sqsJob, delay)

	if err != nil {
		t.Fatalf("RetryJobWithMetadata failed: %v", err)
	}

	if !changeVisibilityCalled {
		t.Error(changeVisibilityNotCalledError)
	}

	// Should be capped at 12 hours (43200 seconds)
	if lastVisibilityTimeout != 43200 {
		t.Errorf("Expected visibility timeout to be capped at 43200 seconds (12 hours), got %d", lastVisibilityTimeout)
	}
}

// TestSQSRetryNoReceiptHandle tests that retry fails when no receipt handle is available
func TestSQSRetryNoReceiptHandle(t *testing.T) {
	ensureRegistered()

	mock := &mockSQSClient{}
	sqsCfg, cfg := makeConfig()
	store := NewSQSStoreWithClient(mock, sqsCfg, cfg, logger.NewZapLogger())

	// Create an SQS job without receipt handle
	sqsJob := SQSQueuedJob{
		ID:            testJobID,
		JobName:       "TestJob",
		ReceiptHandle: "", // Empty receipt handle
		RetryCount:    0,
	}

	// Test retry should fail without receipt handle
	delay := 5 * time.Second
	err := store.RetryJobWithMetadata(testQueueName, sqsJob, delay)

	if err == nil {
		t.Error("Expected error when retrying job without receipt handle")
	}

	if err.Error() != "receipt handle not available for retry" {
		t.Errorf("Expected specific error message, got: %v", err)
	}
}

// TestSQSRetryChangeVisibilityError tests error handling when ChangeMessageVisibility fails
func TestSQSRetryChangeVisibilityError(t *testing.T) {
	ensureRegistered()

	// Create mock client that returns error for ChangeMessageVisibility
	mock := &mockSQSClient{
		ChangeMessageVisibilityFunc: func(ctx context.Context, params *sqs.ChangeMessageVisibilityInput, optFns ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error) {
			return nil, errors.New("ChangeMessageVisibility failed")
		},
	}

	sqsCfg, cfg := makeConfig()
	store := NewSQSStoreWithClient(mock, sqsCfg, cfg, logger.NewZapLogger())

	// Create an SQS job with receipt handle
	sqsJob := SQSQueuedJob{
		ID:            testJobID,
		JobName:       "TestJob",
		ReceiptHandle: testReceiptHandle,
		RetryCount:    0,
	}

	// Test retry should fail when ChangeMessageVisibility fails
	delay := 5 * time.Second
	err := store.RetryJobWithMetadata(testQueueName, sqsJob, delay)

	if err == nil {
		t.Error("Expected error when ChangeMessageVisibility fails")
	}

	// Check that health status is set to false on error
	if store.IsHealthy() {
		t.Error("Expected store to be unhealthy after ChangeMessageVisibility error")
	}
}

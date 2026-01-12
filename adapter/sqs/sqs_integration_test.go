package sqs

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/danish-a1/goqueue/adapter/utils"
	jobConfig "github.com/danish-a1/goqueue/config"
	"github.com/danish-a1/goqueue/internal/logger"
	"github.com/danish-a1/goqueue/internal/registry"
	"github.com/danish-a1/goqueue/job"
)

// minimal mock for integration-style tests
type intMockSQSClient struct {
	SendMessageFunc             func(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
	SendMessageBatchFunc        func(ctx context.Context, params *sqs.SendMessageBatchInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageBatchOutput, error)
	ReceiveMessageFunc          func(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessageFunc           func(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
	GetQueueAttrFunc            func(ctx context.Context, params *sqs.GetQueueAttributesInput, optFns ...func(*sqs.Options)) (*sqs.GetQueueAttributesOutput, error)
	ChangeMessageVisibilityFunc func(ctx context.Context, params *sqs.ChangeMessageVisibilityInput, optFns ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error)
}

func (m *intMockSQSClient) SendMessage(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	if m.SendMessageFunc != nil {
		return m.SendMessageFunc(ctx, params, optFns...)
	}
	return &sqs.SendMessageOutput{}, nil
}
func (m *intMockSQSClient) SendMessageBatch(ctx context.Context, params *sqs.SendMessageBatchInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageBatchOutput, error) {
	if m.SendMessageBatchFunc != nil {
		return m.SendMessageBatchFunc(ctx, params, optFns...)
	}
	return &sqs.SendMessageBatchOutput{}, nil
}
func (m *intMockSQSClient) ReceiveMessage(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	if m.ReceiveMessageFunc != nil {
		return m.ReceiveMessageFunc(ctx, params, optFns...)
	}
	return &sqs.ReceiveMessageOutput{}, nil
}
func (m *intMockSQSClient) DeleteMessage(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	if m.DeleteMessageFunc != nil {
		return m.DeleteMessageFunc(ctx, params, optFns...)
	}
	return &sqs.DeleteMessageOutput{}, nil
}
func (m *intMockSQSClient) GetQueueAttributes(ctx context.Context, params *sqs.GetQueueAttributesInput, optFns ...func(*sqs.Options)) (*sqs.GetQueueAttributesOutput, error) {
	if m.GetQueueAttrFunc != nil {
		return m.GetQueueAttrFunc(ctx, params, optFns...)
	}
	return &sqs.GetQueueAttributesOutput{Attributes: map[string]string{"ApproximateNumberOfMessages": "0"}}, nil
}
func (m *intMockSQSClient) ChangeMessageVisibility(ctx context.Context, params *sqs.ChangeMessageVisibilityInput, optFns ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error) {
	if m.ChangeMessageVisibilityFunc != nil {
		return m.ChangeMessageVisibilityFunc(ctx, params, optFns...)
	}
	return &sqs.ChangeMessageVisibilityOutput{}, nil
}

// test job
type SQSIntegrationJob struct {
	ID   string `json:"id"`
	Data string `json:"data"`
}

func (j *SQSIntegrationJob) Process(ctx context.Context) error { return nil }

func ensureSQSIntegrationRegistered() {
	if _, ok := registry.GetFromRegistery("SQSIntegrationJob"); !ok {
		registry.Register("SQSIntegrationJob", func() job.Job { return &SQSIntegrationJob{} })
	}
}

func makeSQSConfig() (jobConfig.SQSConfig, jobConfig.Config) {
	sqsCfg := jobConfig.SQSConfig{
		QueueURL:          "https://sqs.test/test-queue",
		Region:            "test",
		VisibilityTimeout: 30 * time.Second,
		MaxMessages:       1,
	}
	cfg := jobConfig.Config{Driver: jobConfig.DriverSQS, DriverConfig: sqsCfg}
	return sqsCfg, cfg
}

func TestSQSIntegrationPushPopAck(t *testing.T) {
	sqsCfg, cfg := makeSQSConfig()
	mock := &intMockSQSClient{}
	var lastBody string
	mock.SendMessageFunc = func(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
		lastBody = *params.MessageBody
		return &sqs.SendMessageOutput{}, nil
	}
	// prepare a message that Pop will return
	inner := &SQSIntegrationJob{ID: "p1", Data: "d1"}
	innerBytes, _ := json.Marshal(inner)
	sqsJob := SQSQueuedJob{Job: innerBytes, ID: utils.GenerateID(), JobName: "SQSIntegrationJob", EnqueuedAt: time.Now()}
	body, _ := json.Marshal(sqsJob)
	msg := types.Message{Body: awsString(string(body)), ReceiptHandle: awsString("rh1"), MessageAttributes: map[string]types.MessageAttributeValue{attrJobID: {StringValue: awsString(sqsJob.ID), DataType: awsString("String")}}}
	mock.ReceiveMessageFunc = func(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
		return &sqs.ReceiveMessageOutput{Messages: []types.Message{msg}}, nil
	}
	deleted := false
	mock.DeleteMessageFunc = func(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
		deleted = true
		return &sqs.DeleteMessageOutput{}, nil
	}

	store := NewSQSStoreWithClient(mock, sqsCfg, cfg, logger.NewZapLogger())
	ensureSQSIntegrationRegistered()
	if err := store.Push("", &SQSIntegrationJob{ID: "x", Data: "y"}); err != nil {
		t.Fatalf("Push failed: %v", err)
	}
	jc, err := store.Pop("")
	if err != nil {
		t.Fatalf("Pop error: %v", err)
	}
	if jc.Job == nil {
		t.Fatal("expected job from Pop")
	}
	if err := store.Ack("", jc.JobID); err != nil {
		t.Fatalf("Ack error: %v", err)
	}
	if !deleted {
		t.Fatal("expected DeleteMessage to have been called")
	}
	if lastBody == "" {
		t.Fatal("expected SendMessage to have been called and lastBody captured")
	}
}

func TestSQSIntegrationPushBatch(t *testing.T) {
	sqsCfg, cfg := makeSQSConfig()
	mock := &intMockSQSClient{}
	captured := 0
	mock.SendMessageBatchFunc = func(ctx context.Context, params *sqs.SendMessageBatchInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageBatchOutput, error) {
		captured += len(params.Entries)
		return &sqs.SendMessageBatchOutput{Successful: []types.SendMessageBatchResultEntry{}, Failed: []types.BatchResultErrorEntry{}}, nil
	}
	store := NewSQSStoreWithClient(mock, sqsCfg, cfg, logger.NewZapLogger())
	ensureSQSIntegrationRegistered()
	jobs := []job.Job{&SQSIntegrationJob{ID: "a", Data: "1"}, &SQSIntegrationJob{ID: "b", Data: "2"}}
	if err := store.PushBatch("", jobs); err != nil {
		t.Fatalf("PushBatch failed: %v", err)
	}
	if captured != len(jobs) {
		t.Fatalf("expected %d entries sent in batch, got %d", len(jobs), captured)
	}
}

func TestSQSIntegrationEnqueueDequeueMetrics(t *testing.T) {
	sqsCfg, cfg := makeSQSConfig()
	mock := &intMockSQSClient{}
	metricsBody := ""
	mock.SendMessageFunc = func(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
		metricsBody = *params.MessageBody
		return &sqs.SendMessageOutput{}, nil
	}
	mock.ReceiveMessageFunc = func(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
		// Return the previously sent metrics body for any requested queue URL
		if params != nil && params.QueueUrl != nil {
			return &sqs.ReceiveMessageOutput{Messages: []types.Message{{Body: &metricsBody, ReceiptHandle: awsString("rh-m")}}}, nil
		}
		return &sqs.ReceiveMessageOutput{}, nil
	}
	store := NewSQSStoreWithClient(mock, sqsCfg, cfg, logger.NewZapLogger())
	m := jobConfig.JobMetrics{QueueName: "", JobID: "jm1", Duration: 5 * time.Millisecond, Timestamp: time.Now()}
	if err := store.EnqueueMetrics(m); err != nil {
		t.Fatalf("EnqueueMetrics error: %v", err)
	}
	got, err := store.DequeueMetrics("")
	if err != nil {
		t.Fatalf("DequeueMetrics error: %v", err)
	}
	if got.JobID != m.JobID {
		t.Fatalf("metrics mismatch want=%v got=%v", m.JobID, got.JobID)
	}
}

func TestSQSIntegrationIsHealthy(t *testing.T) {
	sqsCfg, cfg := makeSQSConfig()
	mock := &intMockSQSClient{}
	store := NewSQSStoreWithClient(mock, sqsCfg, cfg, logger.NewZapLogger())
	if !store.IsHealthy() {
		t.Fatalf("expected store to be healthy")
	}
}

func TestSQSIntegrationPushWithDelay(t *testing.T) {
	// Define constants for the test
	const (
		testQueue    = "delay-test-queue"
		delayJobID   = "delay-job"
		delayJobData = "delayed data"
	)

	// Setup test environment and mocks
	sqsCfg, cfg := makeSQSConfig()
	mock, makeMessagesAvailable := setupDelayTestMocks()
	store := NewSQSStoreWithClient(mock, sqsCfg, cfg, logger.NewZapLogger())
	ensureSQSIntegrationRegistered()

	// Test standard delay
	testDelay := 5 * time.Second
	verifyPushDelay(t, store, mock, testQueue, testDelay)

	// Test over-maximum delay (should be capped)
	maxDelay := 15 * time.Minute
	verifyPushDelay(t, store, mock, testQueue, maxDelay+time.Hour)

	// Test job delivery with delays
	verifyJobDelayedDelivery(t, store, testQueue, delayJobID, delayJobData, makeMessagesAvailable)
}

// setupDelayTestMocks creates and configures mock SQS client for delay tests
func setupDelayTestMocks() (*intMockSQSClient, func()) {
	mock := &intMockSQSClient{}

	// Mock SendMessage to capture delay value
	mock.SendMessageFunc = func(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
		return &sqs.SendMessageOutput{}, nil
	}

	// Setup for message delivery testing
	messageAvailable := false

	// Create a delayed job message
	const (
		delayJobID   = "delay-job"
		delayJobData = "delayed data"
	)
	inner := &SQSIntegrationJob{ID: delayJobID, Data: delayJobData}
	innerBytes, _ := json.Marshal(inner)
	sqsJob := SQSQueuedJob{
		Job:        innerBytes,
		ID:         utils.GenerateID(),
		JobName:    "SQSIntegrationJob",
		EnqueuedAt: time.Now(),
	}
	body, _ := json.Marshal(sqsJob)
	delayedMessage := types.Message{
		Body:          awsString(string(body)),
		ReceiptHandle: awsString("rh-delay"),
		MessageAttributes: map[string]types.MessageAttributeValue{
			attrJobID: {StringValue: awsString(sqsJob.ID), DataType: awsString("String")},
		},
	}

	// Mock ReceiveMessage to control message delivery based on delay
	mock.ReceiveMessageFunc = func(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
		if messageAvailable {
			return &sqs.ReceiveMessageOutput{Messages: []types.Message{delayedMessage}}, nil
		}
		return &sqs.ReceiveMessageOutput{Messages: []types.Message{}}, nil
	}

	// Function to make messages available (simulating delay completion)
	makeAvailable := func() {
		messageAvailable = true
	}

	return mock, makeAvailable
}

// verifyPushDelay tests that a specific delay is correctly passed to SQS
func verifyPushDelay(t *testing.T, store *SQSStore, mock *intMockSQSClient, queueName string, testDelay time.Duration) {
	t.Helper()

	// Capture the actual delay sent to SQS
	var capturedDelay int32
	originalSendFunc := mock.SendMessageFunc

	mock.SendMessageFunc = func(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
		capturedDelay = params.DelaySeconds
		return originalSendFunc(ctx, params, optFns...)
	}

	// Push job with specified delay
	job := &SQSIntegrationJob{ID: "test-job", Data: "test data"}
	if err := store.Push(queueName, job, testDelay); err != nil {
		t.Fatalf("Push with delay failed: %v", err)
	}

	// Calculate expected delay (capped at 15 minutes)
	var expectedDelay int32
	maxDelaySecs := int32((15 * time.Minute).Seconds())
	delaySecs := int32(testDelay.Seconds())

	if delaySecs > maxDelaySecs {
		expectedDelay = maxDelaySecs
	} else {
		expectedDelay = delaySecs
	}

	// Verify the delay was correctly set
	if capturedDelay != expectedDelay {
		t.Fatalf("Expected delay of %d seconds, but got %d seconds", expectedDelay, capturedDelay)
	}
}

// verifyJobDelayedDelivery tests that a job is correctly delivered after its delay
func verifyJobDelayedDelivery(t *testing.T, store *SQSStore, queueName, expectedJobID, expectedJobData string, makeAvailable func()) {
	t.Helper()

	// First attempt to pop should return nothing (simulating job is still delayed)
	jc1, err := store.Pop(queueName)
	if err != nil {
		t.Fatalf("Pop failed during delay: %v", err)
	}
	if jc1.Job != nil {
		t.Fatalf("Expected no job available during delay period, but got one")
	}

	// Now simulate delay completion
	makeAvailable()

	// Next pop should return the job (delay has expired)
	jc2, err := store.Pop(queueName)
	if err != nil {
		t.Fatalf("Pop after delay failed: %v", err)
	}
	if jc2.Job == nil {
		t.Fatalf("Expected job after delay, but got nil")
	}

	// Verify it's the correct job with expected data
	gotJob, ok := jc2.Job.(*SQSIntegrationJob)
	if !ok {
		t.Fatalf("Expected *SQSIntegrationJob, got %T", jc2.Job)
	}
	if gotJob.ID != expectedJobID || gotJob.Data != expectedJobData {
		t.Fatalf("Job data mismatch: got=%+v, want={ID:%s Data:%s}",
			gotJob, expectedJobID, expectedJobData)
	}
}

func TestSQSIntegrationRetryJobWithMetadata(t *testing.T) {
	// Test constants
	const (
		testQueue = "test-queue"
		origData  = "original"
		modData   = "modified-for-retry"
	)

	// Setup
	sqsCfg, cfg := makeSQSConfig()
	mock := &intMockSQSClient{}
	store := NewSQSStoreWithClient(mock, sqsCfg, cfg, logger.NewZapLogger())
	ensureSQSIntegrationRegistered()

	// Create a test job and prepare mock for job operation
	setupMocksAndJob(t, mock, testQueue, origData, modData)

	// Phase 1: Pop the original job
	jc, err := store.Pop(testQueue)
	if err != nil {
		t.Fatalf("Pop failed: %v", err)
	}

	// Verify original job data
	verifyJobData(t, jc.Job, origData)

	// Phase 2: Retry job with modified data
	retryDelay := 3 * time.Second
	modifiedJob := &SQSIntegrationJob{ID: "retry1", Data: modData}
	jc.Job = modifiedJob

	if err := store.RetryJobWithMetadata(testQueue, jc, retryDelay); err != nil {
		t.Fatalf("RetryJobWithMetadata failed: %v", err)
	}

	// Phase 3: Verify retry by popping again (simulating after delay)
	// First pop should return nothing (visibility timeout)
	emptyJc, _ := store.Pop(testQueue)
	if emptyJc.Job != nil {
		t.Fatalf("Expected no job due to visibility timeout")
	}

	// Second pop should return retried job with incremented retry count
	retryJc, _ := store.Pop(testQueue)
	verifyJobData(t, retryJc.Job, modData)

	// Verify retry count is incremented
	if retryJc.RetryCount != 1 {
		t.Fatalf("Expected retry count to be 1, got %d", retryJc.RetryCount)
	}
}

// Helper functions to reduce cognitive complexity

// setupMocksAndJob configures the mock client for the retry test
func setupMocksAndJob(t *testing.T, mock *intMockSQSClient, queueName, origData, modData string) {
	t.Helper()

	// Create an original and modified job for the test
	originalJob := &SQSIntegrationJob{ID: "retry1", Data: origData}
	jobID := utils.GenerateID()

	// Set up mock receive function to simulate the test scenario
	receiveCallCount := 0
	mock.ReceiveMessageFunc = func(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
		receiveCallCount++

		if receiveCallCount == 1 {
			// First call: return original job
			return createMockSQSResponse(originalJob, jobID, 0)
		} else if receiveCallCount == 2 {
			// Second call: return no messages (simulating visibility timeout)
			return &sqs.ReceiveMessageOutput{Messages: []types.Message{}}, nil
		} else {
			// Third call: return modified job with incremented retry count
			modifiedJob := &SQSIntegrationJob{ID: "retry1", Data: modData}
			return createMockSQSResponse(modifiedJob, jobID, 1)
		}
	}

	// Set up mock for ChangeMessageVisibility to track retry requests
	mock.ChangeMessageVisibilityFunc = func(ctx context.Context, params *sqs.ChangeMessageVisibilityInput, optFns ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error) {
		return &sqs.ChangeMessageVisibilityOutput{}, nil
	}
}

// createMockSQSResponse creates a mock SQS response message
func createMockSQSResponse(job *SQSIntegrationJob, jobID string, retryCount int) (*sqs.ReceiveMessageOutput, error) {
	jobBytes, _ := json.Marshal(job)
	sqsJob := SQSQueuedJob{
		Job:           jobBytes,
		ID:            jobID,
		JobName:       "SQSIntegrationJob",
		EnqueuedAt:    time.Now(),
		RetryCount:    retryCount,
		ReceiptHandle: fmt.Sprintf("receipt-handle-%d", retryCount+1),
	}

	body, _ := json.Marshal(sqsJob)
	return &sqs.ReceiveMessageOutput{
		Messages: []types.Message{
			{
				Body:          awsString(string(body)),
				ReceiptHandle: awsString(sqsJob.ReceiptHandle),
				MessageAttributes: map[string]types.MessageAttributeValue{
					attrJobID:      {StringValue: awsString(sqsJob.ID), DataType: awsString("String")},
					attrRetryCount: {StringValue: awsString(fmt.Sprintf("%d", retryCount)), DataType: awsString("Number")},
				},
			},
		},
	}, nil
}

// verifyJobData checks that the job data matches expected values
func verifyJobData(t *testing.T, j job.Job, expectedData string) {
	t.Helper()

	gotJob, ok := j.(*SQSIntegrationJob)
	if !ok {
		t.Fatalf("Expected SQSIntegrationJob, got %T", j)
	}

	if gotJob.ID != "retry1" || gotJob.Data != expectedData {
		t.Fatalf("Job data mismatch: got {ID:%s Data:%s}, want {ID:retry1 Data:%s}",
			gotJob.ID, gotJob.Data, expectedData)
	}
}

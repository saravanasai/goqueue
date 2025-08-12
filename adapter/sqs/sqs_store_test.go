package sqs

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/saravanasai/goqueue/config"
	"github.com/saravanasai/goqueue/internal/logger"
)

// mockSQSClient is a simple mock for the SQS client
type mockSQSClient struct{}

// GetQueueAttributes mocks the SQS GetQueueAttributes operation
func (m *mockSQSClient) GetQueueAttributes(ctx context.Context, params *sqs.GetQueueAttributesInput, optFns ...func(*sqs.Options)) (*sqs.GetQueueAttributesOutput, error) {
	return &sqs.GetQueueAttributesOutput{
		Attributes: map[string]string{
			"ApproximateNumberOfMessages": "0",
		},
	}, nil
}

// The following methods are required by the SQSClient interface but not used in this test
func (m *mockSQSClient) SendMessage(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	return &sqs.SendMessageOutput{}, nil
}

func (m *mockSQSClient) SendMessageBatch(ctx context.Context, params *sqs.SendMessageBatchInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageBatchOutput, error) {
	return &sqs.SendMessageBatchOutput{}, nil
}

func (m *mockSQSClient) ReceiveMessage(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	return &sqs.ReceiveMessageOutput{}, nil
}

func (m *mockSQSClient) DeleteMessage(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	return &sqs.DeleteMessageOutput{}, nil
}

// TestSQSHealth tests the health check functionality for SQS
func TestSQSHealth(t *testing.T) {
	// Create a simple SQS configuration
	cfg := config.Config{
		Driver: config.DriverSQS,
		DriverConfig: config.SQSConfig{
			QueueURL:          "https://sqs.test-region.amazonaws.com/123456789012/test-queue",
			Region:            "test-region",
			AccessKeyID:       "test-key-id",
			SecretAccessKey:   "test-secret-key",
			VisibilityTimeout: 30 * time.Second,
			MaxMessages:       10,
		},
	}

	// Create a mock SQS client
	mockClient := &mockSQSClient{}

	// Create a logger
	testLogger := logger.NewZapLogger()

	// Initialize the SQS store
	store := &SQSStore{
		client: mockClient,
		config: cfg,
		logger: testLogger,
	}

	// Test IsHealthy
	healthy := store.IsHealthy()
	if !healthy {
		t.Error("Expected SQS store to be healthy, but it reported as unhealthy")
	}
}

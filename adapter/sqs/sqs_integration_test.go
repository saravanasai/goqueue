package sqs

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/saravanasai/goqueue/adapter/utils"
	jobConfig "github.com/saravanasai/goqueue/config"
	"github.com/saravanasai/goqueue/internal/logger"
	"github.com/saravanasai/goqueue/internal/registry"
	"github.com/saravanasai/goqueue/job"
)

// minimal mock for integration-style tests
type intMockSQSClient struct {
	SendMessageFunc      func(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
	SendMessageBatchFunc func(ctx context.Context, params *sqs.SendMessageBatchInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageBatchOutput, error)
	ReceiveMessageFunc   func(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessageFunc    func(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
	GetQueueAttrFunc     func(ctx context.Context, params *sqs.GetQueueAttributesInput, optFns ...func(*sqs.Options)) (*sqs.GetQueueAttributesOutput, error)
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

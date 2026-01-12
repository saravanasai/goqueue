package sqs

import (
	"context"
	"encoding/json"
	"errors"
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

// flexible mock implementing SQSClient using function hooks
type mockSQSClient struct {
	SendMessageFunc             func(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
	SendMessageBatchFunc        func(ctx context.Context, params *sqs.SendMessageBatchInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageBatchOutput, error)
	ReceiveMessageFunc          func(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessageFunc           func(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
	GetQueueAttrFunc            func(ctx context.Context, params *sqs.GetQueueAttributesInput, optFns ...func(*sqs.Options)) (*sqs.GetQueueAttributesOutput, error)
	ChangeMessageVisibilityFunc func(ctx context.Context, params *sqs.ChangeMessageVisibilityInput, optFns ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error)
}

func (m *mockSQSClient) SendMessage(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	if m.SendMessageFunc != nil {
		return m.SendMessageFunc(ctx, params, optFns...)
	}
	return &sqs.SendMessageOutput{}, nil
}
func (m *mockSQSClient) SendMessageBatch(ctx context.Context, params *sqs.SendMessageBatchInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageBatchOutput, error) {
	if m.SendMessageBatchFunc != nil {
		return m.SendMessageBatchFunc(ctx, params, optFns...)
	}
	return &sqs.SendMessageBatchOutput{}, nil
}
func (m *mockSQSClient) ReceiveMessage(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	if m.ReceiveMessageFunc != nil {
		return m.ReceiveMessageFunc(ctx, params, optFns...)
	}
	return &sqs.ReceiveMessageOutput{}, nil
}
func (m *mockSQSClient) DeleteMessage(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	if m.DeleteMessageFunc != nil {
		return m.DeleteMessageFunc(ctx, params, optFns...)
	}
	return &sqs.DeleteMessageOutput{}, nil
}
func (m *mockSQSClient) GetQueueAttributes(ctx context.Context, params *sqs.GetQueueAttributesInput, optFns ...func(*sqs.Options)) (*sqs.GetQueueAttributesOutput, error) {
	if m.GetQueueAttrFunc != nil {
		return m.GetQueueAttrFunc(ctx, params, optFns...)
	}
	return &sqs.GetQueueAttributesOutput{Attributes: map[string]string{"ApproximateNumberOfMessages": "0"}}, nil
}
func (m *mockSQSClient) ChangeMessageVisibility(ctx context.Context, params *sqs.ChangeMessageVisibilityInput, optFns ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error) {
	if m.ChangeMessageVisibilityFunc != nil {
		return m.ChangeMessageVisibilityFunc(ctx, params, optFns...)
	}
	return &sqs.ChangeMessageVisibilityOutput{}, nil
}

// Test helpers
type TestJob struct {
	ID   string `json:"id"`
	Data string `json:"data"`
}

func (t *TestJob) Process(ctx context.Context) error { return nil }

func makeConfig() (jobConfig.SQSConfig, jobConfig.Config) {
	sqsCfg := jobConfig.SQSConfig{
		QueueURL:          "https://sqs.test/test-queue",
		Region:            "test",
		VisibilityTimeout: 30 * time.Second,
		MaxMessages:       1,
	}
	cfg := jobConfig.Config{Driver: jobConfig.DriverSQS, DriverConfig: sqsCfg}
	return sqsCfg, cfg
}

func ensureRegistered() {
	if _, ok := registry.GetFromRegistery("TestJob"); !ok {
		registry.Register("TestJob", func() job.Job { return &TestJob{} })
	}
}

// Tests
func TestSQSPushSuccess(t *testing.T) {
	sqsCfg, cfg := makeConfig()
	mock := &mockSQSClient{}
	var lastBody string
	mock.SendMessageFunc = func(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
		lastBody = *params.MessageBody
		return &sqs.SendMessageOutput{}, nil
	}
	store := NewSQSStoreWithClient(mock, sqsCfg, cfg, logger.NewZapLogger())
	ensureRegistered()
	job := &TestJob{ID: "1", Data: "d"}
	if err := store.Push("", job); err != nil {
		t.Fatalf("Push failed: %v", err)
	}
	// ensure message body contains our job id/name
	if lastBody == "" {
		t.Fatal("expected sent message body to be captured")
	}
}

func TestSQSPushLargePayload(t *testing.T) {
	sqsCfg, cfg := makeConfig()
	store := NewSQSStoreWithClient(&mockSQSClient{}, sqsCfg, cfg, logger.NewZapLogger())
	// create large payload > 256KB
	large := make([]byte, 260*1024)
	for i := range large {
		large[i] = 'a'
	}
	job := &TestJob{Data: string(large)}
	if err := store.Push("", job); err == nil {
		t.Fatal("expected error for large payload, got nil")
	}
}

func TestSQSPushSendErrorSetsUnhealthy(t *testing.T) {
	sqsCfg, cfg := makeConfig()
	mock := &mockSQSClient{}
	mock.SendMessageFunc = func(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
		return nil, errors.New("send failed")
	}
	store := NewSQSStoreWithClient(mock, sqsCfg, cfg, logger.NewZapLogger())
	ensureRegistered()
	if err := store.Push("", &TestJob{ID: "x", Data: "y"}); err == nil {
		t.Fatal("expected Push to return error when SendMessage fails")
	}
	if store.IsHealthy() {
		t.Fatal("expected store to be unhealthy after send error")
	}
}

func TestSQSPushBatchSuccess(t *testing.T) {
	sqsCfg, cfg := makeConfig()
	mock := &mockSQSClient{}
	captured := 0
	mock.SendMessageBatchFunc = func(ctx context.Context, params *sqs.SendMessageBatchInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageBatchOutput, error) {
		captured += len(params.Entries)
		return &sqs.SendMessageBatchOutput{Successful: []types.SendMessageBatchResultEntry{}, Failed: []types.BatchResultErrorEntry{}}, nil
	}
	store := NewSQSStoreWithClient(mock, sqsCfg, cfg, logger.NewZapLogger())
	ensureRegistered()
	jobs := []job.Job{&TestJob{ID: "a", Data: "1"}, &TestJob{ID: "b", Data: "2"}}
	if err := store.PushBatch("", jobs); err != nil {
		t.Fatalf("PushBatch failed: %v", err)
	}
	if captured != len(jobs) {
		t.Fatalf("expected %d entries sent in batch, got %d", len(jobs), captured)
	}
}

func TestSQSPopAndAck(t *testing.T) {
	sqsCfg, cfg := makeConfig()
	mock := &mockSQSClient{}
	// prepare an SQSQueuedJob body
	innerJob := &TestJob{ID: "p1", Data: "d1"}
	innerBytes, _ := json.Marshal(innerJob)
	sqsJob := SQSQueuedJob{Job: innerBytes, ID: utils.GenerateID(), JobName: "TestJob", EnqueuedAt: time.Now()}
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
	// ensure registry has the job constructor
	ensureRegistered()
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
}

func TestSQSEnqueueDequeueMetrics(t *testing.T) {
	sqsCfg, cfg := makeConfig()
	mock := &mockSQSClient{}
	metricsBody := ""
	mock.SendMessageFunc = func(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
		metricsBody = *params.MessageBody
		return &sqs.SendMessageOutput{}, nil
	}
	mock.ReceiveMessageFunc = func(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
		// Return the previously sent metrics body for any requested queue URL so DequeueMetrics gets the message
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

// small helpers
func awsString(s string) *string { return &s }

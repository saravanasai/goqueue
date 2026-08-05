package nats

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	jobConfig "github.com/saravanasai/goqueue/config"
	"github.com/saravanasai/goqueue/internal/logger"
	"github.com/saravanasai/goqueue/internal/registry"
	"github.com/saravanasai/goqueue/job"
)

// mockJetStream implements jetstream.JetStream using function hooks
type mockJetStream struct {
	jetstream.JetStream
	PublishFunc                func(ctx context.Context, subject string, payload []byte, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error)
	PublishMsgFunc             func(ctx context.Context, msg *nats.Msg, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error)
	PublishAsyncFunc           func(subject string, payload []byte, opts ...jetstream.PublishOpt) (jetstream.PubAckFuture, error)
	PublishMsgAsyncFunc        func(msg *nats.Msg, opts ...jetstream.PublishOpt) (jetstream.PubAckFuture, error)
	PublishAsyncCompleteFunc   func() <-chan struct{}
	CreateOrUpdateStreamFunc   func(ctx context.Context, cfg jetstream.StreamConfig) (jetstream.Stream, error)
	CreateOrUpdateConsumerFunc func(ctx context.Context, stream string, cfg jetstream.ConsumerConfig) (jetstream.Consumer, error)
}

func (m *mockJetStream) Publish(ctx context.Context, subject string, payload []byte, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	if m.PublishFunc != nil {
		return m.PublishFunc(ctx, subject, payload, opts...)
	}
	return &jetstream.PubAck{}, nil
}
func (m *mockJetStream) PublishMsg(ctx context.Context, msg *nats.Msg, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	if m.PublishMsgFunc != nil {
		return m.PublishMsgFunc(ctx, msg, opts...)
	}
	return &jetstream.PubAck{}, nil
}
func (m *mockJetStream) PublishAsync(subject string, payload []byte, opts ...jetstream.PublishOpt) (jetstream.PubAckFuture, error) {
	if m.PublishAsyncFunc != nil {
		return m.PublishAsyncFunc(subject, payload, opts...)
	}
	return &mockPubAckFuture{}, nil
}
func (m *mockJetStream) PublishMsgAsync(msg *nats.Msg, opts ...jetstream.PublishOpt) (jetstream.PubAckFuture, error) {
	if m.PublishMsgAsyncFunc != nil {
		return m.PublishMsgAsyncFunc(msg, opts...)
	}
	return &mockPubAckFuture{}, nil
}
func (m *mockJetStream) PublishAsyncComplete() <-chan struct{} {
	if m.PublishAsyncCompleteFunc != nil {
		return m.PublishAsyncCompleteFunc()
	}
	ch := make(chan struct{})
	close(ch)
	return ch
}
func (m *mockJetStream) CreateOrUpdateStream(ctx context.Context, cfg jetstream.StreamConfig) (jetstream.Stream, error) {
	if m.CreateOrUpdateStreamFunc != nil {
		return m.CreateOrUpdateStreamFunc(ctx, cfg)
	}
	return nil, nil
}
func (m *mockJetStream) CreateOrUpdateConsumer(ctx context.Context, stream string, cfg jetstream.ConsumerConfig) (jetstream.Consumer, error) {
	if m.CreateOrUpdateConsumerFunc != nil {
		return m.CreateOrUpdateConsumerFunc(ctx, stream, cfg)
	}
	return &mockConsumer{}, nil
}

// mockConsumer implements jetstream.Consumer, overriding only Next
type mockConsumer struct {
	jetstream.Consumer
	NextFunc func(opts ...jetstream.FetchOpt) (jetstream.Msg, error)
}

func (m *mockConsumer) Next(opts ...jetstream.FetchOpt) (jetstream.Msg, error) {
	if m.NextFunc != nil {
		return m.NextFunc(opts...)
	}
	return nil, jetstream.ErrNoMessages
}

// mockMsg implements jetstream.Msg, overriding Data, Ack, NakWithDelay, Metadata
type mockMsg struct {
	jetstream.Msg
	data         []byte
	numDelivered uint64
	AckFunc      func() error
	NakDelayFunc func(delay time.Duration) error
}

func (m *mockMsg) Data() []byte { return m.data }
func (m *mockMsg) Ack() error {
	if m.AckFunc != nil {
		return m.AckFunc()
	}
	return nil
}
func (m *mockMsg) NakWithDelay(delay time.Duration) error {
	if m.NakDelayFunc != nil {
		return m.NakDelayFunc(delay)
	}
	return nil
}

func (m *mockMsg) Metadata() (*jetstream.MsgMetadata, error) {
	n := m.numDelivered
	if n == 0 {
		n = 1
	}
	return &jetstream.MsgMetadata{NumDelivered: n}, nil
}

// mockNATSClient implements NATSClient using function hooks
type mockNATSClient struct {
	StatusFunc func() nats.Status
}

func (m *mockNATSClient) Status() nats.Status {
	if m.StatusFunc != nil {
		return m.StatusFunc()
	}
	return nats.CONNECTED
}

// mockPubAckFuture implements jetstream.PubAckFuture
type mockPubAckFuture struct {
	ack *jetstream.PubAck
	err error
}

func (m *mockPubAckFuture) Msg() *nats.Msg {
	return nil
}

func (m *mockPubAckFuture) Ok() <-chan *jetstream.PubAck {
	ch := make(chan *jetstream.PubAck, 1)
	if m.err == nil {
		ack := m.ack
		if ack == nil {
			ack = &jetstream.PubAck{}
		}
		ch <- ack
		close(ch)
	}
	return ch
}
func (m *mockPubAckFuture) Err() <-chan error {
	ch := make(chan error, 1)
	if m.err != nil {
		ch <- m.err
		close(ch)
	}
	return ch
}

// Test helpers
type TestJob struct {
	ID   string `json:"id"`
	Data string `json:"data"`
}

func (t *TestJob) Process(ctx context.Context) error { return nil }

func makeConfig() (jobConfig.NATSConfig, jobConfig.Config) {
	natsCfg := jobConfig.NATSConfig{
		URL:        "nats://test",
		StreamName: "TESTSTREAM",
	}
	cfg := jobConfig.Config{Driver: jobConfig.DriverNATS, DriverConfig: natsCfg, MaxRetryAttempts: 3}
	return natsCfg, cfg
}

func ensureRegistered() {
	if _, ok := registry.GetFromRegistery("TestJob"); !ok {
		registry.Register("TestJob", func() job.Job { return &TestJob{} })
	}
}

// Tests

func TestNATSPushSuccess(t *testing.T) {
	natsCfg, cfg := makeConfig()
	var lastSubject string
	mock := &mockJetStream{
		PublishFunc: func(ctx context.Context, subject string, payload []byte, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
			lastSubject = subject
			return &jetstream.PubAck{}, nil
		},
	}
	store := NewNATSStoreWithClient(&mockNATSClient{}, mock, cfg, natsCfg, logger.NewZapLogger())
	ensureRegistered()

	if err := store.Push("emails", &TestJob{ID: "1", Data: "d"}); err != nil {
		t.Fatalf("Push failed: %v", err)
	}
	if lastSubject != "TESTSTREAM.emails" {
		t.Fatalf("expected subject TESTSTREAM.emails, got %s", lastSubject)
	}
}

func TestNATSPushErrorSetsUnhealthy(t *testing.T) {
	natsCfg, cfg := makeConfig()
	mock := &mockJetStream{
		PublishFunc: func(ctx context.Context, subject string, payload []byte, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
			return nil, errors.New("publish failed")
		},
	}
	store := NewNATSStoreWithClient(&mockNATSClient{}, mock, cfg, natsCfg, logger.NewZapLogger())
	ensureRegistered()

	if err := store.Push("emails", &TestJob{ID: "3", Data: "d"}); err == nil {
		t.Fatal("expected Push to return error when Publish fails")
	}
	if store.IsHealthy() {
		t.Fatal("expected store to be unhealthy after publish error")
	}
}

func TestNATSPushBatchSuccess(t *testing.T) {
	natsCfg, cfg := makeConfig()
	ensureRegistered()

	captured := 0
	mock := &mockJetStream{
		PublishAsyncFunc: func(subject string, payload []byte, opts ...jetstream.PublishOpt) (jetstream.PubAckFuture, error) {
			captured++
			return &mockPubAckFuture{}, nil
		},
	}
	store := NewNATSStoreWithClient(&mockNATSClient{}, mock, cfg, natsCfg, logger.NewZapLogger())

	jobs := []job.Job{&TestJob{ID: "a", Data: "1"}, &TestJob{ID: "b", Data: "2"}}
	if err := store.PushBatch("emails", jobs); err != nil {
		t.Fatalf("PushBatch failed: %v", err)
	}
	if captured != len(jobs) {
		t.Fatalf("expected %d entries published, got %d", len(jobs), captured)
	}
}

func TestNATSPopAndAck(t *testing.T) {
	natsCfg, cfg := makeConfig()
	ensureRegistered()

	innerJob := &TestJob{ID: "p1", Data: "d1"}
	innerBytes, _ := json.Marshal(innerJob)
	natsJob := NATSQueuedJob{Job: innerBytes, ID: "job-1", JobName: "TestJob", EnqueuedAt: time.Now()}
	body, _ := json.Marshal(natsJob)

	msg := &mockMsg{data: body}
	acked := false
	msg.AckFunc = func() error { acked = true; return nil }

	mock := &mockJetStream{
		CreateOrUpdateConsumerFunc: func(ctx context.Context, stream string, cfg jetstream.ConsumerConfig) (jetstream.Consumer, error) {
			return &mockConsumer{NextFunc: func(opts ...jetstream.FetchOpt) (jetstream.Msg, error) {
				return msg, nil
			}}, nil
		},
	}

	store := NewNATSStoreWithClient(&mockNATSClient{}, mock, cfg, natsCfg, logger.NewZapLogger())

	jc, err := store.Pop("emails")
	if err != nil {
		t.Fatalf("Pop error: %v", err)
	}
	if jc.JobID != "job-1" {
		t.Fatalf("expected JobID job-1, got %s", jc.JobID)
	}

	if err := store.Ack("emails", jc.JobID); err != nil {
		t.Fatalf("Ack error: %v", err)
	}
	if !acked {
		t.Fatal("expected msg.Ack to have been called")
	}
}

func TestNATSEnqueueDequeueMetrics(t *testing.T) {
	natsCfg, cfg := makeConfig()

	m := jobConfig.JobMetrics{QueueName: "emails", JobID: "jm1", Duration: 5 * time.Millisecond, Timestamp: time.Now()}
	var publishedBody []byte

	mock := &mockJetStream{
		PublishFunc: func(ctx context.Context, subject string, payload []byte, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
			publishedBody = payload
			return &jetstream.PubAck{}, nil
		},
		CreateOrUpdateConsumerFunc: func(ctx context.Context, stream string, cfg jetstream.ConsumerConfig) (jetstream.Consumer, error) {
			return &mockConsumer{NextFunc: func(opts ...jetstream.FetchOpt) (jetstream.Msg, error) {
				return &mockMsg{data: publishedBody}, nil
			}}, nil
		},
	}
	store := NewNATSStoreWithClient(&mockNATSClient{}, mock, cfg, natsCfg, logger.NewZapLogger())

	if err := store.EnqueueMetrics(m); err != nil {
		t.Fatalf("EnqueueMetrics error: %v", err)
	}
	got, err := store.DequeueMetrics("emails")
	if err != nil {
		t.Fatalf("DequeueMetrics error: %v", err)
	}
	if got.JobID != m.JobID {
		t.Fatalf("metrics mismatch want=%v got=%v", m.JobID, got.JobID)
	}
}

func TestNATSPushLargePayload(t *testing.T) {
	natsCfg, cfg := makeConfig()
	store := NewNATSStoreWithClient(&mockNATSClient{}, &mockJetStream{}, cfg, natsCfg, logger.NewZapLogger())
	ensureRegistered()

	// create a payload over 1MB
	large := make([]byte, maxNATSPayloadSize+1024)
	for i := range large {
		large[i] = 'a'
	}
	job := &TestJob{Data: string(large)}
	if err := store.Push("emails", job); err == nil {
		t.Fatal("expected error for large payload, got nil")
	}
}

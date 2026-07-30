package nats

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	jobConfig "github.com/saravanasai/goqueue/config"
	"github.com/saravanasai/goqueue/internal/logger"
	"github.com/saravanasai/goqueue/internal/registry"
	"github.com/saravanasai/goqueue/job"
)

// NATSIntegrationJob is a distinct job type from the plain unit test file
type NATSIntegrationJob struct {
	ID   string `json:"id"`
	Data string `json:"data"`
}

func (j *NATSIntegrationJob) Process(ctx context.Context) error { return nil }

func ensureNATSIntegrationRegistered() {
	if _, ok := registry.GetFromRegistery("NATSIntegrationJob"); !ok {
		registry.Register("NATSIntegrationJob", func() job.Job { return &NATSIntegrationJob{} })
	}
}

func makeNATSIntegrationConfig() (jobConfig.NATSConfig, jobConfig.Config) {
	natsCfg := jobConfig.NATSConfig{URL: "nats://test", StreamName: "INTEGSTREAM"}
	cfg := jobConfig.Config{Driver: jobConfig.DriverNATS, DriverConfig: natsCfg, MaxRetryAttempts: 3}
	return natsCfg, cfg
}

// Full Push -> Pop -> Ack workflow
func TestNATSIntegrationPushPopAck(t *testing.T) {
	natsCfg, cfg := makeNATSIntegrationConfig()
	ensureNATSIntegrationRegistered()

	inner := &NATSIntegrationJob{ID: "p1", Data: "d1"}
	innerBytes, _ := json.Marshal(inner)
	natsJob := NATSQueuedJob{Job: innerBytes, ID: "job-int-1", JobName: "NATSIntegrationJob", EnqueuedAt: time.Now()}
	body, _ := json.Marshal(natsJob)
	storedMsg := &mockMsg{data: body}

	acked := false
	storedMsg.AckFunc = func() error { acked = true; return nil }

	var lastSubject string
	mock := &mockJetStream{
		PublishFunc: func(ctx context.Context, subject string, payload []byte, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
			lastSubject = subject
			return &jetstream.PubAck{}, nil
		},
		CreateOrUpdateConsumerFunc: func(ctx context.Context, stream string, cfg jetstream.ConsumerConfig) (jetstream.Consumer, error) {
			return &mockConsumer{NextFunc: func(opts ...jetstream.FetchOpt) (jetstream.Msg, error) {
				return storedMsg, nil
			}}, nil
		},
	}

	store := NewNATSStoreWithClient(&mockNATSClient{}, mock, cfg, natsCfg, logger.NewZapLogger())

	if err := store.Push("emails", &NATSIntegrationJob{ID: "x", Data: "y"}); err != nil {
		t.Fatalf("Push failed: %v", err)
	}
	if lastSubject != "INTEGSTREAM.emails" {
		t.Fatalf("expected subject INTEGSTREAM.emails, got %s", lastSubject)
	}

	jc, err := store.Pop("emails")
	if err != nil {
		t.Fatalf("Pop error: %v", err)
	}
	if jc.Job == nil {
		t.Fatal("expected job from Pop")
	}

	if err := store.Ack("emails", jc.JobID); err != nil {
		t.Fatalf("Ack error: %v", err)
	}
	if !acked {
		t.Fatal("expected msg.Ack to have been called")
	}
}

func TestNATSIntegrationPushBatch(t *testing.T) {
	natsCfg, cfg := makeNATSIntegrationConfig()
	ensureNATSIntegrationRegistered()

	captured := 0
	mock := &mockJetStream{
		PublishAsyncFunc: func(subject string, payload []byte, opts ...jetstream.PublishOpt) (jetstream.PubAckFuture, error) {
			captured++
			return &mockPubAckFuture{}, nil
		},
	}
	store := NewNATSStoreWithClient(&mockNATSClient{}, mock, cfg, natsCfg, logger.NewZapLogger())

	jobs := []job.Job{
		&NATSIntegrationJob{ID: "a", Data: "1"},
		&NATSIntegrationJob{ID: "b", Data: "2"},
	}
	if err := store.PushBatch("emails", jobs); err != nil {
		t.Fatalf("PushBatch failed: %v", err)
	}
	if captured != len(jobs) {
		t.Fatalf("expected %d publishes, got %d", len(jobs), captured)
	}
}

func TestNATSIntegrationEnqueueDequeueMetrics(t *testing.T) {
	natsCfg, cfg := makeNATSIntegrationConfig()

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

func TestNATSIntegrationIsHealthy(t *testing.T) {
	natsCfg, cfg := makeNATSIntegrationConfig()
	store := NewNATSStoreWithClient(&mockNATSClient{}, &mockJetStream{}, cfg, natsCfg, logger.NewZapLogger())
	if !store.IsHealthy() {
		t.Fatal("expected store to be healthy")
	}
}

// Verifies Push with a delay sets the JetStream schedule headers correctly
func TestNATSIntegrationPushWithDelay(t *testing.T) {
	natsCfg, cfg := makeNATSIntegrationConfig()
	ensureNATSIntegrationRegistered()

	var capturedMsg *nats.Msg
	mock := &mockJetStream{
		PublishMsgFunc: func(ctx context.Context, msg *nats.Msg, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
			capturedMsg = msg
			return &jetstream.PubAck{}, nil
		},
	}
	store := NewNATSStoreWithClient(&mockNATSClient{}, mock, cfg, natsCfg, logger.NewZapLogger())

	testDelay := 5 * time.Second
	if err := store.Push("emails", &NATSIntegrationJob{ID: "delay-job", Data: "delayed"}, testDelay); err != nil {
		t.Fatalf("Push with delay failed: %v", err)
	}
	if capturedMsg == nil {
		t.Fatal("expected PublishMsg to be called for delayed push")
	}
	if capturedMsg.Header.Get("Nats-Schedule-Target") != "INTEGSTREAM.emails" {
		t.Fatalf("unexpected schedule target: %s", capturedMsg.Header.Get("Nats-Schedule-Target"))
	}
	if capturedMsg.Header.Get("Nats-Schedule") == "" {
		t.Fatal("expected Nats-Schedule header to be set")
	}
}

// Verifies RetryJobWithMetadata's Nak path and retry count increment
func TestNATSIntegrationRetryJobWithMetadata(t *testing.T) {
	natsCfg, cfg := makeNATSIntegrationConfig()
	ensureNATSIntegrationRegistered()

	inner := &NATSIntegrationJob{ID: "retry1", Data: "original"}
	innerBytes, _ := json.Marshal(inner)
	natsJob := NATSQueuedJob{Job: innerBytes, ID: "retry-job-1", JobName: "NATSIntegrationJob", EnqueuedAt: time.Now()}
	body, _ := json.Marshal(natsJob)

	callCount := 0
	var nakWithDelay time.Duration = -1

	mock := &mockJetStream{
		CreateOrUpdateConsumerFunc: func(ctx context.Context, stream string, cfg jetstream.ConsumerConfig) (jetstream.Consumer, error) {
			return &mockConsumer{NextFunc: func(opts ...jetstream.FetchOpt) (jetstream.Msg, error) {
				callCount++
				if callCount == 1 {
					// First delivery
					m := &mockMsg{data: body, numDelivered: 1}
					m.NakDelayFunc = func(delay time.Duration) error {
						nakWithDelay = delay
						return nil
					}
					return m, nil
				}
				// Second delivery
				return &mockMsg{data: body, numDelivered: 2}, nil
			}}, nil
		},
	}
	store := NewNATSStoreWithClient(&mockNATSClient{}, mock, cfg, natsCfg, logger.NewZapLogger())

	jc, err := store.Pop("emails")
	if err != nil {
		t.Fatalf("Pop failed: %v", err)
	}
	if jc.RetryCount != 0 {
		t.Fatalf("expected RetryCount 0 on first delivery, got %d", jc.RetryCount)
	}

	retryDelay := 3 * time.Second
	if err := store.RetryJobWithMetadata("emails", jc, retryDelay); err != nil {
		t.Fatalf("RetryJobWithMetadata failed: %v", err)
	}
	if nakWithDelay != retryDelay {
		t.Fatalf("expected NakWithDelay called with %v, got %v", retryDelay, nakWithDelay)
	}

	jc2, err := store.Pop("emails")
	if err != nil {
		t.Fatalf("second Pop failed: %v", err)
	}
	if jc2.RetryCount != 1 {
		t.Fatalf("expected RetryCount 1 after retry, got %d", jc2.RetryCount)
	}
}

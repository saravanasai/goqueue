package memory

import (
	"context"
	"testing"
	"time"

	"github.com/saravanasai/goqueue/config"
	"github.com/saravanasai/goqueue/internal/logger"
	"github.com/saravanasai/goqueue/job"
)

// TestJob is a simple job implementation for testing
type TestJob struct {
	ID   string `json:"id"`
	Data string `json:"data"`
}

func (j *TestJob) Process(ctx context.Context) error { return nil }

func setupMemoryStore(t *testing.T) *InMemoryStore {
	t.Helper()
	l := logger.NewZapLogger()
	cfg := config.NewInMemoryConfig()
	return NewInMemoryStore("", cfg, l)
}

func TestMemoryPushAndPop(t *testing.T) {
	store := setupMemoryStore(t)

	q := "mem_q_push"
	tj := &TestJob{ID: "m1", Data: "hello"}
	if err := store.Push(q, tj); err != nil {
		t.Fatalf("Push error: %v", err)
	}

	jc, err := store.Pop(q)
	if err != nil {
		t.Fatalf("Pop error: %v", err)
	}
	if jc.Job == nil {
		t.Fatal("expected job from Pop, got nil")
	}
	got, ok := jc.Job.(*TestJob)
	if !ok {
		t.Fatalf("expected *TestJob, got %T", jc.Job)
	}
	if got.ID != "m1" || got.Data != "hello" {
		t.Fatalf("job data mismatch: %+v", got)
	}

	// Ack should succeed
	if err := store.Ack(q, jc.JobID); err != nil {
		t.Fatalf("Ack error: %v", err)
	}
	// Ack again should fail
	if err := store.Ack(q, jc.JobID); err == nil {
		t.Fatalf("expected error acknowledging already-acked job")
	}
}

func TestMemoryPushBatchAndPop(t *testing.T) {
	store := setupMemoryStore(t)
	q := "mem_q_batch"
	jobs := []job.Job{
		&TestJob{ID: "b1", Data: "one"},
		&TestJob{ID: "b2", Data: "two"},
	}
	if err := store.PushBatch(q, jobs); err != nil {
		t.Fatalf("PushBatch error: %v", err)
	}

	jc1, err := store.Pop(q)
	if err != nil {
		t.Fatalf("Pop1 error: %v", err)
	}
	jc2, err := store.Pop(q)
	if err != nil {
		t.Fatalf("Pop2 error: %v", err)
	}
	if jc1.Job == nil || jc2.Job == nil {
		t.Fatalf("expected two jobs from Pop calls")
	}
}

func TestMemoryPopMissingQueue(t *testing.T) {
	store := setupMemoryStore(t)
	if _, err := store.Pop("no_such_q"); err == nil {
		t.Fatalf("expected error when popping missing queue")
	}
}

func TestMemoryAckInvalid(t *testing.T) {
	store := setupMemoryStore(t)
	q := "mem_q_ack_invalid"
	// initialize queue by pushing then popping to create processing map
	_ = store.Push(q, &TestJob{ID: "x", Data: "x"})
	jc, err := store.Pop(q)
	if err != nil {
		t.Fatalf("setup Pop error: %v", err)
	}
	// ack a wrong id
	if err := store.Ack(q, "wrong-id"); err == nil {
		t.Fatalf("expected error when acking invalid job id")
	}
	// cleanup
	if err := store.Ack(q, jc.JobID); err != nil {
		t.Fatalf("expected to cleanup valid job, got: %v", err)
	}
}

func TestMemoryRetryNil(t *testing.T) {
	store := setupMemoryStore(t)
	if err := store.Retry(nil, 10*time.Millisecond); err == nil {
		t.Fatalf("expected error when retrying nil job")
	}
}

func TestMemoryEnqueueDequeueMetrics(t *testing.T) {
	store := setupMemoryStore(t)
	q := "metrics_q"
	m := config.JobMetrics{
		QueueName: q,
		JobID:     "mm1",
		Duration:  10 * time.Millisecond,
		Timestamp: time.Now(),
	}
	if err := store.EnqueueMetrics(m); err != nil {
		t.Fatalf("EnqueueMetrics error: %v", err)
	}
	got, err := store.DequeueMetrics(q)
	if err != nil {
		t.Fatalf("DequeueMetrics error: %v", err)
	}
	if got.JobID != m.JobID || got.QueueName != m.QueueName {
		t.Fatalf("metrics mismatch. want=%+v got=%+v", m, got)
	}
}

func TestMemoryIsHealthy(t *testing.T) {
	store := setupMemoryStore(t)
	if !store.IsHealthy() {
		t.Fatalf("expected IsHealthy true")
	}
}

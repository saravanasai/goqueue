package memory

import (
	"testing"
	"time"

	"github.com/saravanasai/goqueue/config"
	"github.com/saravanasai/goqueue/job"
)

func TestMemoryIntegrationPushPopAck(t *testing.T) {
	store := setupMemoryStore(t)
	q := "integration_q_ppa"

	pushJob := &TestJob{ID: "j1", Data: "hello"}
	if err := store.Push(q, pushJob); err != nil {
		t.Fatalf("Push failed: %v", err)
	}

	jc, err := store.Pop(q)
	if err != nil {
		t.Fatalf("Pop failed: %v", err)
	}
	if jc.Job == nil {
		t.Fatal("expected job from Pop, got nil")
	}
	gotJob, ok := jc.Job.(*TestJob)
	if !ok {
		t.Fatalf("expected *TestJob, got %T", jc.Job)
	}
	if gotJob.ID != "j1" || gotJob.Data != "hello" {
		t.Fatalf("job data mismatch: %+v", gotJob)
	}

	if err := store.Ack(q, jc.JobID); err != nil {
		t.Fatalf("Ack failed: %v", err)
	}
}

func TestMemoryIntegrationPushBatchPopAck(t *testing.T) {
	store := setupMemoryStore(t)
	q := "integration_q_batch"
	jobs := []job.Job{&TestJob{ID: "b1", Data: "one"}, &TestJob{ID: "b2", Data: "two"}}
	if err := store.PushBatch(q, jobs); err != nil {
		t.Fatalf("PushBatch failed: %v", err)
	}

	jc1, err := store.Pop(q)
	if err != nil {
		t.Fatalf("Pop1 failed: %v", err)
	}
	jc2, err := store.Pop(q)
	if err != nil {
		t.Fatalf("Pop2 failed: %v", err)
	}
	if jc1.Job == nil || jc2.Job == nil {
		t.Fatal("expected two jobs from Pop calls")
	}

	if err := store.Ack(q, jc1.JobID); err != nil {
		t.Fatalf("Ack1 failed: %v", err)
	}
	if err := store.Ack(q, jc2.JobID); err != nil {
		t.Fatalf("Ack2 failed: %v", err)
	}
}

func TestMemoryIntegrationEnqueueDequeueMetrics(t *testing.T) {
	store := setupMemoryStore(t)
	q := "integration_q_metrics"
	m := config.JobMetrics{QueueName: q, JobID: "mm1", Duration: 10 * time.Millisecond, Timestamp: time.Now()}
	if err := store.EnqueueMetrics(m); err != nil {
		t.Fatalf("EnqueueMetrics failed: %v", err)
	}
	got, err := store.DequeueMetrics(q)
	if err != nil {
		t.Fatalf("DequeueMetrics failed: %v", err)
	}
	if got.JobID != m.JobID || got.QueueName != m.QueueName {
		t.Fatalf("metrics mismatch. want=%+v got=%+v", m, got)
	}
}

func TestMemoryIntegrationIsHealthy(t *testing.T) {
	store := setupMemoryStore(t)
	if !store.IsHealthy() {
		t.Fatalf("expected IsHealthy true")
	}
}

func TestMemoryIntegrationPushWithDelay(t *testing.T) {
	store := setupMemoryStore(t)
	q := "integration_q_delay"
	tj := &TestJob{ID: "d1", Data: "delayed"}
	delay := 5 * time.Second
	if err := store.Push(q, tj, delay); err != nil {
		t.Fatalf("Push with delay failed: %v", err)
	}
	// Should not be available immediately
	_, err := store.Pop(q)
	if err == nil {
		t.Fatalf("expected no job ready to run immediately after delay push")
	}
	time.Sleep(delay + 100*time.Millisecond)
	jc, err := store.Pop(q)
	if err != nil {
		t.Fatalf("Pop after delay failed: %v", err)
	}
	got, ok := jc.Job.(*TestJob)
	if !ok || got.ID != "d1" {
		t.Fatalf("expected delayed job, got %+v", jc.Job)
	}
	if err := store.Ack(q, jc.JobID); err != nil {
		t.Fatalf("Ack after delay failed: %v", err)
	}
}

func TestMemoryIntegrationPushBatchWithDelay(t *testing.T) {
	store := setupMemoryStore(t)
	q := "integration_q_batch_delay"
	jobs := []job.Job{
		&TestJob{ID: "bd1", Data: "batch1"},
		&TestJob{ID: "bd2", Data: "batch2"},
	}
	delay := 5 * time.Second
	if err := store.PushBatch(q, jobs, delay); err != nil {
		t.Fatalf("PushBatch with delay failed: %v", err)
	}
	// Should not be available immediately
	_, err := store.Pop(q)
	if err == nil {
		t.Fatalf("expected no job ready to run immediately after batch delay push")
	}
	time.Sleep(delay + 100*time.Millisecond)
	jc1, err := store.Pop(q)
	if err != nil {
		t.Fatalf("Pop1 after batch delay failed: %v", err)
	}
	jc2, err := store.Pop(q)
	if err != nil {
		t.Fatalf("Pop2 after batch delay failed: %v", err)
	}
	got1, ok1 := jc1.Job.(*TestJob)
	got2, ok2 := jc2.Job.(*TestJob)
	if !ok1 || !ok2 || got1.ID != "bd1" || got2.ID != "bd2" {
		t.Fatalf("expected batch delayed jobs, got %+v %+v", jc1.Job, jc2.Job)
	}
	if err := store.Ack(q, jc1.JobID); err != nil {
		t.Fatalf("Ack1 after batch delay failed: %v", err)
	}
	if err := store.Ack(q, jc2.JobID); err != nil {
		t.Fatalf("Ack2 after batch delay failed: %v", err)
	}
}

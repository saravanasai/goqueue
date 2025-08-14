package memory

import (
	"testing"
	"time"

	"github.com/saravanasai/goqueue/adapter/utils"
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

func TestMemoryIntegrationRetryRequeues(t *testing.T) {
	store := setupMemoryStore(t)
	rjob := &TestJob{ID: "r1", Data: "retry"}
	qname := utils.GetJobName(rjob)
	if qname == "" {
		qname = "default"
	}
	if err := store.Retry(rjob, 50*time.Millisecond); err != nil {
		t.Fatalf("Retry failed: %v", err)
	}
	// wait a little longer than the retry delay
	time.Sleep(80 * time.Millisecond)
	popped, err := store.Pop(qname)
	if err != nil {
		t.Fatalf("Pop after Retry failed: %v", err)
	}
	if popped.Job == nil {
		t.Fatal("expected job from Pop after Retry")
	}
	if err := store.Ack(qname, popped.JobID); err != nil {
		t.Fatalf("Ack after Retry failed: %v", err)
	}
}

func TestMemoryIntegrationIsHealthy(t *testing.T) {
	store := setupMemoryStore(t)
	if !store.IsHealthy() {
		t.Fatalf("expected IsHealthy true")
	}
}

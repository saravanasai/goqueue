package queue

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/saravanasai/goqueue/config"
	"github.com/saravanasai/goqueue/internal/registry"
	"github.com/saravanasai/goqueue/job"
)

// TestJob implements the job.Job interface for testing
type TestJob struct {
	ID       string
	Data     string
	Executed bool
}

func (j *TestJob) Process(ctx context.Context) error {
	j.Executed = true
	return nil
}

// registerTestJob registers the test job if it's not already registered
func registerTestJob() {
	defer func() {
		// Recover from panic if job is already registered
		if r := recover(); r != nil {
			// Job already registered, this is expected and can be ignored
		}
	}()
	registry.Register("TestQueueJob", func() job.Job {
		return &TestJob{}
	})
}

func TestQueueCreation(t *testing.T) {
	// Register test job
	registerTestJob()

	// Create a memory-based queue for testing
	cfg := config.Config{
		Driver:           config.DriverMemory,
		MaxWorkers:       2,
		ConcurrencyLimit: 10,
	}

	shutdownTimeout := 5 * time.Second
	q, err := NewQueue("test_queue", cfg, shutdownTimeout)
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}

	// Check queue properties
	if q.queueName != "test_queue" {
		t.Errorf("Expected queue name 'test_queue', got '%s'", q.queueName)
	}

	if q.ShutdownTimeout != shutdownTimeout {
		t.Errorf("Expected shutdown timeout %v, got %v", shutdownTimeout, q.ShutdownTimeout)
	}

	// Check health
	if !q.IsHealthy() {
		t.Error("Expected queue to be healthy")
	}

	// Test queue shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = q.Shutdown(ctx)
	if err != nil {
		t.Errorf("Error shutting down queue: %v", err)
	}
}

func TestQueueDispatch(t *testing.T) {
	// Register test job
	registerTestJob()

	// Create a memory-based queue for testing
	cfg := config.Config{
		Driver:           config.DriverMemory,
		MaxWorkers:       2,
		ConcurrencyLimit: 10,
	}

	shutdownTimeout := 5 * time.Second
	q, err := NewQueue(fmt.Sprintf("test_queue_dispatch_%d", time.Now().UnixNano()), cfg, shutdownTimeout)
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}
	defer func() {
		if err := q.Shutdown(context.Background()); err != nil {
			t.Logf("Error during shutdown: %v", err)
		}
	}()

	// Create a test job
	testJob := &TestJob{
		ID:   "test-job-1",
		Data: "test data",
	}

	// Test dispatching a job
	err = q.Dispatch(testJob)
	if err != nil {
		t.Fatalf("Failed to dispatch job: %v", err)
	}

	// Start the queue with workers
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	q.StartWorkers(ctx, 1)

	// Wait briefly for the job to be processed
	time.Sleep(100 * time.Millisecond)
}

func TestQueueBatchDispatch(t *testing.T) {
	// Register test job
	registerTestJob()

	// Create a memory-based queue for testing
	cfg := config.Config{
		Driver:           config.DriverMemory,
		MaxWorkers:       2,
		ConcurrencyLimit: 10,
	}

	shutdownTimeout := 5 * time.Second
	q, err := NewQueue(fmt.Sprintf("test_queue_batch_%d", time.Now().UnixNano()), cfg, shutdownTimeout)
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}
	defer func() {
		if err := q.Shutdown(context.Background()); err != nil {
			t.Logf("Error during shutdown: %v", err)
		}
	}()

	// Create test jobs
	jobs := []job.Job{
		&TestJob{ID: "job1", Data: "data1"},
		&TestJob{ID: "job2", Data: "data2"},
		&TestJob{ID: "job3", Data: "data3"},
	}

	// Test batch dispatching
	err = q.DispatchBatch(jobs)
	if err != nil {
		t.Fatalf("Failed to batch dispatch jobs: %v", err)
	}

	// Start the queue with workers
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	q.StartWorkers(ctx, 1)

	// Wait briefly for jobs to be processed
	time.Sleep(100 * time.Millisecond)
}

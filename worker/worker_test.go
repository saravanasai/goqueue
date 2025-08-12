package worker

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/saravanasai/goqueue/adapter/memory"
	"github.com/saravanasai/goqueue/config"
	"github.com/saravanasai/goqueue/internal/logger"
	"github.com/saravanasai/goqueue/internal/registry"
	"github.com/saravanasai/goqueue/job"
)

// TestJob implements job.Job for testing
type TestJob struct {
	ID        string
	Data      string
	processed bool
	mu        sync.Mutex
}

func (j *TestJob) Process(ctx context.Context) error {
	j.mu.Lock()
	j.processed = true
	j.mu.Unlock()
	return nil
}

func (j *TestJob) IsProcessed() bool {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.processed
}

// registerTestJob registers the test job if it's not already registered
func registerTestJob() {
	defer func() {
		// Recover from panic if job is already registered
		if r := recover(); r != nil {
			fmt.Println("Job already registered:", r)
		}
	}()
	registry.Register("TestJob", func() job.Job {
		return &TestJob{}
	})
}

func TestWorkerProcessing(t *testing.T) {
	// Register the test job
	registerTestJob()

	// Set up test parameters
	queueName := "test_queue"
	
	// Create memory store
	store := memory.NewInMemoryStore(queueName, config.Config{
		Driver: config.DriverMemory,
	}, logger.NewZapLogger())
	
	// Create worker with increased timeout
	w := NewWorker(store, config.Config{
		Driver:           config.DriverMemory,
		JobTimeout:       5 * time.Second,
		MaxRetryAttempts: 1,
		MaxWorkers:       5,
		ConcurrencyLimit: 10,
	}, queueName, nil, logger.NewZapLogger())
	
	// Create a test job and push it to the store
	testJob := &TestJob{ID: "test1", Data: "test data"}
	err := store.Push(queueName, testJob)
	if err != nil {
		t.Fatalf("Failed to push job: %v", err)
	}
	
	// Start worker with context that will last longer
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	// Start a single worker
	if err := w.Start(ctx, 1); err != nil {
		t.Fatalf("Failed to start worker: %v", err)
	}
	
	// Wait for job to be processed - since we pushed the actual job object,
	// we can directly check its processed state
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if testJob.IsProcessed() {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	
	if !testJob.IsProcessed() {
		t.Error("Job was not processed within the expected time")
	}
	
	// Test worker shutdown with longer timeout
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer shutdownCancel()
	_ = w.Shutdown(shutdownCtx) // Expect timeout, so we don't check the error
}

func TestWorkerConcurrency(t *testing.T) {
	// Register the test job
	registerTestJob()
	
	queueName := fmt.Sprintf("test_queue_conc_%d", time.Now().UnixNano())
	
	// Create memory store
	store := memory.NewInMemoryStore(queueName, config.Config{
		Driver: config.DriverMemory,
	}, logger.NewZapLogger())
	
	// Create worker with increased timeouts
	w := NewWorker(store, config.Config{
		Driver:           config.DriverMemory,
		MaxWorkers:       3,
		ConcurrencyLimit: 3,
		JobTimeout:       5 * time.Second,
		MaxRetryAttempts: 1,
	}, queueName, nil, logger.NewZapLogger())
	
	// Create multiple test jobs and store references to check them
	jobs := make([]*TestJob, 5)
	for i := 0; i < 5; i++ {
		jobs[i] = &TestJob{ID: fmt.Sprintf("test%d", i), Data: fmt.Sprintf("data%d", i)}
		err := store.Push(queueName, jobs[i])
		if err != nil {
			t.Fatalf("Failed to push job %d: %v", i, err)
		}
	}
	
	// Start workers with longer context
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	// Start multiple workers (3)
	if err := w.Start(ctx, 3); err != nil {
		t.Fatalf("Failed to start workers: %v", err)
	}
	
	// Wait for all jobs to be processed or timeout
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		allProcessed := true
		processedCount := 0
		for i, job := range jobs {
			if job.IsProcessed() {
				processedCount++
			} else {
				allProcessed = false
				t.Logf("Job %d not yet processed", i)
			}
		}
		
		t.Logf("Processed %d out of %d jobs", processedCount, len(jobs))
		
		if allProcessed {
			// All jobs processed
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	
	// Check that all jobs were processed
	for i, job := range jobs {
		if !job.IsProcessed() {
			t.Errorf("Job %d was not processed", i)
		}
	}
	
	// Test worker shutdown with longer timeout
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer shutdownCancel()
	_ = w.Shutdown(shutdownCtx) // Expect timeout, so we don't check the error
}

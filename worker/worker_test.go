package worker

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/danish-a1/goqueue/adapter/memory"
	"github.com/danish-a1/goqueue/config"
	"github.com/danish-a1/goqueue/internal/logger"
	"github.com/danish-a1/goqueue/internal/registry"
	"github.com/danish-a1/goqueue/job"
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
	registerTestJob()
	queueName := "test_queue"
	store := memory.NewInMemoryStore(queueName, config.Config{
		Driver: config.DriverMemory,
	}, logger.NewZapLogger())
	w := NewWorker(store, config.Config{
		Driver:           config.DriverMemory,
		JobTimeout:       5 * time.Second,
		MaxRetryAttempts: 1,
		MaxWorkers:       5,
		ConcurrencyLimit: 10,
	}, queueName, nil, logger.NewZapLogger())
	testJob := &TestJob{ID: "test1", Data: "test data"}
	err := store.Push(queueName, testJob)
	if err != nil {
		t.Fatalf("Failed to push job: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := w.Start(ctx, 1); err != nil {
		t.Fatalf("Failed to start worker: %v", err)
	}
	processed := false
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if testJob.IsProcessed() {
			processed = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !processed {
		t.Error("Job was not processed within the expected time")
	}
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer shutdownCancel()
	if err := w.Shutdown(shutdownCtx); err != nil {
		t.Logf("Shutdown error: %v", err)
	}
}

func TestWorkerConcurrency(t *testing.T) {
	registerTestJob()
	queueName := fmt.Sprintf("test_queue_conc_%d", time.Now().UnixNano())
	store := memory.NewInMemoryStore(queueName, config.Config{
		Driver: config.DriverMemory,
	}, logger.NewZapLogger())
	w := NewWorker(store, config.Config{
		Driver:           config.DriverMemory,
		MaxWorkers:       3,
		ConcurrencyLimit: 3,
		JobTimeout:       5 * time.Second,
		MaxRetryAttempts: 1,
	}, queueName, nil, logger.NewZapLogger())
	jobs := make([]*TestJob, 5)
	for i := 0; i < 5; i++ {
		jobs[i] = &TestJob{ID: fmt.Sprintf("test%d", i), Data: fmt.Sprintf("data%d", i)}
		err := store.Push(queueName, jobs[i])
		if err != nil {
			t.Fatalf("Failed to push job %d: %v", i, err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := w.Start(ctx, 3); err != nil {
		t.Fatalf("Failed to start workers: %v", err)
	}
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
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	for i, job := range jobs {
		if !job.IsProcessed() {
			t.Errorf("Job %d was not processed", i)
		}
	}
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer shutdownCancel()
	if err := w.Shutdown(shutdownCtx); err != nil {
		t.Logf("Shutdown error: %v", err)
	}
}

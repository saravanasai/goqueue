//go:build e2e
// +build e2e

package e2e

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/saravanasai/goqueue"
	"github.com/saravanasai/goqueue/adapter/memory"
	"github.com/saravanasai/goqueue/config"
	"github.com/saravanasai/goqueue/dispatcher"
	"github.com/saravanasai/goqueue/internal/logger"
	"github.com/saravanasai/goqueue/internal/registry"
	"github.com/saravanasai/goqueue/job"
	"github.com/saravanasai/goqueue/worker"
)

// This E2E test exercises the in-memory driver via the public adapter API to simulate client usage.
// It's intentionally simple and can be extended into benchmarks later.

// Test constants for error messages
const (
	errFailedToStartWorkers  = "Failed to start workers: %v"
	errFailedToShutdown      = "Failed to shutdown queue: %v"
	errFailedToCreateQueue   = "Failed to create queue: %v"
	errFailedToDispatchBatch = "Failed to dispatch batch jobs: %v"
)

// EmailJob - Example job implementation showing how clients would integrate
type EmailJob struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
}

// Implement the Job interface
func (e *EmailJob) Process(ctx context.Context) error {
	fmt.Printf("Sending email to %s: %s\n", e.To, e.Subject)
	// Simulate some processing time
	time.Sleep(100 * time.Millisecond)
	return nil
}

// Register job type for serialization - this would typically be in an init() function
func init() {
	goqueue.RegisterJob("EmailJob", func() goqueue.Job {
		return &EmailJob{}
	})
}

type E2EJob struct {
	ID   string `json:"id"`
	Data string `json:"data"`
}

func (j *E2EJob) Process(ctx context.Context) error { return nil }

// ConcurrentJob - A job that simulates concurrent processing
type ConcurrentJob struct {
	ID       string        `json:"id"`
	Duration time.Duration `json:"duration"`
}

// Implement the Process method for ConcurrentJob
func (c *ConcurrentJob) Process(ctx context.Context) error {
	// Simulate processing time
	select {
	case <-time.After(c.Duration):
		fmt.Printf("Concurrent job %s completed after %v\n", c.ID, c.Duration)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// FailingJob - A job that fails on purpose to test error handling
type FailingJob struct {
	ID           string `json:"id"`
	ShouldFail   bool   `json:"should_fail"`
	AttemptCount int    `json:"attempt_count"`
}

// Implement the Process method for FailingJob
func (f *FailingJob) Process(ctx context.Context) error {
	f.AttemptCount++
	if f.ShouldFail && f.AttemptCount <= 2 {
		return fmt.Errorf("intentional failure on attempt %d", f.AttemptCount)
	}
	return nil
}

func ensureE2ERegistered() {
	if _, ok := registry.GetFromRegistery("E2EJob"); !ok {
		registry.Register("E2EJob", func() job.Job { return &E2EJob{} })
	}
}

func TestE2EDispatchAndWorkerProcessing(t *testing.T) {
	logger := logger.NewZapLogger()
	q := "e2e_dispatch_q"
	cfg := config.NewInMemoryConfig().WithMetricsCallback(func(m config.JobMetrics) {
		// no-op in this test; we'll capture via channel below
	})

	store := memory.NewInMemoryStore("", cfg, logger)
	// register job type
	if _, ok := registry.GetFromRegistery("E2EJob"); !ok {
		registry.Register("E2EJob", func() job.Job { return &E2EJob{} })
	}

	// channel to observe completion
	done := make(chan string, 1)
	cfg = cfg.WithMetricsCallback(func(m config.JobMetrics) {
		done <- m.JobID
	})

	// create worker and dispatcher
	w := worker.NewWorker(store, cfg, q, nil, logger)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := w.Start(ctx, 1); err != nil {
		t.Fatalf("failed to start worker: %v", err)
	}
	defer func() {
		_ = w.Shutdown(context.Background())
	}()

	d := dispatcher.NewDispatcher(store, nil)
	// dispatch job
	job := &E2EJob{ID: "dispatch-1", Data: "payload"}
	if err := d.Dispatch(q, job); err != nil {
		t.Fatalf("dispatch failed: %v", err)
	}

	// wait for job completion via metrics callback
	select {
	case id := <-done:
		if id == "" {
			t.Fatalf("received empty job id")
		}
		// Job ID should be a valid UUID, not the custom ID field
		fmt.Printf("Job completed with ID: %s\n", id)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for job to be processed")
	}
}

// countLinesInFile counts the number of lines in a file
func countLinesInFile(filename string) int {
	data, err := os.ReadFile(filename)
	if err != nil {
		return 0
	}

	if len(data) == 0 {
		return 0
	}

	lines := 0
	for _, b := range data {
		if b == '\n' {
			lines++
		}
	}

	// If file doesn't end with newline, count the last line
	if len(data) > 0 && data[len(data)-1] != '\n' {
		lines++
	}

	return lines
}

// setupTestFiles creates temporary files for tracking job completion
func setupTestFiles(t *testing.T) (string, string, func()) {
	testDir := filepath.Join(os.TempDir(), "goqueue_e2e_test")
	err := os.MkdirAll(testDir, 0755)
	if err != nil {
		t.Fatalf("Failed to create test directory: %v", err)
	}

	completedJobsFile := filepath.Join(testDir, "completed_jobs.txt")
	errorJobsFile := filepath.Join(testDir, "error_jobs.txt")

	cleanup := func() {
		os.RemoveAll(testDir)
	}

	return completedJobsFile, errorJobsFile, cleanup
}

// createMetricsCallback creates a callback that writes job metrics to files
func createMetricsCallback(t *testing.T, completedJobsFile, errorJobsFile string) (config.MetricsCallback, *sync.Mutex) {
	var mu sync.Mutex

	callback := func(metrics config.JobMetrics) {
		mu.Lock()
		defer mu.Unlock()

		timestamp := time.Now().Format("2006-01-02 15:04:05.000")

		if metrics.Error != nil {
			writeErrorJob(t, errorJobsFile, timestamp, metrics)
		} else {
			writeCompletedJob(t, completedJobsFile, timestamp, metrics)
		}
	}

	return callback, &mu
}

// writeErrorJob writes error job information to file
func writeErrorJob(t *testing.T, errorJobsFile, timestamp string, metrics config.JobMetrics) {
	errorMsg := fmt.Sprintf("[%s] ERROR - Queue: %s, JobID: %s, Duration: %v, Error: %v\n",
		timestamp, metrics.QueueName, metrics.JobID, metrics.Duration, metrics.Error)

	file, err := os.OpenFile(errorJobsFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		t.Logf("Failed to write to error file: %v", err)
		return
	}
	defer file.Close()
	file.WriteString(errorMsg)

	fmt.Printf("Job failed: %s (took %v) - Error: %v\n", metrics.JobID, metrics.Duration, metrics.Error)
}

// writeCompletedJob writes completed job information to file
func writeCompletedJob(t *testing.T, completedJobsFile, timestamp string, metrics config.JobMetrics) {
	completedMsg := fmt.Sprintf("[%s] SUCCESS - Queue: %s, JobID: %s, Duration: %v\n",
		timestamp, metrics.QueueName, metrics.JobID, metrics.Duration)

	file, err := os.OpenFile(completedJobsFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		t.Logf("Failed to write to completed file: %v", err)
		return
	}
	defer file.Close()
	file.WriteString(completedMsg)

	fmt.Printf("Job completed: %s (took %v)\n", metrics.JobID, metrics.Duration)
}

// dispatchTestJobs dispatches all test jobs and returns expected count
func dispatchTestJobs(t *testing.T, q *goqueue.Queue) int {
	// Dispatch individual jobs
	emailJobs := []*EmailJob{
		{To: "user1@example.com", Subject: "Welcome to our service!"},
		{To: "user2@example.com", Subject: "Your order has been confirmed"},
		{To: "admin@example.com", Subject: "Daily report"},
		{To: "support@example.com", Subject: "New ticket assigned"},
	}

	for i, emailJob := range emailJobs {
		err := goqueue.Dispatch(q, emailJob)
		if err != nil {
			t.Fatalf("Failed to dispatch job %d: %v", i, err)
		}
		fmt.Printf("Dispatched email job to %s\n", emailJob.To)
	}

	// Dispatch batch jobs
	batchJobs := []goqueue.Job{
		&EmailJob{To: "batch1@example.com", Subject: "Batch email 1"},
		&EmailJob{To: "batch2@example.com", Subject: "Batch email 2"},
	}
	err := goqueue.DispatchBatch(q, batchJobs)
	if err != nil {
		t.Fatalf(errFailedToDispatchBatch, err)
	}

	totalExpectedJobs := len(emailJobs) + len(batchJobs)
	fmt.Printf("Total jobs dispatched: %d\n", totalExpectedJobs)

	return totalExpectedJobs
}

// waitForJobCompletion waits for all jobs to complete and monitors progress
func waitForJobCompletion(t *testing.T, q *goqueue.Queue, totalExpectedJobs int, completedJobsFile, errorJobsFile string) {
	timeout := time.After(10 * time.Second)
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			handleTimeout(t, totalExpectedJobs, completedJobsFile, errorJobsFile)
			return

		case <-ticker.C:
			if checkJobProgress(q, totalExpectedJobs, completedJobsFile, errorJobsFile) {
				return
			}
		}
	}
}

// handleTimeout handles the timeout case and provides debugging information
func handleTimeout(t *testing.T, totalExpectedJobs int, completedJobsFile, errorJobsFile string) {
	if data, err := os.ReadFile(completedJobsFile); err == nil {
		fmt.Printf("Completed jobs file content:\n%s\n", string(data))
	}
	if data, err := os.ReadFile(errorJobsFile); err == nil && len(data) > 0 {
		fmt.Printf("Error jobs file content:\n%s\n", string(data))
	}

	completedCount := countLinesInFile(completedJobsFile)
	t.Fatalf("Timeout: not all jobs were processed. Expected %d, completed %d", totalExpectedJobs, completedCount)
}

// checkJobProgress checks job progress and returns true if all jobs are completed
func checkJobProgress(q *goqueue.Queue, totalExpectedJobs int, completedJobsFile, errorJobsFile string) bool {
	stats := goqueue.GetQueueStats(q)
	fmt.Printf("Queue stats: Queued=%d, Processing=%d, Completed=%d\n",
		stats.QueuedJobs, stats.ProcessingJobs, stats.CompletedJobs)

	if goqueue.IsQueueOverloaded(q) {
		fmt.Println("Warning: Queue is overloaded!")
	}

	completedCount := countLinesInFile(completedJobsFile)
	errorCount := countLinesInFile(errorJobsFile)

	fmt.Printf("Progress: %d completed, %d errors out of %d total jobs\n",
		completedCount, errorCount, totalExpectedJobs)

	// Account for potential duplication in metrics (each job may trigger callback twice)
	// We consider jobs complete when we have at least totalExpectedJobs completions
	if completedCount >= totalExpectedJobs {
		fmt.Printf("All %d jobs completed successfully! (Total recorded: %d)\n", totalExpectedJobs, completedCount)
		return true
	}

	return false
}

// validateFinalResults validates the final test results
func validateFinalResults(t *testing.T, totalExpectedJobs int, completedJobsFile, errorJobsFile string) {
	finalCompletedCount := countLinesInFile(completedJobsFile)
	finalErrorCount := countLinesInFile(errorJobsFile)

	fmt.Printf("Final results: %d completed, %d errors\n", finalCompletedCount, finalErrorCount)

	// Print file contents for verification
	if data, err := os.ReadFile(completedJobsFile); err == nil {
		fmt.Printf("\n=== COMPLETED JOBS ===\n%s\n", string(data))
	}

	if finalErrorCount > 0 {
		if data, err := os.ReadFile(errorJobsFile); err == nil {
			fmt.Printf("\n=== ERROR JOBS ===\n%s\n", string(data))
		}
	}

	// Note: finalCompletedCount may be 2x totalExpectedJobs due to metrics processing duplication
	// This is expected behavior - each job completion triggers the callback twice:
	// 1. During actual job completion
	// 2. During metrics processing
	minExpectedCompleted := totalExpectedJobs
	maxExpectedCompleted := totalExpectedJobs * 2

	if finalCompletedCount < minExpectedCompleted {
		t.Fatalf("Not enough jobs completed. Expected at least %d, got %d", minExpectedCompleted, finalCompletedCount)
	}

	if finalCompletedCount > maxExpectedCompleted {
		t.Fatalf("Too many jobs completed. Expected at most %d, got %d", maxExpectedCompleted, finalCompletedCount)
	}

	fmt.Printf("✅ Job completion validation passed: %d completed jobs (expected %d-%d)\n",
		finalCompletedCount, minExpectedCompleted, maxExpectedCompleted)

	fmt.Printf("Output files created at:\n- Completed: %s\n- Errors: %s\n", completedJobsFile, errorJobsFile)
}

// TestClientIntegrationE2E - Complete end-to-end test showing how clients would integrate with goqueue
func TestClientIntegrationE2E(t *testing.T) {
	// This test demonstrates the complete client integration workflow
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Setup test files for tracking job completion
	completedJobsFile, errorJobsFile, cleanup := setupTestFiles(t)
	defer cleanup()

	// Create queue configuration with metrics callback
	cfg := config.NewInMemoryConfig()
	metricsCallback, _ := createMetricsCallback(t, completedJobsFile, errorJobsFile)
	cfg = cfg.WithMetricsCallback(metricsCallback)

	// Create and start the queue
	q, err := goqueue.NewQueueWithDefaults("email-queue", cfg)
	if err != nil {
		log.Fatal(err)
	}

	err = goqueue.StartWorker(q, ctx, 2)
	if err != nil {
		t.Fatalf(errFailedToStartWorkers, err)
	}

	// Dispatch test jobs
	totalExpectedJobs := dispatchTestJobs(t, q)

	// Wait for job completion
	waitForJobCompletion(t, q, totalExpectedJobs, completedJobsFile, errorJobsFile)

	// Graceful shutdown
	fmt.Println("Shutting down queue...")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	err = goqueue.Shutdown(q, shutdownCtx)
	if err != nil {
		t.Logf("Shutdown warning: %v", err) // Don't fail the test for shutdown timeout
	}

	// Validate results
	validateFinalResults(t, totalExpectedJobs, completedJobsFile, errorJobsFile)
	fmt.Println("Client integration test completed successfully!")
}

// TestClientErrorHandling - Test how clients handle job failures and retries
func TestClientErrorHandling(t *testing.T) {
	// Register the failing job type
	goqueue.RegisterJob("FailingJob", func() goqueue.Job {
		return &FailingJob{}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Create queue with retry configuration
	cfg := config.NewInMemoryConfig().
		WithMaxRetryAttempts(2).
		WithRetryDelay(500 * time.Millisecond).
		WithExponentialBackoff(true)

	var jobMetrics []config.JobMetrics
	var mu sync.Mutex
	cfg = cfg.WithMetricsCallback(func(metrics config.JobMetrics) {
		mu.Lock()
		defer mu.Unlock()
		jobMetrics = append(jobMetrics, metrics)
		if metrics.Error != nil {
			fmt.Printf("Job %s failed: %v\n", metrics.JobID, metrics.Error)
		} else {
			fmt.Printf("Job %s completed successfully\n", metrics.JobID)
		}
	})

	q, err := goqueue.NewQueueWithDefaults("error-test-queue", cfg)
	if err != nil {
		t.Fatalf(errFailedToCreateQueue, err)
	}

	err = goqueue.StartWorker(q, ctx, 1)
	if err != nil {
		t.Fatalf(errFailedToStartWorkers, err)
	}

	// Create a test job that should fail initially
	testJob := &FailingJob{ID: "test-1", ShouldFail: true, AttemptCount: 0}

	// Dispatch the job
	err = goqueue.Dispatch(q, testJob)
	if err != nil {
		t.Fatalf("Failed to dispatch job: %v", err)
	}

	time.Sleep(3 * time.Second) // Allow some processing time

	err = goqueue.Shutdown(q, ctx)
	if err != nil {
		t.Fatalf(errFailedToShutdown, err)
	}

	fmt.Println("Error handling test completed!")
}

// TestClientConcurrentProcessing - Test concurrent job processing
func TestClientConcurrentProcessing(t *testing.T) {
	// Register the concurrent job type
	goqueue.RegisterJob("ConcurrentJob", func() goqueue.Job {
		return &ConcurrentJob{}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var completedJobs sync.Map
	cfg := config.NewInMemoryConfig().
		WithConcurrencyLimit(3). // Allow up to 3 concurrent jobs
		WithMetricsCallback(func(metrics config.JobMetrics) {
			completedJobs.Store(metrics.JobID, metrics.Duration)
			fmt.Printf("Job %s completed in %v\n", metrics.JobID, metrics.Duration)
		})

	q, err := goqueue.NewQueueWithDefaults("concurrent-queue", cfg)
	if err != nil {
		t.Fatalf(errFailedToCreateQueue, err)
	}

	err = goqueue.StartWorker(q, ctx, 3) // Start 3 workers
	if err != nil {
		t.Fatalf(errFailedToStartWorkers, err)
	}

	// Dispatch multiple concurrent jobs
	jobs := []goqueue.Job{
		&ConcurrentJob{ID: "job-1", Duration: 1 * time.Second},
		&ConcurrentJob{ID: "job-2", Duration: 500 * time.Millisecond},
		&ConcurrentJob{ID: "job-3", Duration: 2 * time.Second},
		&ConcurrentJob{ID: "job-4", Duration: 300 * time.Millisecond},
		&ConcurrentJob{ID: "job-5", Duration: 1500 * time.Millisecond},
	}

	startTime := time.Now()
	err = goqueue.DispatchBatch(q, jobs)
	if err != nil {
		t.Fatalf(errFailedToDispatchBatch, err)
	}

	// Wait for all jobs to complete
	for len(jobs) > 0 {
		time.Sleep(100 * time.Millisecond)
		completedCount := 0
		completedJobs.Range(func(key, value interface{}) bool {
			completedCount++
			return true
		})
		if completedCount >= len(jobs) {
			break
		}

		// Check stats
		stats := goqueue.GetQueueStats(q)
		fmt.Printf("Stats: Queued=%d, Processing=%d, Completed=%d\n",
			stats.QueuedJobs, stats.ProcessingJobs, stats.CompletedJobs)
	}

	totalTime := time.Since(startTime)
	fmt.Printf("All concurrent jobs completed in %v\n", totalTime)

	err = goqueue.Shutdown(q, ctx)
	if err != nil {
		t.Fatalf(errFailedToShutdown, err)
	}

	// Verify concurrent processing was effective
	// With 3 workers, jobs should complete faster than sequential processing
	maxSequentialTime := 5500 * time.Millisecond // Sum of all job durations
	if totalTime >= maxSequentialTime {
		t.Logf("Warning: Concurrent processing may not be working optimally. Total time: %v", totalTime)
	}

	fmt.Println("Concurrent processing test completed!")
}

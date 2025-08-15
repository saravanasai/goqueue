package e2e

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/saravanasai/goqueue"
	"github.com/saravanasai/goqueue/config"
)

// getSQSTestConfig returns SQS configuration from environment variables
// Required environment variables:
//   - SQS_TEST_QUEUE_URL: The URL of the SQS queue for testing
//   - SQS_TEST_REGION: AWS region (e.g., "ap-south-1")
//   - SQS_TEST_ACCESS_KEY_ID: AWS access key ID
//   - SQS_TEST_SECRET_ACCESS_KEY: AWS secret access key
func getSQSTestConfig() (queueURL, region, accessKeyID, secretAccessKey string, ok bool) {
	queueURL = os.Getenv("SQS_TEST_QUEUE_URL")
	region = os.Getenv("SQS_TEST_REGION")
	accessKeyID = os.Getenv("SQS_TEST_ACCESS_KEY_ID")
	secretAccessKey = os.Getenv("SQS_TEST_SECRET_ACCESS_KEY")

	if queueURL == "" || region == "" || accessKeyID == "" || secretAccessKey == "" {
		return "", "", "", "", false
	}

	return queueURL, region, accessKeyID, secretAccessKey, true
}

// SQSEmailJob - Example job implementation for SQS e2e testing
type SQSEmailJob struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
	Content string `json:"content"`
	JobID   string `json:"job_id"`
}

// Global tracking for processed jobs
var (
	processedJobs  sync.Map
	processedCount int64
)

// Implement the Job interface
func (e *SQSEmailJob) Process(ctx context.Context) error {
	fmt.Printf("Processing SQS email to %s: %s (JobID: %s)\n", e.To, e.Subject, e.JobID)

	// Track processed job
	processedJobs.Store(e.JobID, true)
	atomic.AddInt64(&processedCount, 1)

	// Simulate some processing time
	time.Sleep(100 * time.Millisecond)
	return nil
}

// Register job type for serialization
func init() {
	goqueue.RegisterJob("SQSEmailJob", func() goqueue.Job {
		return &SQSEmailJob{}
	})
}

// createSQSConfig creates a basic SQS config with a no-op metrics callback
func createSQSConfig() config.Config {
	queueURL, region, accessKeyID, secretAccessKey, ok := getSQSTestConfig()
	if !ok {
		panic("SQS test configuration not available. Please set SQS_TEST_* environment variables.")
	}

	return config.NewSQSConfig(
		queueURL,
		region,
		accessKeyID,
		secretAccessKey,
	).WithMetricsCallback(func(metrics config.JobMetrics) {
		// No-op metrics callback to avoid nil pointer issues
		// Actual job tracking is done in the Process method
	})
}

// TestSQSQueueIntegration - Comprehensive e2e test for SQS driver
func TestSQSQueueIntegration(t *testing.T) {
	// Skip test if running in CI or if SQS credentials are not available
	if testing.Short() {
		t.Skip("Skipping SQS integration test in short mode")
	}

	// Check if SQS configuration is available
	_, _, _, _, ok := getSQSTestConfig()
	if !ok {
		t.Skip("Skipping SQS integration test - SQS_TEST_* environment variables not set")
	}

	// Reset global counters
	processedJobs = sync.Map{}
	atomic.StoreInt64(&processedCount, 0)

	// Create SQS config
	cfg := createSQSConfig()

	// Create new queue with SQS config
	queue, err := goqueue.NewQueueWithDefaults("sqs-test-queue", cfg)
	if err != nil {
		t.Fatalf("Failed to create SQS queue: %v", err)
	}

	// Start workers
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	workerCount := 2 // Use fewer workers for SQS to avoid rate limiting
	err = goqueue.StartWorker(queue, ctx, workerCount)
	if err != nil {
		t.Fatalf("Failed to start SQS workers: %v", err)
	}

	// Create test jobs for SQS
	testJobs := []*SQSEmailJob{
		{To: "sqs-user1@example.com", Subject: "SQS Welcome", Content: "Welcome to SQS queue!", JobID: "sqs-job-1"},
		{To: "sqs-user2@example.com", Subject: "SQS Order Confirmation", Content: "Your order has been confirmed.", JobID: "sqs-job-2"},
		{To: "sqs-user3@example.com", Subject: "SQS Newsletter", Content: "Monthly newsletter from SQS queue.", JobID: "sqs-job-3"},
		{To: "sqs-admin@example.com", Subject: "SQS Report", Content: "Daily SQS performance report.", JobID: "sqs-job-4"},
		{To: "sqs-support@example.com", Subject: "SQS Ticket Update", Content: "Your support ticket has been updated.", JobID: "sqs-job-5"},
	}

	expectedJobCount := len(testJobs)
	fmt.Printf("🚀 Dispatching %d SQS jobs to %d workers...\n", expectedJobCount, workerCount)

	// Dispatch individual jobs with delay to respect SQS rate limits
	for i, job := range testJobs {
		err := goqueue.Dispatch(queue, job)
		if err != nil {
			t.Fatalf("Failed to dispatch SQS job %d: %v", i+1, err)
		}
		// Add delay between dispatches to respect SQS rate limits
		time.Sleep(200 * time.Millisecond)
	}

	// Wait for all jobs to complete with timeout
	timeout := time.After(25 * time.Second)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	fmt.Println("⏳ Waiting for SQS jobs to complete...")

	for {
		select {
		case <-timeout:
			executed := atomic.LoadInt64(&processedCount)
			t.Fatalf("Timeout: Expected %d jobs, executed %d jobs", expectedJobCount, executed)
		case <-ticker.C:
			executed := atomic.LoadInt64(&processedCount)
			fmt.Printf("Progress: %d/%d SQS jobs executed\n", executed, expectedJobCount)

			if executed >= int64(expectedJobCount) {
				fmt.Printf("All %d SQS jobs completed!\n", expectedJobCount)
				goto completed
			}
		}
	}

completed:
	// Verify all jobs were processed
	finalExecuted := atomic.LoadInt64(&processedCount)

	// Verify each job was processed
	for _, job := range testJobs {
		if _, exists := processedJobs.Load(job.JobID); !exists {
			t.Errorf("Job %s was not processed", job.JobID)
		}
	}

	// Check queue stats to verify jobs were processed
	stats := queue.Stats()
	fmt.Printf("📊 SQS Queue Stats - Healthy: %v\n", stats.IsHealthy)

	// Verify queue is healthy
	if !queue.IsHealthy() {
		t.Fatalf("SQS queue is not healthy")
	}

	// Cancel workers context to stop processing
	cancel()

	// Wait a moment for workers to stop
	time.Sleep(500 * time.Millisecond)

	// Shutdown queue with extended timeout for SQS
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	err = goqueue.Shutdown(queue, shutdownCtx)
	if err != nil {
		t.Logf("SQS shutdown warning: %v", err)
	}

	// Verify job count
	if finalExecuted < int64(expectedJobCount) {
		t.Fatalf("Not enough SQS jobs executed: expected %d, got %d", expectedJobCount, finalExecuted)
	}

	// Verify queue was healthy during operation
	if !stats.IsHealthy {
		t.Fatalf("SQS queue was not healthy during operation")
	}

	// Print final success message with clear separation
	fmt.Println("\n" + strings.Repeat("=", 70))
	fmt.Printf("✅ SQS TEST PASSED: %d jobs executed successfully\n", finalExecuted)
	fmt.Printf("📊 Expected: %d jobs | Actual: %d jobs | Workers: %d | Queue Healthy: %v\n",
		expectedJobCount, finalExecuted, workerCount, stats.IsHealthy)
	fmt.Printf("🔗 SQS Queue: configured via environment variables\n")
	fmt.Println(strings.Repeat("=", 70))
}

// TestSQSQueueConcurrentDispatch - Test concurrent job dispatching with SQS
func TestSQSQueueConcurrentDispatch(t *testing.T) {
	// Skip test if running in CI or if SQS credentials are not available
	if testing.Short() {
		t.Skip("Skipping SQS concurrent test in short mode")
	}

	// Check if SQS configuration is available
	_, _, _, _, ok := getSQSTestConfig()
	if !ok {
		t.Skip("Skipping SQS concurrent test - SQS_TEST_* environment variables not set")
	}

	// Reset global counters
	processedJobs = sync.Map{}
	atomic.StoreInt64(&processedCount, 0)

	// Create SQS config
	cfg := createSQSConfig()

	// Create queue
	queue, err := goqueue.NewQueueWithDefaults("sqs-concurrent-queue", cfg)
	if err != nil {
		t.Fatalf("Failed to create SQS queue for concurrent test: %v", err)
	}

	// Start workers
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	workerCount := 3 // Limited workers for SQS to avoid rate limiting
	err = goqueue.StartWorker(queue, ctx, workerCount)
	if err != nil {
		t.Fatalf("Failed to start SQS workers for concurrent test: %v", err)
	}

	// Number of jobs to dispatch concurrently (limited for SQS)
	jobCount := 6
	var wg sync.WaitGroup

	fmt.Printf("🚀 Starting concurrent dispatch of %d SQS jobs...\n", jobCount)

	// Dispatch jobs concurrently but with rate limiting for SQS
	semaphore := make(chan struct{}, 3) // Limit concurrent dispatches
	wg.Add(jobCount)

	for i := 0; i < jobCount; i++ {
		go func(jobNum int) {
			defer wg.Done()

			// Acquire semaphore
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			job := &SQSEmailJob{
				To:      fmt.Sprintf("sqs-concurrent-user%d@example.com", jobNum),
				Subject: fmt.Sprintf("SQS Concurrent Job #%d", jobNum),
				Content: fmt.Sprintf("This is SQS concurrent job number %d", jobNum),
				JobID:   fmt.Sprintf("sqs-concurrent-job-%d", jobNum),
			}

			err := goqueue.Dispatch(queue, job)
			if err != nil {
				t.Errorf("Failed to dispatch SQS concurrent job %d: %v", jobNum, err)
			}

			// Small delay to respect SQS rate limits
			time.Sleep(150 * time.Millisecond)
		}(i)
	}

	// Wait for all dispatches to complete
	wg.Wait()
	fmt.Printf("📤 All %d SQS jobs dispatched concurrently\n", jobCount)

	// Wait for all jobs to complete with timeout
	timeout := time.After(35 * time.Second)
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	fmt.Println("⏳ Waiting for concurrent SQS jobs to complete...")

	for {
		select {
		case <-timeout:
			executed := atomic.LoadInt64(&processedCount)
			t.Fatalf("Concurrent test timeout: Expected %d jobs, executed %d jobs", jobCount, executed)
		case <-ticker.C:
			executed := atomic.LoadInt64(&processedCount)
			fmt.Printf("Concurrent Progress: %d/%d SQS jobs executed\n", executed, jobCount)

			if executed >= int64(jobCount) {
				fmt.Printf("All %d concurrent SQS jobs completed!\n", jobCount)
				goto concurrentCompleted
			}
		}
	}

concurrentCompleted:
	// Check queue health
	if !queue.IsHealthy() {
		t.Fatalf("SQS queue is not healthy after concurrent operations")
	}

	// Final validation
	finalExecuted := atomic.LoadInt64(&processedCount)

	// Verify job count
	if finalExecuted < int64(jobCount) {
		t.Fatalf("Not enough concurrent SQS jobs executed: expected %d, got %d", jobCount, finalExecuted)
	}

	fmt.Printf("🎉 All %d concurrent SQS jobs handled successfully!\n", finalExecuted)

	// Cancel workers context first
	cancel()

	// Wait a moment for workers to stop
	time.Sleep(500 * time.Millisecond)

	// Shutdown
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	err = goqueue.Shutdown(queue, shutdownCtx)
	if err != nil {
		t.Logf("SQS concurrent test shutdown warning: %v", err)
	}

	// Print success message
	fmt.Println("\n" + strings.Repeat("=", 70))
	fmt.Printf("✅ SQS CONCURRENT TEST PASSED: %d jobs executed successfully\n", finalExecuted)
	fmt.Printf("📊 Dispatched: %d jobs | Actual: %d jobs | Queue remained healthy throughout\n", jobCount, finalExecuted)
	fmt.Printf("🔗 SQS Queue: configured via environment variables\n")
	fmt.Printf("ℹ️  Note: Rate-limited dispatching used to respect SQS limits\n")
	fmt.Println(strings.Repeat("=", 70))
}

// TestSQSQueueHealthCheck - Test SQS queue health monitoring
func TestSQSQueueHealthCheck(t *testing.T) {
	// Skip test if running in CI
	if testing.Short() {
		t.Skip("Skipping SQS health check test in short mode")
	}

	// Check if SQS configuration is available
	_, _, _, _, ok := getSQSTestConfig()
	if !ok {
		t.Skip("Skipping SQS health check test - SQS_TEST_* environment variables not set")
	}

	// Create SQS config
	cfg := createSQSConfig()

	// Create queue
	queue, err := goqueue.NewQueueWithDefaults("sqs-health-test-queue", cfg)
	if err != nil {
		t.Fatalf("Failed to create SQS queue for health test: %v", err)
	}

	// Test initial health
	if !queue.IsHealthy() {
		t.Fatalf("SQS queue should be healthy upon creation")
	}

	// Get stats
	stats := queue.Stats()
	if !stats.IsHealthy {
		t.Fatalf("SQS queue stats should show healthy status")
	}

	fmt.Printf("✅ SQS Queue health check passed - Status: Healthy\n")
	fmt.Printf("📊 Queue Stats: %+v\n", stats)

	// Test shutdown
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = goqueue.Shutdown(queue, shutdownCtx)
	if err != nil {
		t.Logf("SQS health test shutdown warning: %v", err)
	}

	fmt.Println("🎉 SQS health check test completed successfully!")
}

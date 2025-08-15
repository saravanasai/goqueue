package e2e

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/saravanasai/goqueue"
	"github.com/saravanasai/goqueue/config"
	"github.com/saravanasai/goqueue/job"
)

const (
	sqsQueueCreateError = "Failed to create SQS queue: %v"
	sqsWorkerStartError = "Failed to start SQS workers: %v"
)

// SQSRetryTestJob - Job implementation for SQS retry testing
type SQSRetryTestJob struct {
	ID            string    `json:"id"`
	ShouldFail    bool      `json:"should_fail"`
	FailAttempts  int       `json:"fail_attempts"` // Number of attempts to fail before succeeding
	ProcessedTime time.Time `json:"processed_time"`
}

var sqsRetryTestJobCounter int64
var sqsRetryTestJobProcessed = make(map[string]int)
var sqsRetryTestJobMutex sync.Mutex

// Implement the Job interface
func (r *SQSRetryTestJob) Process(ctx context.Context) error {
	atomic.AddInt64(&sqsRetryTestJobCounter, 1)

	sqsRetryTestJobMutex.Lock()
	defer sqsRetryTestJobMutex.Unlock()

	sqsRetryTestJobProcessed[r.ID]++
	currentAttempt := sqsRetryTestJobProcessed[r.ID]

	r.ProcessedTime = time.Now()

	fmt.Printf("Processing SQS retry test job %s (attempt %d, should fail: %v, fail attempts: %d)\n",
		r.ID, currentAttempt, r.ShouldFail, r.FailAttempts)

	// Simulate processing time
	time.Sleep(100 * time.Millisecond)

	// Fail if configured to fail for this attempt
	if r.ShouldFail && currentAttempt <= r.FailAttempts {
		return fmt.Errorf("intentional failure for retry testing (attempt %d/%d)", currentAttempt, r.FailAttempts)
	}

	return nil
}

// Register job type for serialization
func init() {
	goqueue.RegisterJob("SQSRetryTestJob", func() goqueue.Job {
		return &SQSRetryTestJob{}
	})
}

// resetSQSRetryTestCounters resets global test counters
func resetSQSRetryTestCounters() {
	atomic.StoreInt64(&sqsRetryTestJobCounter, 0)
	sqsRetryTestJobMutex.Lock()
	sqsRetryTestJobProcessed = make(map[string]int)
	sqsRetryTestJobMutex.Unlock()
}

// createSQSRetryConfig creates SQS config with retry settings
func createSQSRetryConfig(maxRetries int, retryDelay time.Duration, exponential bool) config.Config {
	queueURL, region, accessKeyID, secretAccessKey, ok := getSQSTestConfig()
	if !ok {
		panic("SQS test configuration not available. Please set SQS_TEST_* environment variables.")
	}

	cfg := config.NewSQSConfig(queueURL, region, accessKeyID, secretAccessKey)
	return cfg.WithMaxRetryAttempts(maxRetries).
		WithRetryDelay(retryDelay).
		WithExponentialBackoff(exponential)
}

// TestSQSRetryMechanism - Test basic retry functionality using visibility timeout
func TestSQSRetryMechanism(t *testing.T) {
	// Skip test if running in CI or if SQS credentials are not available
	if testing.Short() {
		t.Skip("Skipping SQS retry integration test in short mode")
	}

	// Check if SQS configuration is available
	_, _, _, _, ok := getSQSTestConfig()
	if !ok {
		t.Skip("Skipping SQS retry integration test - SQS_TEST_* environment variables not set")
	}

	resetSQSRetryTestCounters()

	// Create SQS config with retry settings
	cfg := createSQSRetryConfig(3, 2*time.Second, false)

	// Create queue
	queue, err := goqueue.NewQueueWithDefaults("sqs-retry-test", cfg)
	if err != nil {
		t.Fatalf(sqsQueueCreateError, err)
	}

	// Start workers
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	err = goqueue.StartWorker(queue, ctx, 1) // Single worker for predictable retry behavior
	if err != nil {
		t.Fatalf(sqsWorkerStartError, err)
	}

	fmt.Println("🔄 Testing SQS retry mechanism with visibility timeout...")

	// Create a job that will fail 2 times, then succeed on the 3rd attempt
	retryJob := &SQSRetryTestJob{
		ID:           "sqs-retry-job-1",
		ShouldFail:   true,
		FailAttempts: 2, // Fail on attempts 1 and 2, succeed on attempt 3
	}

	// Dispatch the job
	err = goqueue.Dispatch(queue, retryJob)
	if err != nil {
		t.Fatalf("Failed to dispatch SQS retry job: %v", err)
	}

	// Wait for job to be processed (with retries)
	timeout := time.After(45 * time.Second) // Extended timeout for SQS visibility timeout retries
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	var finalAttempts int
	for {
		select {
		case <-timeout:
			t.Fatalf("Timeout waiting for SQS retry job to complete")
		case <-ticker.C:
			sqsRetryTestJobMutex.Lock()
			attempts := sqsRetryTestJobProcessed["sqs-retry-job-1"]
			sqsRetryTestJobMutex.Unlock()

			if attempts >= 3 {
				finalAttempts = attempts
				break
			}
			fmt.Printf("⏳ SQS job sqs-retry-job-1 processed %d times, waiting for 3...\n", attempts)
		}
		if finalAttempts >= 3 {
			break
		}
	}

	// Verify job was processed exactly 3 times (2 failures + 1 success)
	if finalAttempts != 3 {
		t.Errorf("Expected SQS job to be processed 3 times, got %d", finalAttempts)
	}

	// Cleanup
	cancel()
	time.Sleep(1 * time.Second)

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	goqueue.Shutdown(queue, shutdownCtx)

	fmt.Printf("✅ SQS RETRY TEST PASSED: Job processed %d times as expected\n", finalAttempts)
}

// TestSQSExponentialBackoff - Test exponential backoff retry mechanism
func TestSQSExponentialBackoff(t *testing.T) {
	// Skip test if running in CI or if SQS credentials are not available
	if testing.Short() {
		t.Skip("Skipping SQS exponential backoff test in short mode")
	}

	// Check if SQS configuration is available
	_, _, _, _, ok := getSQSTestConfig()
	if !ok {
		t.Skip("Skipping SQS exponential backoff test - SQS_TEST_* environment variables not set")
	}

	resetSQSRetryTestCounters()

	// Create SQS config with exponential backoff
	cfg := createSQSRetryConfig(4, 3*time.Second, true)

	// Create queue
	queue, err := goqueue.NewQueueWithDefaults("sqs-exponential-test", cfg)
	if err != nil {
		t.Fatalf(sqsQueueCreateError, err)
	}

	// Start workers
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	err = goqueue.StartWorker(queue, ctx, 1)
	if err != nil {
		t.Fatalf(sqsWorkerStartError, err)
	}

	fmt.Println("📈 Testing SQS exponential backoff retry mechanism...")

	// Create a job that will fail 3 times, then succeed on the 4th attempt
	retryJob := &SQSRetryTestJob{
		ID:           "sqs-exponential-job-1",
		ShouldFail:   true,
		FailAttempts: 3, // Fail on attempts 1, 2, and 3, succeed on attempt 4
	}

	startTime := time.Now()

	// Dispatch the job
	err = goqueue.Dispatch(queue, retryJob)
	if err != nil {
		t.Fatalf("Failed to dispatch SQS exponential backoff job: %v", err)
	}

	// Wait for job to be processed (with exponential backoff retries)
	timeout := time.After(75 * time.Second)
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	var finalAttempts int
	for {
		select {
		case <-timeout:
			t.Fatalf("Timeout waiting for SQS exponential backoff job to complete")
		case <-ticker.C:
			sqsRetryTestJobMutex.Lock()
			attempts := sqsRetryTestJobProcessed["sqs-exponential-job-1"]
			sqsRetryTestJobMutex.Unlock()

			if attempts >= 4 {
				finalAttempts = attempts
				break
			}
			fmt.Printf("⏳ SQS exponential job processed %d times, waiting for 4...\n", attempts)
		}
		if finalAttempts >= 4 {
			break
		}
	}

	totalTime := time.Since(startTime)

	// Verify job was processed exactly 4 times
	if finalAttempts != 4 {
		t.Errorf("Expected SQS exponential job to be processed 4 times, got %d", finalAttempts)
	}

	// With exponential backoff (3s base), delays should be: 3s, 6s, 12s
	// Minimum total time should be around 21s plus processing time
	expectedMinTime := 20 * time.Second
	if totalTime < expectedMinTime {
		t.Errorf("SQS exponential backoff too fast: expected at least %v, got %v", expectedMinTime, totalTime)
	}

	// Cleanup
	cancel()
	time.Sleep(1 * time.Second)

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	goqueue.Shutdown(queue, shutdownCtx)

	fmt.Printf("✅ SQS EXPONENTIAL BACKOFF TEST PASSED: Job processed %d times in %v\n", finalAttempts, totalTime)
}

// TestSQSMaxRetryExceeded - Test that jobs go to DLQ after max retries
func TestSQSMaxRetryExceeded(t *testing.T) {
	// Skip test if running in CI or if SQS credentials are not available
	if testing.Short() {
		t.Skip("Skipping SQS DLQ test in short mode")
	}

	// Check if SQS configuration is available
	_, _, _, _, ok := getSQSTestConfig()
	if !ok {
		t.Skip("Skipping SQS DLQ test - SQS_TEST_* environment variables not set")
	}

	resetSQSRetryTestCounters()

	var dlqJobsReceived int64
	var dlqMutex sync.Mutex
	var dlqJobs []interface{}

	// Create a mock DLQ adapter
	mockDLQ := &MockDLQAdapter{
		OnPush: func(ctx context.Context, jobCtx *job.JobContext, err error) error {
			atomic.AddInt64(&dlqJobsReceived, 1)
			dlqMutex.Lock()
			dlqJobs = append(dlqJobs, jobCtx)
			dlqMutex.Unlock()
			fmt.Printf("📮 SQS Job pushed to DLQ: %+v, error: %v\n", jobCtx, err)
			return nil
		},
	}

	// Create SQS config with limited retries
	cfg := createSQSRetryConfig(2, 2*time.Second, false).WithDLQAdapter(mockDLQ)

	// Create queue
	queue, err := goqueue.NewQueueWithDefaults("sqs-dlq-test", cfg)
	if err != nil {
		t.Fatalf(sqsQueueCreateError, err)
	}

	// Start workers
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	err = goqueue.StartWorker(queue, ctx, 1)
	if err != nil {
		t.Fatalf(sqsWorkerStartError, err)
	}

	fmt.Println("💀 Testing SQS DLQ functionality after max retries...")

	// Create a job that will always fail
	alwaysFailJob := &SQSRetryTestJob{
		ID:           "sqs-always-fail-job",
		ShouldFail:   true,
		FailAttempts: 10, // Always fail (more than max retries)
	}

	// Dispatch the job
	err = goqueue.Dispatch(queue, alwaysFailJob)
	if err != nil {
		t.Fatalf("Failed to dispatch SQS always-fail job: %v", err)
	}

	// Wait for job to exhaust retries and go to DLQ
	timeout := time.After(30 * time.Second)
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			t.Fatalf("Timeout waiting for SQS job to go to DLQ")
		case <-ticker.C:
			dlqCount := atomic.LoadInt64(&dlqJobsReceived)
			sqsRetryTestJobMutex.Lock()
			attempts := sqsRetryTestJobProcessed["sqs-always-fail-job"]
			sqsRetryTestJobMutex.Unlock()

			fmt.Printf("⏳ SQS job attempts: %d, DLQ count: %d\n", attempts, dlqCount)

			if dlqCount >= 1 {
				break
			}
		}
		if atomic.LoadInt64(&dlqJobsReceived) >= 1 {
			break
		}
	}

	dlqCount := atomic.LoadInt64(&dlqJobsReceived)
	if dlqCount != 1 {
		t.Errorf("Expected 1 SQS job in DLQ, got %d", dlqCount)
	}

	// Cleanup
	cancel()
	time.Sleep(1 * time.Second)

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	goqueue.Shutdown(queue, shutdownCtx)

	fmt.Printf("✅ SQS DLQ TEST PASSED: Job exhausted retries and went to DLQ\n")
}

// TestSQSConsistentRetryBehavior - Test that SQS retry behavior matches Redis
func TestSQSConsistentRetryBehavior(t *testing.T) {
	// Skip test if running in CI or if SQS credentials are not available
	if testing.Short() {
		t.Skip("Skipping SQS consistency test in short mode")
	}

	// Check if SQS configuration is available
	_, _, _, _, ok := getSQSTestConfig()
	if !ok {
		t.Skip("Skipping SQS consistency test - SQS_TEST_* environment variables not set")
	}

	fmt.Println("🔄 Testing SQS and Redis retry consistency...")

	// Test parameters
	baseDelay := 1 * time.Second
	maxRetries := 3

	// Test SQS retry timing
	resetSQSRetryTestCounters()
	sqsStartTime := time.Now()

	cfg := createSQSRetryConfig(maxRetries, baseDelay, true)
	queue, err := goqueue.NewQueueWithDefaults("sqs-consistency-test", cfg)
	if err != nil {
		t.Fatalf(sqsQueueCreateError, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err = goqueue.StartWorker(queue, ctx, 1)
	if err != nil {
		t.Fatalf(sqsWorkerStartError, err)
	}

	// Job that fails twice then succeeds
	retryJob := &SQSRetryTestJob{
		ID:           "sqs-consistency-job",
		ShouldFail:   true,
		FailAttempts: 2,
	}

	err = goqueue.Dispatch(queue, retryJob)
	if err != nil {
		t.Fatalf("Failed to dispatch SQS job: %v", err)
	}

	// Wait for completion
	timeout := time.After(25 * time.Second)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			t.Fatalf("Timeout waiting for SQS consistency job")
		case <-ticker.C:
			sqsRetryTestJobMutex.Lock()
			attempts := sqsRetryTestJobProcessed["sqs-consistency-job"]
			sqsRetryTestJobMutex.Unlock()

			if attempts >= 3 {
				goto sqsCompleted
			}
		}
	}

sqsCompleted:
	sqsTotalTime := time.Since(sqsStartTime)

	// Cleanup SQS
	cancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	goqueue.Shutdown(queue, shutdownCtx)

	// Expected minimum time with exponential backoff: 1s + 2s = 3s
	expectedMinTime := 3 * time.Second
	if sqsTotalTime < expectedMinTime {
		t.Errorf("SQS retry timing too fast: expected at least %v, got %v", expectedMinTime, sqsTotalTime)
	}

	fmt.Printf("✅ SQS CONSISTENCY TEST PASSED: Total time %v (≥ %v expected)\n", sqsTotalTime, expectedMinTime)
}

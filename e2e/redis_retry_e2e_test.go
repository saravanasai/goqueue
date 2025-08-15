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

// RedisRetryTestJob - Job implementation for retry testing
type RedisRetryTestJob struct {
	ID            string    `json:"id"`
	ShouldFail    bool      `json:"should_fail"`
	FailAttempts  int       `json:"fail_attempts"` // Number of attempts to fail before succeeding
	ProcessedTime time.Time `json:"processed_time"`
}

var retryTestJobCounter int64
var retryTestJobProcessed = make(map[string]int)
var retryTestJobMutex sync.Mutex

// Implement the Job interface
func (r *RedisRetryTestJob) Process(ctx context.Context) error {
	atomic.AddInt64(&retryTestJobCounter, 1)

	retryTestJobMutex.Lock()
	defer retryTestJobMutex.Unlock()

	retryTestJobProcessed[r.ID]++
	currentAttempt := retryTestJobProcessed[r.ID]

	r.ProcessedTime = time.Now()

	fmt.Printf("Processing retry test job %s (attempt %d, should fail: %v, fail attempts: %d)\n",
		r.ID, currentAttempt, r.ShouldFail, r.FailAttempts)

	// Simulate processing time
	time.Sleep(50 * time.Millisecond)

	// Fail if configured to fail for this attempt
	if r.ShouldFail && currentAttempt <= r.FailAttempts {
		return fmt.Errorf("intentional failure for retry testing (attempt %d/%d)", currentAttempt, r.FailAttempts)
	}

	return nil
}

// Register job type for serialization
func init() {
	goqueue.RegisterJob("RedisRetryTestJob", func() goqueue.Job {
		return &RedisRetryTestJob{}
	})
}

// resetRetryTestCounters resets global test counters
func resetRetryTestCounters() {
	atomic.StoreInt64(&retryTestJobCounter, 0)
	retryTestJobMutex.Lock()
	retryTestJobProcessed = make(map[string]int)
	retryTestJobMutex.Unlock()
}

// TestRedisRetryMechanism - Test basic retry functionality
func TestRedisRetryMechanism(t *testing.T) {
	resetRetryTestCounters()

	// Setup mini Redis server
	miniRedis := setupMiniRedis(t)
	defer miniRedis.Close()

	// Create Redis config with retry settings
	cfg := config.NewRedisConfig(miniRedis.Addr(), "", 0)
	cfg = cfg.WithMaxRetryAttempts(3).
		WithRetryDelay(500 * time.Millisecond).
		WithExponentialBackoff(false)

	// Create queue
	queue, err := goqueue.NewQueueWithDefaults("redis-retry-test", cfg)
	if err != nil {
		t.Fatalf("Failed to create Redis queue: %v", err)
	}

	// Start workers
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err = goqueue.StartWorker(queue, ctx, 2)
	if err != nil {
		t.Fatalf("Failed to start Redis workers: %v", err)
	}

	fmt.Println("🔄 Testing basic retry mechanism...")

	// Create a job that will fail 2 times, then succeed on the 3rd attempt
	retryJob := &RedisRetryTestJob{
		ID:           "retry-job-1",
		ShouldFail:   true,
		FailAttempts: 2, // Fail on attempts 1 and 2, succeed on attempt 3
	}

	// Dispatch the job
	err = goqueue.Dispatch(queue, retryJob)
	if err != nil {
		t.Fatalf("Failed to dispatch retry job: %v", err)
	}

	// Wait for job to be processed (with retries)
	timeout := time.After(15 * time.Second)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	var finalAttempts int
	for {
		select {
		case <-timeout:
			t.Fatalf("Timeout waiting for retry job to complete")
		case <-ticker.C:
			retryTestJobMutex.Lock()
			attempts := retryTestJobProcessed["retry-job-1"]
			retryTestJobMutex.Unlock()

			if attempts >= 3 {
				finalAttempts = attempts
				goto checkResults
			}
			fmt.Printf("⏳ Job retry-job-1 processed %d times, waiting for 3...\n", attempts)
		}
	}

checkResults:
	// Verify job was processed exactly 3 times (2 failures + 1 success)
	if finalAttempts != 3 {
		t.Errorf("Expected job to be processed 3 times, got %d", finalAttempts)
	}

	// Cleanup
	cancel()
	time.Sleep(500 * time.Millisecond)

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	goqueue.Shutdown(queue, shutdownCtx)

	fmt.Printf("✅ RETRY TEST PASSED: Job processed %d times as expected\n", finalAttempts)
}

// TestRedisExponentialBackoff - Test exponential backoff retry mechanism
func TestRedisExponentialBackoff(t *testing.T) {
	resetRetryTestCounters()

	// Setup mini Redis server
	miniRedis := setupMiniRedis(t)
	defer miniRedis.Close()

	// Create Redis config with exponential backoff
	cfg := config.NewRedisConfig(miniRedis.Addr(), "", 0)
	cfg = cfg.WithMaxRetryAttempts(4).
		WithRetryDelay(200 * time.Millisecond).
		WithExponentialBackoff(true)

	// Create queue
	queue, err := goqueue.NewQueueWithDefaults("redis-exponential-test", cfg)
	if err != nil {
		t.Fatalf("Failed to create Redis queue: %v", err)
	}

	// Start workers
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	err = goqueue.StartWorker(queue, ctx, 1)
	if err != nil {
		t.Fatalf("Failed to start Redis workers: %v", err)
	}

	fmt.Println("📈 Testing exponential backoff retry mechanism...")

	// Create a job that will fail 3 times, then succeed on the 4th attempt
	retryJob := &RedisRetryTestJob{
		ID:           "exponential-job-1",
		ShouldFail:   true,
		FailAttempts: 3, // Fail on attempts 1, 2, and 3, succeed on attempt 4
	}

	startTime := time.Now()

	// Dispatch the job
	err = goqueue.Dispatch(queue, retryJob)
	if err != nil {
		t.Fatalf("Failed to dispatch exponential backoff job: %v", err)
	}

	// Wait for job to be processed (with exponential backoff retries)
	timeout := time.After(30 * time.Second)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	var finalAttempts int
	for {
		select {
		case <-timeout:
			t.Fatalf("Timeout waiting for exponential backoff job to complete")
		case <-ticker.C:
			retryTestJobMutex.Lock()
			attempts := retryTestJobProcessed["exponential-job-1"]
			retryTestJobMutex.Unlock()

			if attempts >= 4 {
				finalAttempts = attempts
				goto checkExponentialResults
			}
			fmt.Printf("⏳ Exponential job processed %d times, waiting for 4...\n", attempts)
		}
	}

checkExponentialResults:
	totalTime := time.Since(startTime)

	// Verify job was processed exactly 4 times
	if finalAttempts != 4 {
		t.Errorf("Expected exponential job to be processed 4 times, got %d", finalAttempts)
	}

	// With exponential backoff (200ms base), delays should be: 200ms, 400ms, 800ms
	// Minimum total time should be around 1.4s (200+400+800) plus processing time
	expectedMinTime := 1400 * time.Millisecond
	if totalTime < expectedMinTime {
		t.Errorf("Exponential backoff too fast: expected at least %v, got %v", expectedMinTime, totalTime)
	}

	// Cleanup
	cancel()
	time.Sleep(500 * time.Millisecond)

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	goqueue.Shutdown(queue, shutdownCtx)

	fmt.Printf("✅ EXPONENTIAL BACKOFF TEST PASSED: Job processed %d times in %v\n", finalAttempts, totalTime)
}

// TestRedisMaxRetryExceeded - Test that jobs go to DLQ after max retries
func TestRedisMaxRetryExceeded(t *testing.T) {
	resetRetryTestCounters()

	// Setup mini Redis server
	miniRedis := setupMiniRedis(t)
	defer miniRedis.Close()

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
			fmt.Printf("📮 Job pushed to DLQ: %+v, error: %v\n", jobCtx, err)
			return nil
		},
	}

	// Create Redis config with limited retries
	cfg := config.NewRedisConfig(miniRedis.Addr(), "", 0)
	cfg = cfg.WithMaxRetryAttempts(2).
		WithRetryDelay(100 * time.Millisecond).
		WithDLQAdapter(mockDLQ)

	// Create queue
	queue, err := goqueue.NewQueueWithDefaults("redis-dlq-test", cfg)
	if err != nil {
		t.Fatalf("Failed to create Redis queue: %v", err)
	}

	// Start workers
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	err = goqueue.StartWorker(queue, ctx, 1)
	if err != nil {
		t.Fatalf("Failed to start Redis workers: %v", err)
	}

	fmt.Println("💀 Testing DLQ functionality after max retries...")

	// Create a job that will always fail
	alwaysFailJob := &RedisRetryTestJob{
		ID:           "always-fail-job",
		ShouldFail:   true,
		FailAttempts: 10, // Always fail (more than max retries)
	}

	// Dispatch the job
	err = goqueue.Dispatch(queue, alwaysFailJob)
	if err != nil {
		t.Fatalf("Failed to dispatch always-fail job: %v", err)
	}

	// Wait for job to exhaust retries and go to DLQ
	timeout := time.After(15 * time.Second)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			t.Fatalf("Timeout waiting for job to go to DLQ")
		case <-ticker.C:
			dlqCount := atomic.LoadInt64(&dlqJobsReceived)
			retryTestJobMutex.Lock()
			attempts := retryTestJobProcessed["always-fail-job"]
			retryTestJobMutex.Unlock()

			fmt.Printf("📊 Job attempts: %d, DLQ jobs: %d\n", attempts, dlqCount)

			if dlqCount >= 1 {
				// Verify job was processed exactly max retry attempts (2)
				if attempts != 2 {
					t.Errorf("Expected job to be processed 2 times before DLQ, got %d", attempts)
				}
				goto checkDLQResults
			}
		}
	}

checkDLQResults:
	dlqCount := atomic.LoadInt64(&dlqJobsReceived)
	if dlqCount != 1 {
		t.Errorf("Expected 1 job in DLQ, got %d", dlqCount)
	}

	// Cleanup
	cancel()
	time.Sleep(500 * time.Millisecond)

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	goqueue.Shutdown(queue, shutdownCtx)

	fmt.Printf("✅ DLQ TEST PASSED: Job exhausted %d retries and went to DLQ\n", 2)
}

// TestRedisMultiWorkerRetry - Test retry mechanism with multiple workers
func TestRedisMultiWorkerRetry(t *testing.T) {
	resetRetryTestCounters()

	// Setup mini Redis server
	miniRedis := setupMiniRedis(t)
	defer miniRedis.Close()

	// Create Redis config
	cfg := config.NewRedisConfig(miniRedis.Addr(), "", 0)
	cfg = cfg.WithMaxRetryAttempts(3).
		WithRetryDelay(200 * time.Millisecond)

	// Create queue
	queue, err := goqueue.NewQueueWithDefaults("redis-multiworker-retry", cfg)
	if err != nil {
		t.Fatalf("Failed to create Redis queue: %v", err)
	}

	// Start multiple workers
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	err = goqueue.StartWorker(queue, ctx, 3) // 3 workers
	if err != nil {
		t.Fatalf("Failed to start Redis workers: %v", err)
	}

	fmt.Println("👥 Testing retry mechanism with multiple workers...")

	// Create multiple jobs with different retry patterns
	jobs := []*RedisRetryTestJob{
		{ID: "multi-1", ShouldFail: true, FailAttempts: 1},
		{ID: "multi-2", ShouldFail: true, FailAttempts: 2},
		{ID: "multi-3", ShouldFail: false, FailAttempts: 0}, // Should succeed immediately
		{ID: "multi-4", ShouldFail: true, FailAttempts: 1},
		{ID: "multi-5", ShouldFail: true, FailAttempts: 2},
	}

	// Dispatch all jobs
	for _, job := range jobs {
		err = goqueue.Dispatch(queue, job)
		if err != nil {
			t.Fatalf("Failed to dispatch job %s: %v", job.ID, err)
		}
		time.Sleep(50 * time.Millisecond) // Small delay between dispatches
	}

	// Wait for all jobs to complete
	timeout := time.After(20 * time.Second)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	expectedTotalAttempts := 2 + 3 + 1 + 2 + 3 // Sum of expected attempts for each job

	for {
		select {
		case <-timeout:
			t.Fatalf("Timeout waiting for multi-worker retry jobs to complete")
		case <-ticker.C:
			retryTestJobMutex.Lock()
			totalAttempts := 0
			for _, job := range jobs {
				totalAttempts += retryTestJobProcessed[job.ID]
			}
			retryTestJobMutex.Unlock()

			fmt.Printf("📊 Total attempts so far: %d/%d\n", totalAttempts, expectedTotalAttempts)

			if totalAttempts >= expectedTotalAttempts {
				goto checkMultiWorkerResults
			}
		}
	}

checkMultiWorkerResults:
	// Verify each job was processed the correct number of times
	retryTestJobMutex.Lock()
	defer retryTestJobMutex.Unlock()

	expectedAttempts := map[string]int{
		"multi-1": 2, // Fail once, succeed on second
		"multi-2": 3, // Fail twice, succeed on third
		"multi-3": 1, // Succeed immediately
		"multi-4": 2, // Fail once, succeed on second
		"multi-5": 3, // Fail twice, succeed on third
	}

	for jobID, expected := range expectedAttempts {
		actual := retryTestJobProcessed[jobID]
		if actual != expected {
			t.Errorf("Job %s: expected %d attempts, got %d", jobID, expected, actual)
		}
	}

	// Cleanup
	cancel()
	time.Sleep(500 * time.Millisecond)

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	goqueue.Shutdown(queue, shutdownCtx)

	fmt.Printf("✅ MULTI-WORKER RETRY TEST PASSED: All jobs processed correctly\n")
}

// MockDLQAdapter for testing DLQ functionality
type MockDLQAdapter struct {
	OnPush func(ctx context.Context, jobCtx *job.JobContext, err error) error
}

func (m *MockDLQAdapter) Push(ctx context.Context, jobCtx *job.JobContext, err error) error {
	if m.OnPush != nil {
		return m.OnPush(ctx, jobCtx, err)
	}
	return nil
}

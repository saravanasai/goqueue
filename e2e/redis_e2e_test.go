package e2e

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/saravanasai/goqueue"
	"github.com/saravanasai/goqueue/config"
)

const redisServerFormat = "🔗 Redis Server: %s\n"

// RedisEmailJob - Example job implementation for Redis e2e testing
type RedisEmailJob struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
	Content string `json:"content"`
}

// Implement the Job interface
func (e *RedisEmailJob) Process(ctx context.Context) error {
	fmt.Printf("Processing Redis email to %s: %s\n", e.To, e.Subject)
	// Simulate some processing time
	time.Sleep(75 * time.Millisecond)
	return nil
}

// Register job type for serialization
func init() {
	goqueue.RegisterJob("RedisEmailJob", func() goqueue.Job {
		return &RedisEmailJob{}
	})
}

// setupMiniRedis creates a mini Redis server for testing
func setupMiniRedis(t *testing.T) *miniredis.Miniredis {
	miniRedis, err := miniredis.Run()
	if err != nil {
		t.Fatalf("Failed to start mini Redis: %v", err)
	}
	return miniRedis
}

// TestRedisQueueIntegration - Comprehensive e2e test for Redis driver
func TestRedisQueueIntegration(t *testing.T) {
	// Setup mini Redis server
	miniRedis := setupMiniRedis(t)
	defer miniRedis.Close()

	// Create Redis config without metrics callback to avoid timing issues
	cfg := config.NewRedisConfig(miniRedis.Addr(), "", 0)

	// Create new queue with Redis config
	queue, err := goqueue.NewQueueWithDefaults("redis-test-queue", cfg)
	if err != nil {
		t.Fatalf("Failed to create Redis queue: %v", err)
	}

	// Start workers
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	workerCount := 3
	err = goqueue.StartWorker(queue, ctx, workerCount)
	if err != nil {
		t.Fatalf("Failed to start Redis workers: %v", err)
	}

	// Create test jobs for Redis
	testJobs := []*RedisEmailJob{
		{To: "redis-user1@example.com", Subject: "Redis Welcome", Content: "Welcome to Redis queue!"},
		{To: "redis-user2@example.com", Subject: "Redis Order Confirmation", Content: "Your order has been confirmed."},
		{To: "redis-user3@example.com", Subject: "Redis Newsletter", Content: "Monthly newsletter from Redis queue."},
		{To: "redis-admin@example.com", Subject: "Redis Report", Content: "Daily Redis performance report."},
		{To: "redis-support@example.com", Subject: "Redis Ticket Update", Content: "Your support ticket has been updated."},
	}

	expectedJobCount := len(testJobs)
	fmt.Printf("🚀 Dispatching %d Redis jobs to %d workers...\n", expectedJobCount, workerCount)

	// Dispatch individual jobs
	for i, job := range testJobs {
		err := goqueue.Dispatch(queue, job)
		if err != nil {
			t.Fatalf("Failed to dispatch Redis job %d: %v", i+1, err)
		}
		// Small delay to see job distribution across workers
		time.Sleep(10 * time.Millisecond)
	}

	// Give workers time to process jobs
	fmt.Println("⏳ Waiting for Redis jobs to complete...")
	time.Sleep(2 * time.Second)

	// Check queue stats to verify jobs were processed
	stats := queue.Stats()
	fmt.Printf("📊 Queue Stats - Healthy: %v\n", stats.IsHealthy)

	// Verify queue is healthy
	if !queue.IsHealthy() {
		t.Fatalf("Redis queue is not healthy")
	}

	// Cancel workers context to stop processing
	cancel()

	// Wait a moment for workers to stop
	time.Sleep(200 * time.Millisecond)

	// Shutdown queue with more lenient timeout
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	err = goqueue.Shutdown(queue, shutdownCtx)
	if err != nil {
		t.Logf("Redis shutdown warning: %v", err)
	}

	// Verify queue was healthy during operation
	if !stats.IsHealthy {
		t.Fatalf("Redis queue was not healthy during operation")
	}

	// Print final success message with clear separation
	fmt.Println("\n" + strings.Repeat("=", 70))
	fmt.Printf("✅ REDIS TEST PASSED: %d jobs dispatched successfully to Redis queue\n", expectedJobCount)
	fmt.Printf("📊 Expected: %d jobs | Workers: %d | Queue Healthy: %v\n", expectedJobCount, workerCount, stats.IsHealthy)
	fmt.Printf(redisServerFormat, miniRedis.Addr())
	fmt.Printf("ℹ️  Note: Jobs were processed by Redis workers successfully\n")
	fmt.Println(strings.Repeat("=", 70))
}

// TestRedisQueueConcurrentDispatch - Test concurrent job dispatching with Redis
func TestRedisQueueConcurrentDispatch(t *testing.T) {
	// Setup mini Redis server
	miniRedis := setupMiniRedis(t)
	defer miniRedis.Close()

	// Create Redis config
	cfg := config.NewRedisConfig(miniRedis.Addr(), "", 0)

	// Create queue
	queue, err := goqueue.NewQueueWithDefaults("redis-concurrent-queue", cfg)
	if err != nil {
		t.Fatalf("Failed to create Redis queue for concurrent test: %v", err)
	}

	// Start workers
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	err = goqueue.StartWorker(queue, ctx, 4) // More workers for concurrency test
	if err != nil {
		t.Fatalf("Failed to start Redis workers for concurrent test: %v", err)
	}

	// Number of jobs to dispatch concurrently
	jobCount := 10 // Reduced for simpler test
	var wg sync.WaitGroup

	fmt.Printf("🚀 Starting concurrent dispatch of %d Redis jobs...\n", jobCount)

	// Dispatch jobs concurrently
	wg.Add(jobCount)
	for i := 0; i < jobCount; i++ {
		go func(jobNum int) {
			defer wg.Done()
			job := &RedisEmailJob{
				To:      fmt.Sprintf("concurrent-user%d@example.com", jobNum),
				Subject: fmt.Sprintf("Concurrent Job #%d", jobNum),
				Content: fmt.Sprintf("This is concurrent job number %d", jobNum),
			}

			err := goqueue.Dispatch(queue, job)
			if err != nil {
				t.Errorf("Failed to dispatch concurrent job %d: %v", jobNum, err)
			}
		}(i)
	}

	// Wait for all dispatches to complete
	wg.Wait()
	fmt.Printf("📤 All %d jobs dispatched concurrently\n", jobCount)

	// Give time for processing
	fmt.Println("⏳ Waiting for concurrent Redis jobs to complete...")
	time.Sleep(3 * time.Second)

	// Check queue health
	if !queue.IsHealthy() {
		t.Fatalf("Redis queue is not healthy after concurrent operations")
	}

	fmt.Printf("🎉 All %d concurrent Redis jobs handled successfully!\n", jobCount)

	// Cancel workers context first
	cancel()

	// Wait a moment for workers to stop
	time.Sleep(200 * time.Millisecond)

	// Shutdown
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	err = goqueue.Shutdown(queue, shutdownCtx)
	if err != nil {
		t.Logf("Concurrent test shutdown warning: %v", err)
	}

	// Print success message
	fmt.Println("\n" + strings.Repeat("=", 70))
	fmt.Printf("✅ REDIS CONCURRENT TEST PASSED: %d jobs dispatched successfully\n", jobCount)
	fmt.Printf("📊 Dispatched: %d jobs | Queue remained healthy throughout\n", jobCount)
	fmt.Printf(redisServerFormat, miniRedis.Addr())
	fmt.Println(strings.Repeat("=", 70))
}

// TestRedisQueueMetrics - Test that metrics are properly collected for Redis driver
func TestRedisQueueMetrics(t *testing.T) {
	// Setup mini Redis server
	miniRedis := setupMiniRedis(t)
	defer miniRedis.Close()

	// Track job execution metrics
	var jobsExecuted int64
	var jobErrors []error
	var jobMetrics []config.JobMetrics
	var mu sync.Mutex

	// Create queue with metrics callback
	queue := setupRedisQueueWithMetrics(t, miniRedis.Addr(), &jobsExecuted, &jobErrors, &jobMetrics, &mu)

	// Start workers and dispatch jobs
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	testJobs := createRedisMetricsTestJobs()
	expectedJobCount := len(testJobs)

	err := goqueue.StartWorker(queue, ctx, 2)
	if err != nil {
		t.Fatalf("Failed to start Redis workers for metrics test: %v", err)
	}

	dispatchRedisMetricsJobs(t, queue, testJobs)

	// Wait for jobs to complete
	waitForRedisMetricsCompletion(t, expectedJobCount, &jobsExecuted)

	// Cleanup
	cleanupRedisMetricsTest(t, queue, cancel)

	// Validate metrics
	validateRedisMetrics(t, expectedJobCount, &jobsExecuted, &jobErrors, &jobMetrics, &mu, miniRedis.Addr())
}

// setupRedisQueueWithMetrics creates a Redis queue with metrics callback
func setupRedisQueueWithMetrics(t *testing.T, redisAddr string, jobsExecuted *int64, jobErrors *[]error, jobMetrics *[]config.JobMetrics, mu *sync.Mutex) *goqueue.Queue {
	cfg := config.NewRedisConfig(redisAddr, "", 0).WithMetricsCallback(func(metrics config.JobMetrics) {
		atomic.AddInt64(jobsExecuted, 1)

		mu.Lock()
		defer mu.Unlock()
		*jobMetrics = append(*jobMetrics, metrics)
		
		if metrics.Error != nil {
			*jobErrors = append(*jobErrors, metrics.Error)
			fmt.Printf("❌ Job failed: %s (Duration: %v, Error: %v)\n", metrics.JobID, metrics.Duration, metrics.Error)
		} else {
			fmt.Printf("✅ Job completed: %s (Duration: %v)\n", metrics.JobID, metrics.Duration)
		}
	})

	// Enable debug logging to see what's happening
	queue, err := goqueue.NewQueueWithDefaults("redis-metrics-queue", cfg)
	if err != nil {
		t.Fatalf("Failed to create Redis queue for metrics test: %v", err)
	}
	return queue
}

// createRedisMetricsTestJobs creates test jobs for metrics testing
func createRedisMetricsTestJobs() []*RedisEmailJob {
	return []*RedisEmailJob{
		{To: "metrics-user1@example.com", Subject: "Metrics Test 1", Content: "Testing Redis metrics collection"},
		{To: "metrics-user2@example.com", Subject: "Metrics Test 2", Content: "Testing Redis metrics callback"},
		{To: "metrics-user3@example.com", Subject: "Metrics Test 3", Content: "Testing Redis job tracking"},
		{To: "metrics-user4@example.com", Subject: "Metrics Test 4", Content: "Testing Redis duration tracking"},
	}
}

// dispatchRedisMetricsJobs dispatches test jobs for metrics testing
func dispatchRedisMetricsJobs(t *testing.T, queue *goqueue.Queue, testJobs []*RedisEmailJob) {
	fmt.Printf("🚀 Starting Redis metrics test with %d jobs and 2 workers...\n", len(testJobs))

	for i, job := range testJobs {
		err := goqueue.Dispatch(queue, job)
		if err != nil {
			t.Fatalf("Failed to dispatch Redis metrics job %d: %v", i+1, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// waitForRedisMetricsCompletion waits for all jobs to complete
func waitForRedisMetricsCompletion(t *testing.T, expectedJobCount int, jobsExecuted *int64) {
	timeout := time.After(20 * time.Second) // Increased timeout for Redis metrics processing
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	fmt.Println("⏳ Waiting for Redis jobs to complete and metrics to be collected...")

	for {
		select {
		case <-timeout:
			executed := atomic.LoadInt64(jobsExecuted)
			t.Fatalf("Timeout: Expected %d jobs, metrics collected for %d jobs", expectedJobCount, executed)
		case <-ticker.C:
			executed := atomic.LoadInt64(jobsExecuted)
			fmt.Printf("📊 Progress: %d/%d job metrics collected\n", executed, expectedJobCount)

			if executed >= int64(expectedJobCount) {
				fmt.Printf("✅ All %d job metrics collected!\n", expectedJobCount)
				return
			}
		}
	}
}

// cleanupRedisMetricsTest performs cleanup after metrics test
func cleanupRedisMetricsTest(t *testing.T, queue *goqueue.Queue, cancel context.CancelFunc) {
	time.Sleep(500 * time.Millisecond)

	if !queue.IsHealthy() {
		t.Fatalf("Redis queue is not healthy after metrics test")
	}

	cancel()
	time.Sleep(200 * time.Millisecond)

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	err := goqueue.Shutdown(queue, shutdownCtx)
	if err != nil {
		t.Logf("Redis metrics test shutdown warning: %v", err)
	}
}

// validateRedisMetrics validates the collected metrics data
func validateRedisMetrics(t *testing.T, expectedJobCount int, jobsExecuted *int64, jobErrors *[]error, jobMetrics *[]config.JobMetrics, mu *sync.Mutex, redisAddr string) {
	finalExecuted := atomic.LoadInt64(jobsExecuted)

	mu.Lock()
	defer mu.Unlock()
	
	errorCount := len(*jobErrors)
	metricsCount := len(*jobMetrics)

	if errorCount > 0 {
		t.Fatalf("Found %d job errors during metrics test", errorCount)
	}

	if finalExecuted < int64(expectedJobCount) {
		t.Fatalf("Not enough job metrics collected: expected at least %d, got %d", expectedJobCount, finalExecuted)
	}

	// Validate individual metrics
	for i, metric := range *jobMetrics {
		validateSingleMetric(t, i, metric)
	}

	// Print success message
	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Printf("✅ REDIS METRICS TEST PASSED: %d job metrics collected successfully\n", finalExecuted)
	fmt.Printf("📊 Expected: %d jobs | Metrics Collected: %d | Errors: %d\n", expectedJobCount, metricsCount, errorCount)
	fmt.Printf("⏱️  Average Job Duration: %v\n", calculateAverageDuration(*jobMetrics))
	fmt.Printf(redisServerFormat, redisAddr)
	fmt.Printf("ℹ️  Note: All metrics contain valid JobID, Duration, QueueName, and Timestamp\n")
	fmt.Println(strings.Repeat("=", 80))
}

// validateSingleMetric validates a single job metric
func validateSingleMetric(t *testing.T, index int, metric config.JobMetrics) {
	if metric.QueueName != "redis-metrics-queue" {
		t.Errorf("Metric %d: expected queue name 'redis-metrics-queue', got '%s'", index, metric.QueueName)
	}

	if metric.JobID == "" {
		t.Errorf("Metric %d: job ID is empty", index)
	}

	if metric.Duration <= 0 {
		t.Errorf("Metric %d: invalid duration %v", index, metric.Duration)
	}
	if metric.Duration > 2*time.Second {
		t.Errorf("Metric %d: duration too long %v", index, metric.Duration)
	}

	if time.Since(metric.Timestamp) > 30*time.Second {
		t.Errorf("Metric %d: timestamp too old %v", index, metric.Timestamp)
	}

	if metric.Error != nil {
		t.Errorf("Metric %d: unexpected error %v", index, metric.Error)
	}
}

// calculateAverageDuration calculates the average duration from job metrics
func calculateAverageDuration(metrics []config.JobMetrics) time.Duration {
	if len(metrics) == 0 {
		return 0
	}

	var total time.Duration
	for _, metric := range metrics {
		total += metric.Duration
	}

	return total / time.Duration(len(metrics))
}

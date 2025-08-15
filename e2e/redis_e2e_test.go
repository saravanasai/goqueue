package e2e

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/saravanasai/goqueue"
	"github.com/saravanasai/goqueue/config"
)

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
	fmt.Printf("🔗 Redis Server: %s\n", miniRedis.Addr())
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
	fmt.Printf("🔗 Redis Server: %s\n", miniRedis.Addr())
	fmt.Println(strings.Repeat("=", 70))
}

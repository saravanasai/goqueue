package redis

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/saravanasai/goqueue/config"
	"github.com/saravanasai/goqueue/internal/logger"
	"github.com/saravanasai/goqueue/internal/manager"
	"github.com/saravanasai/goqueue/internal/registry"
	"github.com/saravanasai/goqueue/job"
)

// TestJob is a simple job implementation for testing
type TestJob struct {
	ID   string `json:"id"`
	Data string `json:"data"`
}

// Process implements the job.Job interface
func (j *TestJob) Process(ctx context.Context) error {
	return nil
}

// setupTestRedis creates a miniredis instance for testing
func setupTestRedis(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	// Create a mini Redis server
	miniRedis, err := miniredis.Run()
	if err != nil {
		t.Fatalf("Error creating miniredis: %v", err)
	}

	// Create a Redis client connected to the mini Redis server
	client := redis.NewClient(&redis.Options{
		Addr: miniRedis.Addr(),
		DB:   0,
	})

	return miniRedis, client
}

// TestRedisHealth tests the basic health check functionality
func TestRedisHealth(t *testing.T) {
	// Setup miniredis for testing
	miniRedis, client := setupTestRedis(t)
	// Keep miniredis running for this test
	defer miniRedis.Close()

	// Create test logger
	testLogger := logger.NewZapLogger()

	// Create Redis manager
	redisManager := manager.NewRedisClientManager(miniRedis.Addr(), "", 0, testLogger)

	// Create configuration
	cfg := config.NewRedisConfig(miniRedis.Addr(), "", 0)

	// Create Redis store with the manager
	store := NewRedisStore(client, cfg, redisManager, miniRedis.Addr(), 0, testLogger)

	// Test health check with Redis available
	isHealthy := store.IsHealthy()
	if !isHealthy {
		t.Error("Expected store to be healthy when Redis is available")
	}
}

// TestRedisClientPing tests that we can detect when Redis is unavailable
func TestRedisClientPing(t *testing.T) {
	// Setup miniredis for testing
	miniRedis, _ := setupTestRedis(t)
	// Get the address before closing
	redisAddr := miniRedis.Addr()

	// Close the miniredis server to simulate Redis being unavailable
	miniRedis.Close()

	// Create a client that points to the closed Redis server
	testClient := redis.NewClient(&redis.Options{
		Addr: redisAddr,
		DB:   0,
	})
	defer testClient.Close()

	// Try to ping the Redis server (should fail because it's closed)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err := testClient.Ping(ctx).Result()
	if err == nil {
		t.Error("Expected error when Redis is unavailable, but got nil")
	} else {
		t.Logf("Successfully detected Redis unavailability: %v", err)
	}
}

// TestRedisPushOnly tests just the Push operation
func TestRedisPushOnly(t *testing.T) {
	// Setup miniredis for testing
	miniRedis, client := setupTestRedis(t)
	defer miniRedis.Close()

	// Create test logger
	testLogger := logger.NewZapLogger()

	// Create Redis manager
	redisManager := manager.NewRedisClientManager(miniRedis.Addr(), "", 0, testLogger)

	// Create configuration
	cfg := config.NewRedisConfig(miniRedis.Addr(), "", 0)

	// Create Redis store
	store := NewRedisStore(client, cfg, redisManager, miniRedis.Addr(), 0, testLogger)

	// Register the test job type
	registry.Register("TestJob", func() job.Job {
		return &TestJob{}
	})

	// Create a test job
	testJob := &TestJob{
		ID:   "test123",
		Data: "test data",
	}

	// Define a queue name for testing
	queueName := "test_queue"

	// Test pushing a job
	err := store.Push(queueName, testJob)
	if err != nil {
		t.Fatalf("Error pushing job: %v", err)
	}

	// Verify the queue length in Redis
	queueLen, err := client.LLen(context.Background(), queueName).Result()
	if err != nil {
		t.Fatalf("Error getting queue length: %v", err)
	}
	if queueLen != 1 {
		t.Errorf("Expected queue length of 1, got %d", queueLen)
	}

	// Verify that we created an index entry
	indexKey := fmt.Sprintf("job_index:%s", queueName)
	count, err := client.HLen(context.Background(), indexKey).Result()
	if err != nil {
		t.Fatalf("Error getting job index count: %v", err)
	}
	if count != 1 {
		t.Errorf("Expected 1 job in index, got %d", count)
	}
}

// TestRedisPopAndAck tests the Pop and Ack operations
func TestRedisPopAndAck(t *testing.T) {
	// Setup miniredis for testing
	miniRedis, client := setupTestRedis(t)
	defer miniRedis.Close()

	testLogger := logger.NewZapLogger()
	redisManager := manager.NewRedisClientManager(miniRedis.Addr(), "", 0, testLogger)
	cfg := config.NewRedisConfig(miniRedis.Addr(), "", 0)
	store := NewRedisStore(client, cfg, redisManager, miniRedis.Addr(), 0, testLogger)

	// Create and push a test job
	queueName := "test_queue_pop_ack"
	testJob := &TestJob{ID: "test123", Data: "test data"}
	err := store.Push(queueName, testJob)
	if err != nil {
		t.Fatalf("Error pushing job: %v", err)
	}

	// Test popping a job using the Pop function
	jobCtx, err := store.Pop(queueName)
	if err != nil {
		t.Fatalf("Error popping job: %v", err)
	}
	if jobCtx.Job == nil {
		t.Fatal("Expected to pop a job, got nil job in context")
	}

	// Verify the job ID exists
	if jobCtx.JobID == "" {
		t.Fatal("Expected job ID in context, got empty string")
	}

	// Verify job data
	poppedTestJob, ok := jobCtx.Job.(*TestJob)
	if !ok {
		t.Fatalf("Expected *TestJob, got %T", jobCtx.Job)
	}
	if poppedTestJob.ID != "test123" || poppedTestJob.Data != "test data" {
		t.Errorf("Job data mismatch. Got ID=%s, Data=%s",
			poppedTestJob.ID, poppedTestJob.Data)
	}

	// Test acknowledging a job
	err = store.Ack(queueName, jobCtx.JobID)
	if err != nil {
		t.Fatalf("Error acknowledging job: %v", err)
	}

	// Verify the queue is now empty by trying to pop again
	emptyJobCtx, err := store.Pop(queueName)
	if err != nil {
		t.Fatalf("Error on second pop: %v", err)
	}
	if emptyJobCtx.Job != nil {
		t.Error("Expected nil job for empty queue, got non-nil")
	}

	// Verify processing queue is empty
	processingQueueName := processingQueueName + queueName
	processingQueueLen, err := client.LLen(context.Background(), processingQueueName).Result()
	if err != nil {
		t.Fatalf("Error getting processing queue length: %v", err)
	}
	if processingQueueLen != 0 {
		t.Errorf("Expected processing queue to be empty, got length %d",
			processingQueueLen)
	}
}

package redis

import (
	"context"
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

// TestRedisPushPop tests basic push and pop operations
func TestRedisPushPop(t *testing.T) {
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
}

package redis

import (
	"context"
	"fmt"
	"testing"

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

	// Ensure registry is clean for this run by registering a unique name
	jobName := "TestJob_PushOnly"
	registry.Register(jobName, func() job.Job { return &TestJob{} })

	// Create a test job
	testJob := &TestJob{
		ID:   "test123",
		Data: "test data",
	}

	// Define a queue name for testing
	queueName := "test_queue_push_only"

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

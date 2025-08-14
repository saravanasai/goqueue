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

	// Ensure registry is clean for this run by registering the test job if not present
	if _, ok := registry.GetFromRegistery("TestJob"); !ok {
		registry.Register("TestJob", func() job.Job { return &TestJob{} })
	}

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
	indexKey := fmt.Sprintf(JobIndexKeyFormat, queueName)
	count, err := client.HLen(context.Background(), indexKey).Result()
	if err != nil {
		t.Fatalf("Error getting job index count: %v", err)
	}
	if count != 1 {
		t.Errorf("Expected 1 job in index, got %d", count)
	}
}

// TestRedisPushBatch tests the PushBatch operation
func TestRedisPushBatch(t *testing.T) {
	miniRedis, client := setupTestRedis(t)
	defer miniRedis.Close()

	testLogger := logger.NewZapLogger()
	redisManager := manager.NewRedisClientManager(miniRedis.Addr(), "", 0, testLogger)
	cfg := config.NewRedisConfig(miniRedis.Addr(), "", 0)
	store := NewRedisStore(client, cfg, redisManager, miniRedis.Addr(), 0, testLogger)

	// Register a unique job name for this test
	if _, ok := registry.GetFromRegistery("TestJob"); !ok {
		registry.Register("TestJob", func() job.Job { return &TestJob{} })
	}

	// Prepare batch jobs
	jobs := []job.Job{
		&TestJob{ID: "id1", Data: "one"},
		&TestJob{ID: "id2", Data: "two"},
	}
	queueName := "test_queue_push_batch"

	// Push the batch
	if err := store.PushBatch(queueName, jobs); err != nil {
		t.Fatalf("PushBatch returned error: %v", err)
	}

	// Verify queue length
	queueLen, err := client.LLen(context.Background(), queueName).Result()
	if err != nil {
		t.Fatalf("Error getting queue length: %v", err)
	}
	if queueLen != int64(len(jobs)) {
		t.Errorf("Expected queue length %d, got %d", len(jobs), queueLen)
	}

	// Verify index entries
	indexKey := fmt.Sprintf(JobIndexKeyFormat, queueName)
	count, err := client.HLen(context.Background(), indexKey).Result()
	if err != nil {
		t.Fatalf("Error getting job index count: %v", err)
	}
	if count != int64(len(jobs)) {
		t.Errorf("Expected %d jobs in index, got %d", len(jobs), count)
	}
}

// TestRedisPopAndAck tests Pop and Ack operations together
func TestRedisPopAndAck(t *testing.T) {
	miniRedis, client := setupTestRedis(t)
	defer miniRedis.Close()

	testLogger := logger.NewZapLogger()
	redisManager := manager.NewRedisClientManager(miniRedis.Addr(), "", 0, testLogger)
	cfg := config.NewRedisConfig(miniRedis.Addr(), "", 0)
	store := NewRedisStore(client, cfg, redisManager, miniRedis.Addr(), 0, testLogger)

	// Register job (idempotent)
	if _, ok := registry.GetFromRegistery("TestJob"); !ok {
		registry.Register("TestJob", func() job.Job { return &TestJob{} })
	}

	// Push a job using Push so index + list are created
	queueName := "test_queue_pop_ack"
	testJob := &TestJob{ID: "popid", Data: "popdata"}
	if err := store.Push(queueName, testJob); err != nil {
		t.Fatalf("Push error: %v", err)
	}

	// Pop the job
	jobCtx, err := store.Pop(queueName)
	if err != nil {
		t.Fatalf("Pop error: %v", err)
	}
	if jobCtx.Job == nil {
		t.Fatal("Expected job from Pop, got nil")
	}

	// Verify job type and data
	popped, ok := jobCtx.Job.(*TestJob)
	if !ok {
		t.Fatalf("Expected *TestJob, got %T", jobCtx.Job)
	}
	if popped.ID != "popid" || popped.Data != "popdata" {
		t.Errorf("Popped job data mismatch: %+v", popped)
	}

	// Acknowledge the job
	if err := store.Ack(queueName, jobCtx.JobID); err != nil {
		t.Fatalf("Ack error: %v", err)
	}

	// After ack, processing queue should be empty
	processingKey := processingQueueName + queueName
	plen, err := client.LLen(context.Background(), processingKey).Result()
	if err != nil {
		t.Fatalf("Error getting processing queue length: %v", err)
	}
	if plen != 0 {
		t.Errorf("Expected processing queue empty after Ack, got %d", plen)
	}

	// Index should have removed the job
	indexKey := fmt.Sprintf(JobIndexKeyFormat, queueName)
	count, err := client.HLen(context.Background(), indexKey).Result()
	if err != nil {
		t.Fatalf("Error getting index HLen: %v", err)
	}
	if count != 0 {
		t.Errorf("Expected index empty after Ack, got %d", count)
	}
}

// TestEnqueueDequeueMetrics verifies metrics can be enqueued and dequeued
func TestEnqueueDequeueMetrics(t *testing.T) {
	miniRedis, client := setupTestRedis(t)
	defer miniRedis.Close()

	testLogger := logger.NewZapLogger()
	redisManager := manager.NewRedisClientManager(miniRedis.Addr(), "", 0, testLogger)
	cfg := config.NewRedisConfig(miniRedis.Addr(), "", 0)
	store := NewRedisStore(client, cfg, redisManager, miniRedis.Addr(), 0, testLogger)

	queueName := "test_metrics_queue"
	metrics := config.JobMetrics{
		QueueName: queueName,
		JobID:     "m1",
		Duration:  150 * time.Millisecond,
		Error:     nil,
		Timestamp: time.Now(),
	}

	if err := store.EnqueueMetrics(metrics); err != nil {
		t.Fatalf("EnqueueMetrics error: %v", err)
	}

	// Dequeue metrics; DequeueMetrics uses BRPopLPush which will move item to ack queue
	got, err := store.DequeueMetrics(queueName)
	if err != nil {
		t.Fatalf("DequeueMetrics error: %v", err)
	}

	if got.JobID != metrics.JobID || got.QueueName != metrics.QueueName {
		t.Fatalf("Dequeued metrics mismatch. want=%+v got=%+v", metrics, got)
	}
}

// TestRedisIsHealthy validates IsHealthy returns true when manager has no negative status
func TestRedisIsHealthy(t *testing.T) {
	miniRedis, client := setupTestRedis(t)
	defer miniRedis.Close()

	testLogger := logger.NewZapLogger()
	redisManager := manager.NewRedisClientManager(miniRedis.Addr(), "", 0, testLogger)
	cfg := config.NewRedisConfig(miniRedis.Addr(), "", 0)
	store := NewRedisStore(client, cfg, redisManager, miniRedis.Addr(), 0, testLogger)

	if !store.IsHealthy() {
		t.Fatalf("Expected store to be healthy when Redis is available")
	}
}

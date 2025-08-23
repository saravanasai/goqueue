package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/saravanasai/goqueue/config"
	"github.com/saravanasai/goqueue/internal/logger"
	"github.com/saravanasai/goqueue/internal/manager"
	"github.com/saravanasai/goqueue/internal/registry"
	"github.com/saravanasai/goqueue/job"
)

// Integration test job type (unique name to avoid collisions with other tests)
type IntegrationTestJob struct {
	ID   string `json:"id"`
	Data string `json:"data"`
}

func (j *IntegrationTestJob) Process(ctx context.Context) error { return nil }

func ensureIntegrationJobRegistered() {
	if _, ok := registry.GetFromRegistery("IntegrationTestJob"); !ok {
		registry.Register("IntegrationTestJob", func() job.Job { return &IntegrationTestJob{} })
	}
}

func TestRedisIntegrationPushPopAck(t *testing.T) {
	miniRedis, client := setupTestRedis(t)
	defer miniRedis.Close()

	testLogger := logger.NewZapLogger()
	redisManager := manager.NewRedisClientManager(miniRedis.Addr(), "", 0, testLogger)
	cfg := config.NewRedisConfig(miniRedis.Addr(), "", 0)
	store := NewRedisStore(client, cfg, redisManager, miniRedis.Addr(), 0, testLogger)

	ensureIntegrationJobRegistered()

	q := "integration_redis_q"
	j := &IntegrationTestJob{ID: "r1", Data: "hello"}
	if err := store.Push(q, j); err != nil {
		t.Fatalf("Push failed: %v", err)
	}

	jc, err := store.Pop(q)
	if err != nil {
		t.Fatalf("Pop failed: %v", err)
	}
	if jc.Job == nil {
		t.Fatal("expected job from Pop, got nil")
	}
	if err := store.Ack(q, jc.JobID); err != nil {
		t.Fatalf("Ack failed: %v", err)
	}
}

func TestRedisIntegrationPushBatchPopAck(t *testing.T) {
	miniRedis, client := setupTestRedis(t)
	defer miniRedis.Close()

	testLogger := logger.NewZapLogger()
	redisManager := manager.NewRedisClientManager(miniRedis.Addr(), "", 0, testLogger)
	cfg := config.NewRedisConfig(miniRedis.Addr(), "", 0)
	store := NewRedisStore(client, cfg, redisManager, miniRedis.Addr(), 0, testLogger)

	ensureIntegrationJobRegistered()

	q := "integration_redis_batch"
	jobs := []job.Job{&IntegrationTestJob{ID: "b1", Data: "one"}, &IntegrationTestJob{ID: "b2", Data: "two"}}
	if err := store.PushBatch(q, jobs); err != nil {
		t.Fatalf("PushBatch failed: %v", err)
	}

	jc1, err := store.Pop(q)
	if err != nil {
		t.Fatalf("Pop1 failed: %v", err)
	}
	jc2, err := store.Pop(q)
	if err != nil {
		t.Fatalf("Pop2 failed: %v", err)
	}
	if jc1.Job == nil || jc2.Job == nil {
		t.Fatal("expected two jobs from Pop calls")
	}
	if err := store.Ack(q, jc1.JobID); err != nil {
		t.Fatalf("Ack1 failed: %v", err)
	}
	if err := store.Ack(q, jc2.JobID); err != nil {
		t.Fatalf("Ack2 failed: %v", err)
	}
}

func TestRedisIntegrationEnqueueDequeueMetrics(t *testing.T) {
	miniRedis, client := setupTestRedis(t)
	defer miniRedis.Close()

	testLogger := logger.NewZapLogger()
	redisManager := manager.NewRedisClientManager(miniRedis.Addr(), "", 0, testLogger)
	cfg := config.NewRedisConfig(miniRedis.Addr(), "", 0)
	store := NewRedisStore(client, cfg, redisManager, miniRedis.Addr(), 0, testLogger)

	q := "integration_metrics_q"
	m := config.JobMetrics{QueueName: q, JobID: "mm1", Duration: 10 * time.Millisecond, Timestamp: time.Now()}
	if err := store.EnqueueMetrics(m); err != nil {
		t.Fatalf("EnqueueMetrics failed: %v", err)
	}
	got, err := store.DequeueMetrics(q)
	if err != nil {
		t.Fatalf("DequeueMetrics failed: %v", err)
	}
	if got.JobID != m.JobID || got.QueueName != m.QueueName {
		t.Fatalf("metrics mismatch. want=%+v got=%+v", m, got)
	}
}

func TestRedisIntegrationIsHealthy(t *testing.T) {
	miniRedis, client := setupTestRedis(t)
	defer miniRedis.Close()

	testLogger := logger.NewZapLogger()
	redisManager := manager.NewRedisClientManager(miniRedis.Addr(), "", 0, testLogger)
	cfg := config.NewRedisConfig(miniRedis.Addr(), "", 0)
	store := NewRedisStore(client, cfg, redisManager, miniRedis.Addr(), 0, testLogger)

	if !store.IsHealthy() {
		t.Fatalf("expected IsHealthy true")
	}
}

func TestRedisIntegrationRetryJobWithMetadata(t *testing.T) {
	miniRedis, client := setupTestRedis(t)
	defer miniRedis.Close()

	testLogger := logger.NewZapLogger()
	redisManager := manager.NewRedisClientManager(miniRedis.Addr(), "", 0, testLogger)
	cfg := config.NewRedisConfig(miniRedis.Addr(), "", 0)
	store := NewRedisStore(client, cfg, redisManager, miniRedis.Addr(), 0, testLogger)

	ensureIntegrationJobRegistered()

	// Test setup
	q := "integration_redis_retry"
	originalJob := &IntegrationTestJob{ID: "retry1", Data: "original"}

	// Push and pop the original job
	if err := store.Push(q, originalJob); err != nil {
		t.Fatalf("Initial Push failed: %v", err)
	}

	jc, err := store.Pop(q)
	if err != nil || jc.Job == nil {
		t.Fatalf("Pop failed: %v", err)
	}

	// Get job for retry
	ctx := context.Background()
	indexKey := fmt.Sprintf(JobIndexKeyFormat, q)
	payload, err := client.HGet(ctx, indexKey, jc.JobID).Result()
	if err != nil {
		t.Fatalf("Failed to get job from index: %v", err)
	}

	// Prepare job for retry with modified data
	var redisJob job.RedisQueuedJob
	json.Unmarshal([]byte(payload), &redisJob)

	modifiedJob := &IntegrationTestJob{ID: "retry1", Data: "modified-for-retry"}
	jobBytes, _ := json.Marshal(modifiedJob)
	redisJob.Job = jobBytes

	// Retry with delay
	retryDelay := 2 * time.Second
	if err := store.RetryJobWithMetadata(q, redisJob, retryDelay); err != nil {
		t.Fatalf("RetryJobWithMetadata failed: %v", err)
	}

	// Verify retry queue entry
	retryQueueName := "retry:" + q
	retryMembers, err := client.ZRange(ctx, retryQueueName, 0, -1).Result()
	if err != nil || len(retryMembers) == 0 {
		t.Fatalf("Job not found in retry queue: %v", err)
	}

	var retryJobInfo job.RedisQueuedJob
	json.Unmarshal([]byte(retryMembers[0]), &retryJobInfo)
	if retryJobInfo.RetryCount != 1 {
		t.Fatalf("Expected retry count to be 1, got %d", retryJobInfo.RetryCount)
	}

	// Wait for retry poller
	time.Sleep(retryDelay + 1*time.Second)

	// Verify retried job
	retryJc, err := store.Pop(q)
	if err != nil {
		t.Fatalf("Pop after retry delay failed: %v", err)
	}

	gotRetryJob, _ := retryJc.Job.(*IntegrationTestJob)
	if gotRetryJob.Data != "modified-for-retry" || retryJc.RetryCount != 1 {
		t.Fatalf("Retry job mismatch: data=%s, retryCount=%d",
			gotRetryJob.Data, retryJc.RetryCount)
	}

	store.Ack(q, retryJc.JobID)
}

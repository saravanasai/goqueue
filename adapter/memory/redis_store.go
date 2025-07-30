package memory

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/saravanasai/goqueue/config"
	"github.com/saravanasai/goqueue/internal/logger"
	"github.com/saravanasai/goqueue/internal/manager"
	"github.com/saravanasai/goqueue/internal/registry"
	"github.com/saravanasai/goqueue/job"
)

const (
	processingQueueName   = "processing:"
	MetricsQueueSuffix    = ":metrics"
	MetricsAckQueueSuffix = ":metrics:ack"
)

type RedisStore struct {
	client       *redis.Client
	config       config.Config
	redisManager *manager.RedisClientManager
	redisKey     string
	logger       logger.Logger
}

func NewRedisStore(client *redis.Client, config config.Config, redisManager *manager.RedisClientManager, addr string, password string, db int, logger logger.Logger) *RedisStore {

	return &RedisStore{
		client:       client,
		config:       config,
		redisManager: redisManager,
		redisKey:     redisManager.Key(addr, password, db),
		logger:       logger,
	}
}

func (rs *RedisStore) Push(queueName string, jb job.Job) error {

	if rs.redisManager != nil && !rs.redisManager.IsHealthy(rs.redisKey) {
		rs.logger.Error("redis instance is currently unhealthy, cannot push job", "queue", queueName)
		return fmt.Errorf("redis instance is currently unhealthy, cannot push job")
	}

	t := reflect.TypeOf(jb)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	jobName := t.Name()

	// Marshal the actual job separately
	jobPayload, err := json.Marshal(jb)
	if err != nil {
		rs.logger.Error("failed to marshal job", "error", err, "queue", queueName)
		return fmt.Errorf("failed to marshal job: %w", err)
	}

	if jobName == "" {
		rs.logger.Error("could not determine job name from type", "queue", queueName)
		return fmt.Errorf("could not determine job name from type")
	}
	// Create RedisQueuedJob
	meta := job.RedisQueuedJob{
		Job:        jobPayload,
		JobName:    jobName,
		ID:         generateID(),
		EnqueuedAt: time.Now(),
		RetryCount: 0,
	}

	payload, err := json.Marshal(meta)
	if err != nil {
		rs.logger.Error("failed to marshal job metadata", "error", err, "queue", queueName)
		return fmt.Errorf("failed to marshal job metadata: %w", err)
	}

	ctx := context.Background()
	indexKey := "job_index:" + queueName
	rs.client.HSet(ctx, indexKey, meta.ID, payload).Err()
	return rs.client.LPush(ctx, queueName, payload).Err()

}

func (rs *RedisStore) Pop(queueName string) (job.JobContext, error) {
	ctx := context.Background()

	processingQueue := processingQueueName + queueName

	// BRPopLPush blocks until a job is available or context is canceled
	payload, err := rs.client.BRPopLPush(ctx, queueName, processingQueue, 0).Result()
	if err == redis.Nil {
		return job.JobContext{}, nil
	}
	if err != nil {
		rs.logger.Error("redis BRPopLPush error", "error", err, "queue", queueName)
		return job.JobContext{}, fmt.Errorf("redis BRPopLPush error: %w", err)
	}

	var queued job.RedisQueuedJob
	if err := json.Unmarshal([]byte(payload), &queued); err != nil {
		rs.logger.Error("unmarshal RedisQueuedJob error", "error", err, "queue", queueName)
		return job.JobContext{}, fmt.Errorf("unmarshal RedisQueuedJob error: %w", err)
	}

	newJobFunc, ok := registry.GetFromRegistery(queued.JobName)
	if !ok {
		rs.logger.Error("no job registered with name", "jobName", queued.JobName, "queue", queueName)
		return job.JobContext{}, fmt.Errorf("no job registered with name: %s", queued.JobName)
	}

	jobInstance := newJobFunc()
	if err := json.Unmarshal(queued.Job, jobInstance); err != nil {
		rs.logger.Error("failed to decode job into type", "jobName", queued.JobName, "error", err, "queue", queueName)
		return job.JobContext{}, fmt.Errorf("failed to decode job into type %s: %w", queued.JobName, err)
	}

	return job.JobContext{Job: jobInstance, JobID: queued.ID, QueueName: queueName, EnqueuedAt: queued.EnqueuedAt}, nil
}

func (rs *RedisStore) Ack(queueName string, jobID string) error {
	ctx := context.Background()
	processingQueue := processingQueueName + queueName
	indexKey := "job_index:" + queueName

	// Get actual payload using job ID
	payload, err := rs.client.HGet(ctx, indexKey, jobID).Result()
	if err != nil {
		rs.logger.Error("job payload not found for ID", "jobID", jobID, "error", err, "queue", queueName)
		return fmt.Errorf("job payload not found for ID %s: %w", jobID, err)
	}

	// Remove from processing queue
	if _, err := rs.client.LRem(ctx, processingQueue, 1, payload).Result(); err != nil {
		rs.logger.Error("failed to LREM payload", "error", err, "jobID", jobID, "queue", queueName)
		return fmt.Errorf("failed to LREM payload: %w", err)
	}
	// Remove from index
	return rs.client.HDel(ctx, indexKey, jobID).Err()
}

func (rs *RedisStore) Retry(job job.Job, delay time.Duration) error {
	return nil
}

func (r *RedisStore) EnqueueMetrics(metrics config.JobMetrics) error {
	// Create metrics queue name
	metricsQueueName := metrics.QueueName + MetricsQueueSuffix
	// Serialize to JSON
	jsonData, err := json.Marshal(metrics)
	if err != nil {
		r.logger.Error("failed to marshal metrics data", "error", err, "queue", metrics.QueueName)
		return fmt.Errorf("failed to marshal metrics data: %w", err)
	}

	// Push to Redis list (non-blocking)
	ctx := context.Background()
	if err := r.client.LPush(ctx, metricsQueueName, jsonData).Err(); err != nil {
		r.logger.Error("failed to enqueue metrics", "error", err, "queue", metrics.QueueName)
		return fmt.Errorf("failed to enqueue metrics: %w", err)
	}

	return nil
}

func (rs *RedisStore) DequeueMetrics(queueName string) (config.JobMetrics, error) {
	ctx := context.Background()
	processingQueue := queueName + MetricsAckQueueSuffix
	sourceQueueName := queueName + MetricsQueueSuffix
	// BRPopLPush blocks until a job is available or context is canceled
	payload, err := rs.client.BRPopLPush(ctx, sourceQueueName, processingQueue, 0).Result()
	if err == redis.Nil {
		return config.JobMetrics{}, nil
	}
	if err != nil {
		rs.logger.Error("redis BRPopLPush error (metrics)", "error", err, "queue", queueName)
		return config.JobMetrics{}, fmt.Errorf("redis BRPopLPush error: %w", err)
	}

	var metrics config.JobMetrics
	if err := json.Unmarshal([]byte(payload), &metrics); err != nil {
		rs.logger.Error("unmarshal RedisQueuedJob error (metrics)", "error", err, "queue", queueName)
		return config.JobMetrics{}, fmt.Errorf("unmarshal RedisQueuedJob error: %w", err)
	}

	return metrics, nil
}

func (r *RedisStore) IsHealthy() bool {
	return r.redisManager.IsHealthy(r.redisKey)
}

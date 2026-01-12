package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/danish-a1/goqueue/adapter/utils"
	"github.com/danish-a1/goqueue/config"
	"github.com/danish-a1/goqueue/internal/logger"
	"github.com/danish-a1/goqueue/internal/manager"
	"github.com/danish-a1/goqueue/internal/registry"
	"github.com/danish-a1/goqueue/job"
	"github.com/redis/go-redis/v9"
)

const (
	processingQueueName   = "processing:"
	retryQueuePrefix      = "retry:"
	MetricsQueueSuffix    = ":metrics"
	MetricsAckQueueSuffix = ":metrics:ack"
	JobIndexKeyFormat     = "job_index:%s"
	retryPollerInterval   = 1 * time.Second
)

// Lua scripts are embedded from files in adapter/redis/scripts via scripts.go
// Variables available in this package:
//   - moveRetryJobScript
//   - cleanupProcessingJobScript

type RedisStore struct {
	client       *redis.Client
	config       config.Config
	redisManager *manager.RedisClientManager
	redisKey     string
	logger       logger.Logger
	retryPoller  *retryPoller
}

// scheduleDelayedJob adds a job payload to the retry ZSET with the given delay
func (rs *RedisStore) scheduleDelayedJob(ctx context.Context, queueName string, payload interface{}, delay time.Duration) error {
	retryQueueName := retryQueuePrefix + queueName
	retryTimestamp := time.Now().Add(delay).Unix()
	return rs.client.ZAdd(ctx, retryQueueName, redis.Z{
		Score:  float64(retryTimestamp),
		Member: payload,
	}).Err()
}

// retryPoller handles moving jobs from retry queue to main queue
type retryPoller struct {
	store     *RedisStore
	stopCh    chan struct{}
	stoppedCh chan struct{}
}

func NewRedisStore(client *redis.Client, config config.Config, redisManager *manager.RedisClientManager, addr string, db int, logger logger.Logger) *RedisStore {
	rs := &RedisStore{
		client:       client,
		config:       config,
		redisManager: redisManager,
		redisKey:     redisManager.Key(addr, db),
		logger:       logger,
	}

	// Start retry poller
	rs.retryPoller = &retryPoller{
		store:     rs,
		stopCh:    make(chan struct{}),
		stoppedCh: make(chan struct{}),
	}
	go rs.retryPoller.start()

	return rs
}

func (rs *RedisStore) GetDbConnection() interface{} {
	return rs.client
}

// Push supports optional delay. If delay is provided, job is scheduled in retry ZSET.
func (rs *RedisStore) Push(queueName string, jb job.Job, delay ...time.Duration) error {
	if rs.redisManager != nil && !rs.redisManager.IsHealthy(rs.redisKey) {
		rs.logger.Error("redis instance is currently unhealthy, cannot push job", "queue", queueName)
		return fmt.Errorf("redis instance is currently unhealthy, cannot push job")
	}

	jobName := utils.GetJobName(jb)
	jobPayload, err := json.Marshal(jb)
	if err != nil {
		rs.logger.Error("failed to marshal job", "error", err, "queue", queueName)
		return fmt.Errorf("failed to marshal job: %w", err)
	}
	if jobName == "" {
		rs.logger.Error("could not determine job name from type", "queue", queueName)
		return fmt.Errorf("could not determine job name from type")
	}
	meta := job.RedisQueuedJob{
		Job:        jobPayload,
		JobName:    jobName,
		ID:         utils.GenerateID(),
		EnqueuedAt: time.Now(),
		RetryCount: 0,
	}
	payload, err := json.Marshal(meta)
	if err != nil {
		rs.logger.Error("failed to marshal job metadata", "error", err, "queue", queueName)
		return fmt.Errorf("failed to marshal job metadata: %w", err)
	}
	ctx := context.Background()
	indexKey := fmt.Sprintf(JobIndexKeyFormat, queueName)
	if err := rs.client.HSet(ctx, indexKey, meta.ID, payload).Err(); err != nil {
		rs.logger.Error("failed to set job in index", "error", err, "queue", queueName)
		return err
	}
	// If delay is provided and > 0, schedule in retry ZSET
	if len(delay) > 0 && delay[0] > 0 {
		return rs.scheduleDelayedJob(ctx, queueName, payload, delay[0])
	}
	// Immediate job: push to main queue
	return rs.client.LPush(ctx, queueName, payload).Err()
}

// PushBatch adds multiple jobs to the Redis queue, supports optional delay for all jobs.
func (rs *RedisStore) PushBatch(queueName string, jobs []job.Job, delay ...time.Duration) error {
	if rs.redisManager != nil && !rs.redisManager.IsHealthy(rs.redisKey) {
		rs.logger.Error("redis instance is currently unhealthy, cannot push jobs", "queue", queueName)
		return fmt.Errorf("redis instance is currently unhealthy, cannot push jobs")
	}
	ctx := context.Background()
	indexKey := fmt.Sprintf(JobIndexKeyFormat, queueName)
	var payloads []interface{}
	var zPayloads []redis.Z
	useDelay := len(delay) > 0 && delay[0] > 0
	var retryTimestamp int64
	if useDelay {
		retryTimestamp = time.Now().Add(delay[0]).Unix()
	}
	for _, jb := range jobs {
		jobName := utils.GetJobName(jb)
		jobPayload, err := json.Marshal(jb)
		if err != nil {
			rs.logger.Error("failed to marshal job", "error", err, "queue", queueName)
			continue
		}
		if jobName == "" {
			rs.logger.Error("could not determine job name from type", "queue", queueName)
			continue
		}
		meta := job.RedisQueuedJob{
			Job:        jobPayload,
			JobName:    jobName,
			ID:         utils.GenerateID(),
			EnqueuedAt: time.Now(),
			RetryCount: 0,
		}
		payload, err := json.Marshal(meta)
		if err != nil {
			rs.logger.Error("failed to marshal job metadata", "error", err, "queue", queueName)
			continue
		}
		if err := rs.client.HSet(ctx, indexKey, meta.ID, payload).Err(); err != nil {
			rs.logger.Error("failed to set job in index", "error", err, "queue", queueName)
			continue
		}
		if useDelay {
			zPayloads = append(zPayloads, redis.Z{
				Score:  float64(retryTimestamp),
				Member: payload,
			})
		} else {
			payloads = append(payloads, payload)
		}
	}
	if useDelay && len(zPayloads) > 0 {
		retryQueueName := retryQueuePrefix + queueName
		if err := rs.client.ZAdd(ctx, retryQueueName, zPayloads...).Err(); err != nil {
			rs.logger.Error("failed to add jobs to retry queue", "error", err, "queue", queueName)
			return err
		}
		return nil
	}
	if !useDelay && len(payloads) > 0 {
		return rs.client.LPush(ctx, queueName, payloads...).Err()
	}
	return nil
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

	return job.JobContext{Job: jobInstance, JobID: queued.ID, QueueName: queueName, EnqueuedAt: queued.EnqueuedAt, RetryCount: queued.RetryCount}, nil
}

func (rs *RedisStore) Ack(queueName string, jobID string) error {
	ctx := context.Background()
	processingQueue := processingQueueName + queueName
	indexKey := fmt.Sprintf(JobIndexKeyFormat, queueName)

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

// RetryJobWithMetadata pushes a job to the retry queue with retry metadata
func (rs *RedisStore) RetryJobWithMetadata(queueName string, queuedJob job.JobContext, delay ...time.Duration) error {

	if rs.redisManager != nil && !rs.redisManager.IsHealthy(rs.redisKey) {
		rs.logger.Error("redis instance is currently unhealthy, cannot retry job", "queue", queueName)
		return fmt.Errorf("redis instance is currently unhealthy, cannot retry job")
	}

	ctx := context.Background()
	retryQueueName := retryQueuePrefix + queueName
	retryTimestamp := time.Now().Add(delay[0]).Unix()

	// Get job name and serialize the original job
	jobName := utils.GetJobName(queuedJob.Job)
	if jobName == "" {
		rs.logger.Error("could not determine job name from type", "queue", queueName)
		return fmt.Errorf("could not determine job name from type")
	}

	jobPayload, err := json.Marshal(queuedJob.Job)
	if err != nil {
		rs.logger.Error("failed to marshal job for retry", "error", err, "queue", queueName)
		return fmt.Errorf("failed to marshal job for retry: %w", err)
	}

	// Create RedisQueuedJob with incremented retry count
	meta := job.RedisQueuedJob{
		Job:        jobPayload,
		JobName:    jobName,
		ID:         queuedJob.JobID,
		EnqueuedAt: queuedJob.EnqueuedAt,
		RetryCount: queuedJob.RetryCount + 1,
	}

	// Serialize the job with updated metadata
	payload, err := json.Marshal(meta)
	if err != nil {
		rs.logger.Error("failed to marshal job metadata for retry", "error", err, "queue", queueName)
		return fmt.Errorf("failed to marshal job metadata for retry: %w", err)
	}

	// Get the original job payload from processing queue for cleanup
	processingQueue := processingQueueName + queueName
	indexKey := fmt.Sprintf(JobIndexKeyFormat, queueName)

	// Get original payload using job ID for cleanup
	originalPayload, err := rs.client.HGet(ctx, indexKey, queuedJob.JobID).Result()
	if err == nil {
		// Clean up the processing queue (remove the original job)
		rs.client.Eval(ctx, cleanupProcessingJobScript, []string{processingQueue}, originalPayload)
	}

	// Update job in index
	if err := rs.client.HSet(ctx, indexKey, meta.ID, payload).Err(); err != nil {
		rs.logger.Error("failed to set job in index for retry", "error", err, "queue", queueName)
		return fmt.Errorf("failed to set job in index for retry: %w", err)
	}

	// Add to retry sorted set with retry timestamp as score
	rs.logger.Info("job added to retry queue", "queue", queueName, "jobID", meta.ID, "retryCount", meta.RetryCount, "retryAt", retryTimestamp)
	err = rs.client.ZAdd(ctx, retryQueueName, redis.Z{
		Score:  float64(retryTimestamp),
		Member: payload,
	}).Err()

	if err != nil {
		rs.logger.Error("failed to add job to retry queue", "error", err, "queue", queueName)
		return fmt.Errorf("failed to add job to retry queue: %w", err)
	}

	// Update the job index with the new retry payload
	rs.client.HSet(ctx, indexKey, queuedJob.JobID, payload)

	rs.logger.Info("job added to retry queue", "queue", queueName, "jobID", queuedJob.JobID, "retryCount", queuedJob.RetryCount, "retryAt", time.Unix(retryTimestamp, 0))
	return nil
}

// start begins the retry poller goroutine
func (rp *retryPoller) start() {
	defer close(rp.stoppedCh)
	ticker := time.NewTicker(retryPollerInterval)
	defer ticker.Stop()

	rp.store.logger.Info("retry poller started")

	for {
		select {
		case <-rp.stopCh:
			rp.store.logger.Info("retry poller stopping")
			return
		case <-ticker.C:
			rp.processRetryQueues()
		}
	}
}

// stop stops the retry poller
func (rp *retryPoller) stop() {
	close(rp.stopCh)
	<-rp.stoppedCh
}

// processRetryQueues checks all retry queues and moves ready jobs back to main queues
func (rp *retryPoller) processRetryQueues() {
	// Check if Redis is healthy before attempting operations
	if rp.store.redisManager != nil && !rp.store.redisManager.IsHealthy(rp.store.redisKey) {
		// Skip processing if Redis is not healthy
		return
	}

	ctx := context.Background()

	// Get all retry queue keys
	retryKeys, err := rp.store.client.Keys(ctx, retryQueuePrefix+"*").Result()
	if err != nil {
		// Only log error if it's not a connection error during shutdown
		if !isConnectionError(err) {
			rp.store.logger.Error("failed to get retry queue keys", "error", err)
		}
		return
	}

	currentTime := time.Now().Unix()

	for _, retryKey := range retryKeys {
		// Extract main queue name from retry key
		mainQueueName := retryKey[len(retryQueuePrefix):]

		// Use Lua script to atomically move jobs from retry to main queue
		result, err := rp.store.client.Eval(ctx, moveRetryJobScript, []string{retryKey, mainQueueName}, currentTime).Result()
		if err != nil {
			if !isConnectionError(err) {
				rp.store.logger.Error("failed to execute retry job move script", "error", err, "retryQueue", retryKey)
			}
			continue
		}

		if movedCount, ok := result.(int64); ok && movedCount > 0 {
			rp.store.logger.Info("moved jobs from retry queue", "queue", mainQueueName, "count", movedCount)
		}
	}
}

// isConnectionError checks if the error is a connection-related error
func isConnectionError(err error) bool {
	errStr := err.Error()
	return strings.Contains(errStr, "connection") ||
		strings.Contains(errStr, "refused") ||
		strings.Contains(errStr, "broken pipe") ||
		strings.Contains(errStr, "wsarecv") ||
		strings.Contains(errStr, "EOF")
}

// Stop gracefully stops the Redis store and its retry poller
func (rs *RedisStore) Stop() error {
	if rs.retryPoller != nil {
		rs.retryPoller.stop()
	}
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

	// Use a timeout of 1 second to make this non-blocking like the memory store
	// BRPopLPush blocks until a job is available or context timeout
	payload, err := rs.client.BRPopLPush(ctx, sourceQueueName, processingQueue, 1*time.Second).Result()
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

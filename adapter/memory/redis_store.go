package memory

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/saravanasai/goqueue/internal/registry"
	"github.com/saravanasai/goqueue/job"
)

const processingQueueName = "processing:"

type RedisStore struct {
	client *redis.Client
}

func NewRedisStore(addr, password string, db int) *RedisStore {
	rdb := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})

	return &RedisStore{
		client: rdb,
	}
}

func (rs *RedisStore) Push(queueName string, jb job.Job) error {
	t := reflect.TypeOf(jb)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	jobName := t.Name()

	// Marshal the actual job separately
	jobPayload, err := json.Marshal(jb)
	if err != nil {
		return fmt.Errorf("failed to marshal job: %w", err)
	}

	if jobName == "" {
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
		return job.JobContext{}, fmt.Errorf("redis BRPopLPush error: %w", err)
	}

	var queued job.RedisQueuedJob
	if err := json.Unmarshal([]byte(payload), &queued); err != nil {
		return job.JobContext{}, fmt.Errorf("unmarshal RedisQueuedJob error: %w", err)
	}

	newJobFunc, ok := registry.GetFromRegistery(queued.JobName)
	if !ok {
		return job.JobContext{}, fmt.Errorf("no job registered with name: %s", queued.JobName)
	}

	jobInstance := newJobFunc()
	if err := json.Unmarshal(queued.Job, jobInstance); err != nil {
		return job.JobContext{}, fmt.Errorf("failed to decode job into type %s: %w", queued.JobName, err)
	}

	return job.JobContext{Job: jobInstance, JobID: queued.ID, QueueName: queueName}, nil
}

func (rs *RedisStore) Ack(queueName string, jobID string) error {
	ctx := context.Background()
	processingQueue := processingQueueName + queueName
	indexKey := "job_index:" + queueName

	// Get actual payload using job ID
	payload, err := rs.client.HGet(ctx, indexKey, jobID).Result()
	if err != nil {
		return fmt.Errorf("job payload not found for ID %s: %w", jobID, err)
	}

	// Remove from processing queue
	if _, err := rs.client.LRem(ctx, processingQueue, 1, payload).Result(); err != nil {
		return fmt.Errorf("failed to LREM payload: %w", err)
	}
	// Remove from index
	return rs.client.HDel(ctx, indexKey, jobID).Err()
}

func (rs *RedisStore) Retry(job job.Job, delay time.Duration) error {
	return nil
}

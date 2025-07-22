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
	fmt.Println("Raw JB:", jb)
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

	fmt.Println("Marshal Payload:", string(payload))
	return rs.client.LPush(context.Background(), queueName, payload).Err()
}

func (rs *RedisStore) Pop(queueName string) (job.Job, error) {
	ctx := context.Background()

	// BLPop blocks until a job is available or context is canceled
	result, err := rs.client.BLPop(ctx, 0*time.Second, queueName).Result()
	if err == redis.Nil {
		// Queue is empty, no job available
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("redis BLPop error: %w", err)
	}

	if len(result) < 2 {
		return nil, fmt.Errorf("unexpected result from BLPop: %v", result)
	}

	payload := result[1]
	fmt.Println("Payload:", payload)

	// First unmarshal into RedisQueuedJob to get metadata and JobName
	var queued job.RedisQueuedJob
	if err := json.Unmarshal([]byte(payload), &queued); err != nil {
		return nil, fmt.Errorf("unmarshal RedisQueuedJob error: %w", err)
	}

	// Get actual job constructor from registry
	newJobFunc, ok := registry.GetFromRegistery(queued.JobName)
	if !ok {
		return nil, fmt.Errorf("no job registered with name: %s", queued.JobName)
	}

	// Instantiate and unmarshal actual job
	jobInstance := newJobFunc()
	if err := json.Unmarshal(queued.Job, jobInstance); err != nil {
		return nil, fmt.Errorf("failed to decode job into type %s: %w", queued.JobName, err)
	}

	// Return the job instance (which implements job.Job)
	return jobInstance, nil
}

func (rs *RedisStore) Ack(jobID string) error {
	return nil
}
func (rs *RedisStore) Retry(job job.Job, delay time.Duration) error {
	return nil
}

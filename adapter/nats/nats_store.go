package nats

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/saravanasai/goqueue/adapter/utils"
	jobConfig "github.com/saravanasai/goqueue/config"
	"github.com/saravanasai/goqueue/internal/logger"
	"github.com/saravanasai/goqueue/internal/registry"
	"github.com/saravanasai/goqueue/job"
)

const (
	// maxNATSPayloadSize matches JetStream's default max_payload server setting
	maxNATSPayloadSize = 1 * 1024 * 1024 // 1MB
)

// NATSQueuedJob represents a job queued in NATS JetStream
type NATSQueuedJob struct {
	// Job contains the serialized job data
	Job json.RawMessage `json:"job"`
	// ID is a unique identifier for this queued job
	ID string `json:"id"`
	// JobName is the registered name of the job type, used for deserialization
	JobName string `json:"job_name"`
	// EnqueuedAt is the timestamp when the job was added to the queue
	EnqueuedAt time.Time `json:"enqueued_at"`
	// RetryCount is a fallback; Pop derives the real value from delivery metadata
	RetryCount int `json:"retry_count"`
}

// NATSStore is an implementation of the Store interface using NATS JetStream
type NATSStore struct {
	// client is the NATS connection
	client NATSClient
	// js is the JetStream context
	js jetstream.JetStream
	// config contains the queue configuration
	config jobConfig.Config
	// natsConfig contains NATS-specific configuration
	natsConfig jobConfig.NATSConfig
	// logger for errors and debugging
	logger logger.Logger
	// healthStatus tracks connection health
	healthStatus bool
	// maxDeliver is the JetStream delivery cap
	maxDeliver int
	// jobPendingMessages maps jobID to the unacked NATS message
	jobPendingMessages map[string]jetstream.Msg
	// jobPendingMessagesMutex protects jobPendingMessages map from concurrent access
	jobPendingMessagesMutex sync.Mutex
	// consumers caches one pull consumer per queue name
	consumers map[string]jetstream.Consumer
	// consumersMutex protects consumers map from concurrent access
	consumersMutex sync.RWMutex
}

// NATSClient defines the interface for NATS connection operations we use
// This allows us to easily mock the NATS connection for testing
type NATSClient interface {
	Status() nats.Status
}

// applyNATSDefaults fills in zero-valued NATSConfig fields and derives maxDeliver
func applyNATSDefaults(cfg jobConfig.Config, natsCfg jobConfig.NATSConfig, logger logger.Logger) (jobConfig.NATSConfig, int) {
	if natsCfg.AckWait <= 0 {
		natsCfg.AckWait = 30 * time.Second
	}
	if natsCfg.MaxMessages <= 0 {
		natsCfg.MaxMessages = 1
	}
	if natsCfg.RequestTimeout <= 0 {
		natsCfg.RequestTimeout = 10 * time.Second
	}

	maxDeliver := natsCfg.MaxDeliver
	if maxDeliver <= 0 {
		maxDeliver = cfg.MaxRetryAttempts + 2
	} else if maxDeliver <= cfg.MaxRetryAttempts {
		logger.Error("NATS MaxDeliver should exceed MaxRetryAttempts, using safe default",
			"MaxDeliver", maxDeliver, "MaxRetryAttempts", cfg.MaxRetryAttempts)
		maxDeliver = cfg.MaxRetryAttempts + 2
	}
	return natsCfg, maxDeliver
}

// NewNATSStore creates a new NATS JetStream-backed store implementation
func NewNATSStore(cfg jobConfig.Config, logger logger.Logger) (*NATSStore, error) {
	natsCfg, ok := cfg.DriverConfig.(jobConfig.NATSConfig)
	if !ok {
		return nil, fmt.Errorf("invalid NATS JetStream config provided")
	}
	natsCfg, maxDeliver := applyNATSDefaults(cfg, natsCfg, logger)

	nc, err := nats.Connect(natsCfg.URL)
	if err != nil {
		logger.Error("Failed to connect to NATS", "error", err)
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		logger.Error("Failed to initialize JetStream", "error", err)
		return nil, fmt.Errorf("failed to initialize JetStream: %w", err)
	}

	streamCtx, streamCancel := context.WithTimeout(context.Background(), natsCfg.RequestTimeout)
	defer streamCancel()
	_, err = js.CreateOrUpdateStream(streamCtx, jetstream.StreamConfig{
		Name:              natsCfg.StreamName,
		Subjects:          []string{natsCfg.StreamName + ".>"},
		AllowMsgSchedules: true,
		Retention:         jetstream.WorkQueuePolicy,
	})
	if err != nil {
		logger.Error("Failed to create or update NATS stream", "error", err, "stream", natsCfg.StreamName)
		return nil, fmt.Errorf("failed to create or update NATS stream: %w", err)
	}

	return &NATSStore{
		client:             nc,
		js:                 js,
		config:             cfg,
		natsConfig:         natsCfg,
		logger:             logger,
		healthStatus:       true,
		maxDeliver:         maxDeliver,
		jobPendingMessages: make(map[string]jetstream.Msg),
		consumers:          make(map[string]jetstream.Consumer),
	}, nil
}

// NewNATSStoreWithClient creates a new NATS store with a custom client
// This is primarily used for testing with mock clients
func NewNATSStoreWithClient(client NATSClient, js jetstream.JetStream, cfg jobConfig.Config, natsCfg jobConfig.NATSConfig, logger logger.Logger) *NATSStore {
	natsCfg, maxDeliver := applyNATSDefaults(cfg, natsCfg, logger)
	return &NATSStore{
		client:             client,
		js:                 js,
		config:             cfg,
		natsConfig:         natsCfg,
		logger:             logger,
		healthStatus:       true,
		maxDeliver:         maxDeliver,
		jobPendingMessages: make(map[string]jetstream.Msg),
		consumers:          make(map[string]jetstream.Consumer),
	}
}

func (n *NATSStore) GetDbConnection() interface{} {
	return nil
}

// Push adds a single job to the queue, with optional delay
func (n *NATSStore) Push(queueName string, jb job.Job, delay ...time.Duration) error {
	// Get job name from type
	jobName := utils.GetJobName(jb)
	if jobName == "" {
		return fmt.Errorf("could not determine job name from type")
	}

	// Marshal the job data
	jobData, err := json.Marshal(jb)
	if err != nil {
		n.logger.Error("Failed to marshal job", "error", err, "queue", queueName)
		return fmt.Errorf("failed to marshal job: %w", err)
	}

	if len(jobData) > maxNATSPayloadSize {
		n.logger.Error("Job payload exceeds NATS JetStream max payload size", "size", len(jobData), "queue", queueName)
		return fmt.Errorf("job payload size (%d bytes) exceeds NATS JetStream default max payload of %d bytes", len(jobData), maxNATSPayloadSize)
	}

	// Create a job ID
	jobID := utils.GenerateID()

	// Create the NATS message
	natsJob := NATSQueuedJob{
		Job:        jobData,
		ID:         jobID,
		JobName:    jobName,
		EnqueuedAt: time.Now(),
		RetryCount: 0,
	}

	messageBody, err := json.Marshal(natsJob)
	if err != nil {
		n.logger.Error("Failed to marshal NATS job", "error", err, "queue", queueName)
		return fmt.Errorf("failed to marshal NATS job: %w", err)
	}

	targetSubject := n.natsConfig.StreamName + "." + queueName

	ctx, cancel := context.WithTimeout(context.Background(), n.natsConfig.RequestTimeout)
	defer cancel()

	if len(delay) > 0 && delay[0] > 0 {
		fireAt := time.Now().Add(delay[0]).UTC().Format(time.RFC3339)
		// Per-job subject avoids one pending schedule overwriting another
		scheduleSubject := n.natsConfig.StreamName + ".schedules." + queueName + "." + jobID

		msg := &nats.Msg{
			Subject: scheduleSubject,
			Data:    messageBody,
			Header:  nats.Header{},
		}
		msg.Header.Set("Nats-Schedule", "@at "+fireAt)
		msg.Header.Set("Nats-Schedule-Target", targetSubject)

		_, err = n.js.PublishMsg(ctx, msg)
	} else {
		_, err = n.js.Publish(ctx, targetSubject, messageBody)
	}

	if err != nil {
		n.healthStatus = false
		n.logger.Error("Failed to publish to NATS", "error", err, "queue", queueName)
		return fmt.Errorf("failed to publish to NATS: %w", err)
	}

	return nil
}

// PushBatch adds multiple jobs to the queue in a single call, with optional delay
func (n *NATSStore) PushBatch(queueName string, jobs []job.Job, delay ...time.Duration) error {
	return n.pushBatch(queueName, jobs, delay...)
}

// pushBatch publishes each job asynchronously, then waits for all acks before returning
func (n *NATSStore) pushBatch(queueName string, jobs []job.Job, delay ...time.Duration) error {
	targetSubject := n.natsConfig.StreamName + "." + queueName

	var useDelay bool
	var fireAt string
	if len(delay) > 0 && delay[0] > 0 {
		useDelay = true
		fireAt = time.Now().Add(delay[0]).UTC().Format(time.RFC3339)
	}

	type pendingAck struct {
		future jetstream.PubAckFuture
		jobID  string
	}
	var futures []pendingAck

	for _, jb := range jobs {
		jobName := utils.GetJobName(jb)
		if jobName == "" {
			n.logger.Error("Could not determine job name from type", "queue", queueName)
			continue
		}

		jobData, err := json.Marshal(jb)
		if err != nil {
			n.logger.Error("Failed to marshal job", "error", err, "queue", queueName)
			continue
		}

		if len(jobData) > maxNATSPayloadSize {
			n.logger.Error("Job payload exceeds NATS JetStream max payload size", "size", len(jobData), "queue", queueName)
			continue
		}

		jobID := utils.GenerateID()
		natsJob := NATSQueuedJob{
			Job:        jobData,
			ID:         jobID,
			JobName:    jobName,
			EnqueuedAt: time.Now(),
			RetryCount: 0,
		}
		messageBody, err := json.Marshal(natsJob)
		if err != nil {
			n.logger.Error("Failed to marshal NATS job", "error", err, "queue", queueName)
			continue
		}

		var future jetstream.PubAckFuture
		if useDelay {
			scheduleSubject := n.natsConfig.StreamName + ".schedules." + queueName + "." + jobID
			msg := &nats.Msg{
				Subject: scheduleSubject,
				Data:    messageBody,
				Header:  nats.Header{},
			}
			msg.Header.Set("Nats-Schedule", "@at "+fireAt)
			msg.Header.Set("Nats-Schedule-Target", targetSubject)
			future, err = n.js.PublishMsgAsync(msg)
		} else {
			future, err = n.js.PublishAsync(targetSubject, messageBody)
		}
		if err != nil {
			n.logger.Error("Failed to publish job asynchronously", "error", err, "queue", queueName)
			continue
		}
		futures = append(futures, pendingAck{future: future, jobID: jobID})
	}

	if len(futures) == 0 {
		return nil
	}

	// Wait for the server to ack every outstanding publish, or time out.
	select {
	case <-n.js.PublishAsyncComplete():
	case <-time.After(30 * time.Second):
		n.logger.Error("Timed out waiting for batch publish acks", "queue", queueName)
	}

	var failed int
	for _, p := range futures {
		select {
		case <-p.future.Ok():
		case err := <-p.future.Err():
			n.healthStatus = false
			n.logger.Error("Failed to publish job in batch", "error", err, "queue", queueName, "jobID", p.jobID)
			failed++
		default:
			failed++
		}
	}

	if failed > 0 {
		return fmt.Errorf("some jobs in the batch failed to publish (%d of %d)", failed, len(futures))
	}

	return nil
}

// getOrCreateConsumer returns the cached pull consumer for a queue, creating it on first use
func (n *NATSStore) getOrCreateConsumer(queueName string) (jetstream.Consumer, error) {
	n.consumersMutex.RLock()
	if cons, ok := n.consumers[queueName]; ok {
		n.consumersMutex.RUnlock()
		return cons, nil
	}
	n.consumersMutex.RUnlock()

	targetSubject := n.natsConfig.StreamName + "." + queueName

	ctx, cancel := context.WithTimeout(context.Background(), n.natsConfig.RequestTimeout)
	defer cancel()

	cons, err := n.js.CreateOrUpdateConsumer(ctx, n.natsConfig.StreamName, jetstream.ConsumerConfig{
		Durable:       queueName + "-consumer",
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       n.natsConfig.AckWait,
		MaxDeliver:    n.maxDeliver,
		FilterSubject: targetSubject,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create or update NATS consumer: %w", err)
	}

	// Re-check the cache before storing
	n.consumersMutex.Lock()
	defer n.consumersMutex.Unlock()

	if existing, ok := n.consumers[queueName]; ok {
		return existing, nil
	}

	n.consumers[queueName] = cons
	return cons, nil
}

// Pop retrieves a job from the queue
func (n *NATSStore) Pop(queueName string) (job.JobContext, error) {
	cons, err := n.getOrCreateConsumer(queueName)
	if err != nil {
		n.healthStatus = false
		n.logger.Error("Failed to get NATS consumer", "error", err, "queue", queueName)
		return job.JobContext{}, fmt.Errorf("failed to get NATS consumer: %w", err)
	}

	msg, err := cons.Next(jetstream.FetchMaxWait(20 * time.Second))
	if err != nil {
		if errors.Is(err, jetstream.ErrNoMessages) || errors.Is(err, nats.ErrTimeout) {
			// Queue was simply empty for the whole wait window, but not a failure
			return job.JobContext{}, nil
		}

		n.healthStatus = false
		n.logger.Error("Failed to fetch message from NATS", "error", err, "queue", queueName)
		return job.JobContext{}, fmt.Errorf("failed to fetch message from NATS: %w", err)
	}

	// Parse the message body
	var natsJob NATSQueuedJob
	if err := json.Unmarshal(msg.Data(), &natsJob); err != nil {
		n.logger.Error("Failed to unmarshal NATS job", "error", err, "queue", queueName)
		return job.JobContext{}, fmt.Errorf("failed to unmarshal NATS job: %w", err)
	}

	// Get the job type constructor
	newJobFunc, ok := registry.GetFromRegistery(natsJob.JobName)
	if !ok {
		n.logger.Error("No job registered with name", "jobName", natsJob.JobName, "queue", queueName)
		return job.JobContext{}, fmt.Errorf("no job registered with name: %s", natsJob.JobName)
	}

	// Create a new job instance and unmarshal the job data
	jobInstance := newJobFunc()
	if jobInstance == nil {
		n.logger.Error("Job constructor returned nil", "jobName", natsJob.JobName, "queue", queueName)
		return job.JobContext{}, fmt.Errorf("job constructor returned nil for jobName: %s", natsJob.JobName)
	}
	if err := json.Unmarshal(natsJob.Job, jobInstance); err != nil {
		n.logger.Error("Failed to decode job into type", "jobName", natsJob.JobName, "error", err, "queue", queueName)
		return job.JobContext{}, fmt.Errorf("failed to decode job into type %s: %w", natsJob.JobName, err)
	}

	// Derive retry count from delivery metadata; NumDelivered starts at 1
	retryCount := natsJob.RetryCount
	if meta, metaErr := msg.Metadata(); metaErr == nil && meta.NumDelivered > 0 {
		retryCount = int(meta.NumDelivered) - 1
	}

	n.jobPendingMessagesMutex.Lock()
	n.jobPendingMessages[natsJob.ID] = msg
	n.jobPendingMessagesMutex.Unlock()

	// Create and return the job context
	return job.JobContext{
		Job:        jobInstance,
		JobID:      natsJob.ID,
		QueueName:  queueName,
		EnqueuedAt: natsJob.EnqueuedAt,
		RetryCount: retryCount,
	}, nil
}

// Ack acknowledges a job has been processed
func (n *NATSStore) Ack(queueName string, jobID string) error {
	n.jobPendingMessagesMutex.Lock()
	msg, ok := n.jobPendingMessages[jobID]
	n.jobPendingMessagesMutex.Unlock()

	if !ok {
		n.logger.Error("Pending message not found for job ID", "jobID", jobID, "queue", queueName)
		return fmt.Errorf("pending message not found for job ID: %s", jobID)
	}

	if err := msg.Ack(); err != nil {
		n.healthStatus = false
		n.logger.Error("Failed to ack NATS message", "error", err, "jobID", jobID, "queue", queueName)
		return fmt.Errorf("failed to ack NATS message: %w", err)
	}

	// Remove the mapping after successful ack
	n.jobPendingMessagesMutex.Lock()
	delete(n.jobPendingMessages, jobID)
	n.jobPendingMessagesMutex.Unlock()

	n.logger.Info("Job acknowledged and removed from NATS", "jobID", jobID, "queue", queueName)
	return nil
}

// Retry is a stub - use RetryJobWithMetadata instead
func (n *NATSStore) Retry(jb job.Job, delay time.Duration) error {
	return fmt.Errorf("retry not implemented for NATS driver - use RetryJobWithMetadata")
}

// RetryJobWithMetadata handles job retries using JetStream's NakWithDelay
func (n *NATSStore) RetryJobWithMetadata(queueName string, queuedJob job.JobContext, delay ...time.Duration) error {
	n.jobPendingMessagesMutex.Lock()
	msg, ok := n.jobPendingMessages[queuedJob.JobID]
	n.jobPendingMessagesMutex.Unlock()

	if !ok {
		n.logger.Error("Pending message not found for job ID", "jobID", queuedJob.JobID, "queue", queueName)
		return fmt.Errorf("pending message not found for job ID: %s", queuedJob.JobID)
	}

	// delay is variadic
	var d time.Duration
	if len(delay) > 0 {
		d = delay[0]
	}

	if err := msg.NakWithDelay(d); err != nil {
		n.healthStatus = false
		n.logger.Error("Failed to nak NATS message for retry", "error", err, "jobID", queuedJob.JobID, "queue", queueName)
		return fmt.Errorf("failed to nak NATS message for retry: %w", err)
	}

	// Remove the mapping after successful nak
	n.jobPendingMessagesMutex.Lock()
	delete(n.jobPendingMessages, queuedJob.JobID)
	n.jobPendingMessagesMutex.Unlock()

	n.logger.Info("Job scheduled for retry via NakWithDelay",
		"jobID", queuedJob.JobID,
		"queue", queueName,
		"retryCount", queuedJob.RetryCount+1,
		"delay", d,
	)

	return nil
}

// EnqueueMetrics adds job metrics to a metrics queue
func (n *NATSStore) EnqueueMetrics(metrics jobConfig.JobMetrics) error {
	// Serialize metrics to JSON
	jsonData, err := json.Marshal(metrics)
	if err != nil {
		n.logger.Error("Failed to marshal metrics data", "error", err, "queue", metrics.QueueName)
		return fmt.Errorf("failed to marshal metrics data: %w", err)
	}

	metricsSubject := n.natsConfig.StreamName + "." + metrics.QueueName + "-metrics"

	ctx, cancel := context.WithTimeout(context.Background(), n.natsConfig.RequestTimeout)
	defer cancel()

	// Publish metrics to NATS
	if _, err := n.js.Publish(ctx, metricsSubject, jsonData); err != nil {
		n.healthStatus = false
		n.logger.Error("Failed to enqueue metrics", "error", err, "queue", metrics.QueueName)
		return fmt.Errorf("failed to enqueue metrics: %w", err)
	}

	return nil
}

// DequeueMetrics retrieves job metrics from a metrics queue
func (n *NATSStore) DequeueMetrics(queueName string) (jobConfig.JobMetrics, error) {
	// Create metrics queue name
	metricsQueueName := queueName + "-metrics"

	cons, err := n.getOrCreateConsumer(metricsQueueName)
	if err != nil {
		n.healthStatus = false
		n.logger.Error("Failed to get NATS metrics consumer", "error", err, "queue", queueName)
		return jobConfig.JobMetrics{}, fmt.Errorf("failed to get NATS metrics consumer: %w", err)
	}

	// Receive a message from the metrics queue
	msg, err := cons.Next(jetstream.FetchMaxWait(1 * time.Second))
	if err != nil {
		// If no messages, return empty metrics
		if errors.Is(err, jetstream.ErrNoMessages) || errors.Is(err, nats.ErrTimeout) {
			return jobConfig.JobMetrics{}, nil
		}

		n.logger.Error("Failed to fetch metrics from NATS", "error", err, "queue", queueName)
		return jobConfig.JobMetrics{}, fmt.Errorf("failed to fetch metrics from NATS: %w", err)
	}

	// Parse the message body
	var metrics jobConfig.JobMetrics
	if err := json.Unmarshal(msg.Data(), &metrics); err != nil {
		n.logger.Error("Failed to unmarshal metrics", "error", err, "queue", queueName)
		return jobConfig.JobMetrics{}, fmt.Errorf("failed to unmarshal metrics: %w", err)
	}

	if err := msg.Ack(); err != nil {
		n.logger.Error("Failed to ack metrics message", "error", err, "queue", queueName)
	}

	return metrics, nil
}

// IsHealthy returns the last known health status of the NATS connection.
// It does not perform a network call; Status() reads connection state
// tracked by the client's read loop. Operational methods (e.g., Push/Pop)
// update n.healthStatus when errors occur.
func (n *NATSStore) IsHealthy() bool {
	return n.healthStatus && n.client != nil && n.client.Status() == nats.CONNECTED
}

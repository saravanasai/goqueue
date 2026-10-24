package worker

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/saravanasai/goqueue/adapter"
	"github.com/saravanasai/goqueue/config"
	"github.com/saravanasai/goqueue/internal/logger"
	"github.com/saravanasai/goqueue/job"
)

// emptyQueueStore reproduces how the database store reports an empty queue:
// a zero JobContext with a nil error. It counts Pop calls.
type emptyQueueStore struct {
	adapter.Store
	pops atomic.Int64
	err  error
}

func (s *emptyQueueStore) Pop(string) (job.JobContext, error) {
	s.pops.Add(1)
	return job.JobContext{}, s.err
}

func (s *emptyQueueStore) DequeueMetrics(string) (config.JobMetrics, error) {
	return config.JobMetrics{}, context.Canceled
}

func (s *emptyQueueStore) EnqueueMetrics(config.JobMetrics) error { return nil }

// runIdleWorker runs one worker against store for window and returns how many
// Pop calls it made.
func runIdleWorker(t *testing.T, store *emptyQueueStore, pollInterval, window time.Duration) int64 {
	t.Helper()
	w := NewWorker(store, config.Config{
		Driver:           config.DriverDatabase,
		MaxWorkers:       1,
		ConcurrencyLimit: 1,
		PollInterval:     pollInterval,
	}, "idle_queue", nil, logger.NewZapLogger())

	ctx, cancel := context.WithCancel(context.Background())
	if err := w.Start(ctx, 1); err != nil {
		t.Fatalf("start worker: %v", err)
	}
	time.Sleep(window)
	cancel()
	w.wg.Wait()
	return store.pops.Load()
}

// An idle worker must pause between polls. Before the fix an empty queue
// (nil error, nil job) skipped the wait and Pop ran in a tight loop.
func TestIdleWorkerBacksOffOnEmptyQueue(t *testing.T) {
	const window = 500 * time.Millisecond
	pops := runIdleWorker(t, &emptyQueueStore{}, 50*time.Millisecond, window)

	// 500ms / 50ms is about 10 polls. The ceiling is generous yet far below the
	// millions a spinning loop makes.
	if pops > 30 {
		t.Fatalf("idle worker polled %d times in %v with a 50ms interval; it is spinning", pops, window)
	}
	if pops == 0 {
		t.Fatal("idle worker never polled")
	}
}

func TestIdleWorkerBacksOffOnPopError(t *testing.T) {
	const window = 500 * time.Millisecond
	pops := runIdleWorker(t, &emptyQueueStore{err: context.DeadlineExceeded}, 50*time.Millisecond, window)

	if pops > 30 {
		t.Fatalf("idle worker polled %d times in %v after Pop errors; it is spinning", pops, window)
	}
}

func TestIdleWorkerUsesDefaultPollIntervalWhenUnset(t *testing.T) {
	const window = 700 * time.Millisecond
	pops := runIdleWorker(t, &emptyQueueStore{}, 0, window)

	// DefaultPollInterval is 500ms: one poll at start, one after the first wait.
	if pops > 4 {
		t.Fatalf("idle worker polled %d times in %v with the default interval; it is spinning", pops, window)
	}
}

func TestIdleWorkerStopsPromptlyOnCancel(t *testing.T) {
	w := NewWorker(&emptyQueueStore{}, config.Config{
		Driver:           config.DriverDatabase,
		MaxWorkers:       1,
		ConcurrencyLimit: 1,
		PollInterval:     time.Hour,
	}, "idle_queue", nil, logger.NewZapLogger())

	ctx, cancel := context.WithCancel(context.Background())
	if err := w.Start(ctx, 1); err != nil {
		t.Fatalf("start worker: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	stopped := make(chan struct{})
	go func() {
		cancel()
		w.wg.Wait()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not stop within 2s of cancel while waiting for the next poll")
	}
}

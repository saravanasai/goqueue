package stats

import (
	"sync"
	"sync/atomic"
	"time"
)

// Collector tracks and computes queue statistics
type Collector struct {
	// Atomic counters
	queuedCount     atomic.Int64
	processingCount atomic.Int64
	completedCount  atomic.Int64
	failedCount     atomic.Int64

	// Sample data for calculating rates and averages
	mu             sync.RWMutex
	enqueueHistory []time.Time
	dequeueHistory []time.Time
	waitTimes      []time.Duration
	processTimes   []time.Duration

	// Configuration
	options StatsOptions

	// Last update time
	lastUpdated time.Time
}

// NewCollector creates a new stats collector with the provided options
func NewCollector(options StatsOptions) *Collector {
	return &Collector{
		enqueueHistory: make([]time.Time, 0, options.MaxSampleSize),
		dequeueHistory: make([]time.Time, 0, options.MaxSampleSize),
		waitTimes:      make([]time.Duration, 0, options.MaxSampleSize),
		processTimes:   make([]time.Duration, 0, options.MaxSampleSize),
		options:        options,
		lastUpdated:    time.Now(),
	}
}

// RecordEnqueue records a job being added to the queue
func (c *Collector) RecordEnqueue() {
	if !c.options.Enabled {
		return
	}

	c.queuedCount.Add(1)

	c.mu.Lock()
	c.enqueueHistory = append(c.enqueueHistory, time.Now())
	if len(c.enqueueHistory) > c.options.MaxSampleSize {
		c.enqueueHistory = c.enqueueHistory[1:]
	}
	c.mu.Unlock()
}

// RecordDequeue records a job being taken from the queue
func (c *Collector) RecordDequeue(enqueuedAt time.Time) {
	if !c.options.Enabled {
		return
	}

	c.queuedCount.Add(-1)
	c.processingCount.Add(1)

	now := time.Now()

	c.mu.Lock()
	c.dequeueHistory = append(c.dequeueHistory, now)
	if len(c.dequeueHistory) > c.options.MaxSampleSize {
		c.dequeueHistory = c.dequeueHistory[1:]
	}

	// Calculate wait time if we have enqueue time
	if !enqueuedAt.IsZero() {
		waitTime := now.Sub(enqueuedAt)
		c.waitTimes = append(c.waitTimes, waitTime)
		if len(c.waitTimes) > c.options.MaxSampleSize {
			c.waitTimes = c.waitTimes[1:]
		}
	}
	c.mu.Unlock()
}

// RecordComplete records a job completing (success or failure)
func (c *Collector) RecordComplete(processingTime time.Duration, success bool) {
	if !c.options.Enabled {
		return
	}

	c.processingCount.Add(-1)
	if success {
		c.completedCount.Add(1)
	} else {
		c.failedCount.Add(1)
	}

	c.mu.Lock()
	c.processTimes = append(c.processTimes, processingTime)
	if len(c.processTimes) > c.options.MaxSampleSize {
		c.processTimes = c.processTimes[1:]
	}
	c.mu.Unlock()
}

// GetStats returns the current queue statistics
func (c *Collector) GetStats(isHealthy bool) QueueStats {
	if !c.options.Enabled {
		return QueueStats{
			IsHealthy:   isHealthy,
			LastUpdated: time.Now(),
		}
	}

	now := time.Now()
	cutoff := now.Add(-c.options.HistoryWindow)

	c.mu.RLock()
	defer c.mu.RUnlock()

	// Filter data within time window
	recentEnqueues := filterRecent(c.enqueueHistory, cutoff)
	recentDequeues := filterRecent(c.dequeueHistory, cutoff)

	// Calculate rates (per second)
	enqueueRate := float64(len(recentEnqueues)) / max(1.0, c.options.HistoryWindow.Seconds())
	dequeueRate := float64(len(recentDequeues)) / max(1.0, c.options.HistoryWindow.Seconds())

	// Calculate averages
	avgWaitTime := average(c.waitTimes)
	avgProcessTime := average(c.processTimes)

	// Load counter values
	queuedJobs := c.queuedCount.Load()
	processingJobs := c.processingCount.Load()

	// Determine if queue is overloaded
	isOverloaded := queuedJobs > c.options.OverloadThreshold
	if processingJobs > 0 {
		ratio := float64(queuedJobs) / float64(processingJobs)
		isOverloaded = isOverloaded || ratio > c.options.OverloadRatio
	}

	c.lastUpdated = now

	return QueueStats{
		QueuedJobs:     queuedJobs,
		ProcessingJobs: processingJobs,
		CompletedJobs:  c.completedCount.Load(),
		FailedJobs:     c.failedCount.Load(),
		EnqueueRate:    enqueueRate,
		DequeueRate:    dequeueRate,
		AvgWaitTime:    avgWaitTime,
		AvgProcessTime: avgProcessTime,
		IsHealthy:      isHealthy,
		IsOverloaded:   isOverloaded,
		LastUpdated:    now,
	}
}

func filterRecent(times []time.Time, cutoff time.Time) []time.Time {
	result := make([]time.Time, 0, len(times))
	for _, t := range times {
		if t.After(cutoff) {
			result = append(result, t)
		}
	}
	return result
}

func average(durations []time.Duration) time.Duration {
	if len(durations) == 0 {
		return 0
	}

	var sum time.Duration
	for _, d := range durations {
		sum += d
	}
	return sum / time.Duration(len(durations))
}

func max(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

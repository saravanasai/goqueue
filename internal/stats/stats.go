package stats

import (
	"time"
)

// QueueStats represents the current health metrics of a queue
type QueueStats struct {
	// Queue size metrics
	QueuedJobs     int64 `json:"queued_jobs"`
	ProcessingJobs int64 `json:"processing_jobs"`
	CompletedJobs  int64 `json:"completed_jobs"`
	FailedJobs     int64 `json:"failed_jobs"`

	// Performance metrics
	EnqueueRate    float64       `json:"enqueue_rate"`     // Jobs/second
	DequeueRate    float64       `json:"dequeue_rate"`     // Jobs/second
	AvgWaitTime    time.Duration `json:"avg_wait_time"`    // Time in queue
	AvgProcessTime time.Duration `json:"avg_process_time"` // Processing duration

	// Health indicators
	IsHealthy    bool      `json:"is_healthy"`
	IsOverloaded bool      `json:"is_overloaded"`
	LastUpdated  time.Time `json:"last_updated"`
}

// StatsOptions configures how the stats collector operates
type StatsOptions struct {
	Enabled           bool          // Whether stats collection is enabled
	HistoryWindow     time.Duration // Time window for rate calculations (default: 1 minute)
	MaxSampleSize     int           // Maximum samples to keep (prevent memory bloat)
	OverloadThreshold int64         // Jobs in queue that indicates overload
	OverloadRatio     float64       // Ratio of queued:processing jobs that indicates overload
}

// DefaultStatsOptions returns sensible defaults for stats collection
func DefaultStatsOptions() StatsOptions {
	return StatsOptions{
		Enabled:           false,
		HistoryWindow:     time.Minute,
		MaxSampleSize:     500,
		OverloadThreshold: 1000,
		OverloadRatio:     5.0,
	}
}

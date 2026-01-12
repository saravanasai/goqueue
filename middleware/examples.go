package middleware

import (
	"context"
	"fmt"

	"github.com/danish-a1/goqueue/internal/logger"
	"github.com/danish-a1/goqueue/job"
)

// LoggingMiddleware creates a middleware that logs job execution details.
//
// This middleware logs when jobs start and finish processing, along with
// relevant context such as job ID, queue name, and timestamps.
// On failure, it logs the error details for debugging.
//
// Parameters:
//   - logger: A logger instance that implements the logger.Logger interface
//
// Returns:
//   - A middleware function that can be added to the processing chain
func LoggingMiddleware(logger logger.Logger) Middleware {
	return func(next HandlerFunc) HandlerFunc {
		return func(ctx context.Context, jobCtx *job.JobContext) error {
			logger.Info("Starting job execution",
				"jobID", jobCtx.JobID,
				"queue", jobCtx.QueueName,
				"enqueuedAt", jobCtx.EnqueuedAt,
			)

			err := next(ctx, jobCtx)

			if err != nil {
				logger.Error("Job execution failed",
					"jobID", jobCtx.JobID,
					"queue", jobCtx.QueueName,
					"error", err,
				)
			} else {
				logger.Info("Job execution completed",
					"jobID", jobCtx.JobID,
					"queue", jobCtx.QueueName,
				)
			}

			return err
		}
	}
}

// ConditionalSkipMiddleware creates a middleware that conditionally skips job execution.
//
// This middleware evaluates each job against the provided condition function
// and skips processing if the condition returns true. This can be used to
// implement filtering, rate limiting, or time-based execution rules.
//
// Parameters:
//   - shouldSkip: A function that takes a JobContext and returns true if the job should be skipped
//
// Returns:
//   - A middleware function that can be added to the processing chain
func ConditionalSkipMiddleware(shouldSkip func(*job.JobContext) bool) Middleware {
	return func(next HandlerFunc) HandlerFunc {
		return func(ctx context.Context, jobCtx *job.JobContext) error {
			if shouldSkip(jobCtx) {
				fmt.Printf("Skipping job %s based on condition\n", jobCtx.JobID)
				return nil
			}
			return next(ctx, jobCtx)
		}
	}
}

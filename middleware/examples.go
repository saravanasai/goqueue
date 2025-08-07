package middleware

import (
	"context"
	"fmt"

	"github.com/saravanasai/goqueue/internal/logger"
	"github.com/saravanasai/goqueue/job"
)

// LoggingMiddleware creates a middleware that logs job execution details
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

// ConditionalSkipMiddleware creates a middleware that conditionally skips job execution
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

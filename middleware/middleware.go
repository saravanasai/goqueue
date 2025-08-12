// Package middleware provides a flexible middleware system for customizing job processing behavior.
// It implements a chain-of-responsibility pattern where each middleware can perform actions
// before and after job processing, or modify the processing itself.
package middleware

import (
	"context"
	"fmt"

	"github.com/saravanasai/goqueue/job"
)

// HandlerFunc defines the function signature for processing a job.
// It takes a context and job context, and returns an error if processing fails.
// This is the core type that middlewares wrap and enhance.
type HandlerFunc func(ctx context.Context, jobCtx *job.JobContext) error

// Middleware defines a function that wraps one HandlerFunc to create a new one.
// This allows middleware to perform actions before and after the wrapped handler executes,
// modify the context or job, handle errors, or even prevent the handler from executing.
//
// Example middleware that logs job execution:
//
//	func LoggingMiddleware(logger Logger) Middleware {
//	    return func(next HandlerFunc) HandlerFunc {
//	        return func(ctx context.Context, jobCtx *job.JobContext) error {
//	            logger.Info("Starting job", "id", jobCtx.JobID)
//	            err := next(ctx, jobCtx)
//	            if err != nil {
//	                logger.Error("Job failed", "id", jobCtx.JobID, "error", err)
//	            }
//	            return err
//	        }
//	    }
//	}
type Middleware func(next HandlerFunc) HandlerFunc

// Chain creates a new HandlerFunc by chaining the given middlewares together.
// The middlewares are executed in the order they appear in the slice, with each
// middleware wrapping all subsequent middlewares. The DefaultHandler is automatically
// added as the final handler in the chain.
//
// Example usage:
//
//	handler := Chain(
//	    LoggingMiddleware(logger),    // Executes first
//	    MetricsMiddleware(),          // Executes second
//	    ValidationMiddleware(),       // Executes third
//	)   // DefaultHandler executes last
//
// The execution flow for a single job would be:
// LoggingMiddleware
//   → MetricsMiddleware
//     → ValidationMiddleware
//       → DefaultHandler (executes job.Process)
//     ← ValidationMiddleware
//   ← MetricsMiddleware
// ← LoggingMiddleware
func Chain(middlewares ...Middleware) HandlerFunc {
	// Start with the DefaultHandler
	next := DefaultHandler
	// Apply middlewares in reverse order
	// This ensures the first middleware in the slice is the outermost wrapper
	for i := len(middlewares) - 1; i >= 0; i-- {
		next = middlewares[i](next)
	}

	return next
}

// DefaultHandler is the base handler that executes the job's Process method.
// This is always the last handler in the chain and actually processes the job.
// It simply calls the Process method of the job implementation.
func DefaultHandler(ctx context.Context, jobCtx *job.JobContext) error {
	if jobCtx == nil {
		return fmt.Errorf("job context is nil")
	}
	if jobCtx.Job == nil {
		return fmt.Errorf("job is nil")
	}
	return jobCtx.Job.Process(ctx)
}

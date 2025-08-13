# GoQueue Architecture

## Goal
Provide a simple, production‑oriented background job system for Go with pluggable backends (In‑Memory, Redis, AWS SQS), clean dev experience, and predictable semantics (retries, DLQ, middleware, metrics, graceful shutdown).

## High‑Level Overview
Core idea: Applications produce Jobs and enqueue them via a Queue. Workers consume jobs, run user‑defined Handlers, and report results. The queue storage and transport are abstracted behind Drivers so you can swap In‑Memory, Redis, or AWS SQS without changing application code.

```
Producer → Queue API → Driver (In‑Mem | Redis | SQS) → Worker Pool → Dispatcher → Handler → Result
```

## Key Properties

- **Pluggable drivers** behind a common interface.
- **Job registry** for (de)serialization of typed jobs.
- **Middleware pipeline** for cross‑cutting concerns (logging, rate‑limit, tracing…).
- **Retries + backoff** and optional Dead‑Letter Queue (DLQ) adapter.
- **Metrics callback hook** for observability.
- **Graceful shutdown** & concurrency control.

## Packages & Responsibilities

| Package | Responsibility |
|---------|----------------|
| `/adapter` | Optional integrations (e.g., Redis DLQ adapter) |
| `/config` | User‑facing configuration builders & options |
| `/dispatcher` | Job type registry + dispatch to user handler |
| `/internal` | Shared internals (helpers, backoff, clocks, errors) |
| `/job` | Job model, envelope, (de)serialization utilities |
| `/middleware` | Middleware interfaces + built‑ins (logging, conditional skip, etc.) |
| `/queue` | Core queue interfaces + driver implementations |
| `/worker` | Worker loop, pool management, concurrency, shutdown |

End-to-end (E2E) and benchmark harness for queue drivers

Purpose:

- Provide a separate folder for tests that exercise drivers using client code (integration-style).
- Keep these tests out of the fast unit test run; use a build tag `e2e` when executing.
- Provide a place to add benchmarks for performance comparisons.

Usage:

- Run e2e tests: go test -tags=e2e ./e2e
- Run e2e benchmarks: go test -tags=e2e -bench . ./e2e

Notes:

- The repository already contains unit tests for each adapter. E2E tests should run more slowly and may require external services (Redis, SQS). Use miniredis or LocalStack in CI for full verification.
- Files in this folder use the `//go:build e2e` build tag to exclude them from normal `go test ./...` runs.

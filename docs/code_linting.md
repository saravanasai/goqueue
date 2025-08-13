# GoQueue Linting Configuration

This document outlines the steps to add code linting to the GoQueue project.

## Steps Taken

1. Created a `.golangci.yml` configuration file with the following linters enabled:
   - errcheck: Checks for unchecked errors
   - gosimple: Simplifies code
   - govet: Reports suspicious constructs
   - ineffassign: Detects unused assignments
   - staticcheck: Go static analysis
   - typecheck: Type checking
   - unused: Checks for unused constants, variables, functions, and types
   - gofmt: Formats code according to Go standards
   - goimports: Manages imports and formats code
   - revive: Fast, configurable, extensible, flexible, and beautiful linter
   - misspell: Finds commonly misspelled English words
   - godot: Check if comments end with a period
   - gocyclo: Computes and checks the cyclomatic complexity
   - dupl: Detects code clones
   - unconvert: Removes unnecessary type conversions
   - goconst: Finds repeated strings that could be constants
   - prealloc: Finds slice declarations that could pre-allocate
   - unparam: Reports unused function parameters

2. Fixed formatting issues using `go fmt ./...`

3. Fixed unchecked errors:
   - Added error checking for Redis operations in redis_store.go
   - Updated queue/StartWorkers method to return errors
   - Added error handling in test files

4. Fixed comment periods (godot issues):
   - Added periods to comments in config.go
   - Fixed various comments across the codebase to end with periods

5. Addressed code duplication:
   - Created a helper function to generate standard SQS message attributes

6. Fixed shadow variables in SQS adapter

## How to Run Linting

Run the linter using the following command:

```bash
golangci-lint run
```

## Integration with CI/CD

Add this step to your CI/CD pipeline to ensure code quality:

```yaml
lint:
  runs-on: ubuntu-latest
  steps:
    - uses: actions/checkout@v3
    - uses: actions/setup-go@v4
      with:
        go-version: '1.20'
    - name: golangci-lint
      uses: golangci/golangci-lint-action@v3
      with:
        version: latest
```

## Pre-commit Hook

You can set up a pre-commit hook to run linting before each commit:

```bash
#!/bin/sh
# .git/hooks/pre-commit
golangci-lint run
if [ $? -ne 0 ]; then
  echo "Linting failed! Fix the issues before committing."
  exit 1
fi
```

Make the hook executable with `chmod +x .git/hooks/pre-commit`

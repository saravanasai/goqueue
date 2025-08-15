# E2E Tests

End-to-end tests for GoQueue drivers. Test complete queue functionality with realistic job processing.

## Test Commands by Driver

### Memory Driver

```bash
# Run memory e2e test
go test -v ./e2e/ -run TestSimple
```

### Redis Driver

```bash
# Run Redis e2e tests
go test -v ./e2e/ -run TestRedis

# Run specific Redis test
go test -v ./e2e/ -run TestRedisQueueConcurrentDispatch
```

### All Drivers

```bash
# Run all e2e tests
go test -v ./e2e/
```

## Test Features by Driver

| Feature               | Memory | Redis  | SQS\* |
| --------------------- | ------ | ------ | ----- |
| Job Processing        | ✅     | ✅     | ❌    |
| Worker Management     | ✅     | ✅     | ❌    |
| Metrics Collection    | ✅     | ❌\*\* | ❌    |
| Health Monitoring     | ✅     | ✅     | ❌    |
| Concurrent Operations | ❌     | ✅     | ❌    |
| Graceful Shutdown     | ✅     | ✅     | ❌    |

\*SQS has unit tests in `./adapter/sqs/` but no e2e tests yet  
\*\*Redis tests don't use metrics to avoid timing issues with miniredis

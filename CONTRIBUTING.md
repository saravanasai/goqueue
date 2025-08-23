# Contributing to GoQueue

Thank you for your interest in contributing to GoQueue!

## 🚀 Quick Start

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/amazing-feature`
3. Make your changes
4. Add tests for your changes
5. Run tests: `go test ./...`
6. Commit your changes: `git commit -m 'Add amazing feature'`
7. Push to your branch: `git push origin feature/amazing-feature`
8. Open a Pull Request

**Note**: All tests run locally without external dependencies. Redis tests use redis, SQS tests only require AWS credentials for live testing.

## � Code Quality

```bash
# Run linter
golangci-lint run

# Format code
go fmt ./...
```

## 🐛 Reporting Issues

Please include:

- Go version
- Steps to reproduce
- Expected vs actual behavior
- Relevant logs

## � Pull Request Checklist

- [ ] Tests added/updated
- [ ] Code linted and formatted
- [ ] Documentation updated
- [ ] Self-reviewed

## 📄 License

By contributing, you agree that your contributions will be licensed under the [MIT License](LICENSE).

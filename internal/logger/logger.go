package logger

type Logger interface {
	Info(msg string, fields ...interface{})
	Error(msg string, fields ...interface{})
	// Add other methods as needed
}

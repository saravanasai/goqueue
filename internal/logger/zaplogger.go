package logger

import "go.uber.org/zap"

type ZapLogger struct {
	sugar *zap.SugaredLogger
}

func NewZapLogger() *ZapLogger {
	l, _ := zap.NewProduction()
	return &ZapLogger{sugar: l.Sugar()}
}

func (z *ZapLogger) Info(msg string, fields ...interface{}) {
	z.sugar.Infow(msg, fields...)
}

func (z *ZapLogger) Error(msg string, fields ...interface{}) {
	z.sugar.Errorw(msg, fields...)
}

func (z *ZapLogger) Warn(msg string, fields ...interface{}) {
	z.sugar.Warnw(msg, fields...)
}

func (z *ZapLogger) Fatal(msg string, fields ...interface{}) {
	z.sugar.Fatalw(msg, fields...)
}
func (z *ZapLogger) Debug(msg string, fields ...interface{}) {
	z.sugar.Debugw(msg, fields...)
}
